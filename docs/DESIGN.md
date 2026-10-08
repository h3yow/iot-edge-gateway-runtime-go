# IoT 边缘网关运行时（进程内模拟）设计说明

本项目在**单个 Go 进程**内模拟完整的 IoT 边缘网关链路：设备连接 → 主题鉴权与消息接入
→ 消息幂等入库 → 设备影子（期望/上报）状态同步 → 固件分批灰度发布与回滚。
无外部中间件依赖，`go run ./cmd/gateway` 即可在本机复现。

## 1. 目录结构

| 包 | 职责 |
|---|---|
| `internal/topic` | 分层主题解析、`+`/`#` 通配符匹配、过滤器覆盖关系 |
| `internal/auth` | 设备身份注册表 + 发布/订阅 ACL，拒绝原因可区分 |
| `internal/store` | 按 `(deviceID, messageID)` 的线程安全幂等入库 |
| `internal/broker` | 进程内连接、订阅、层级匹配投递、发布鉴权 |
| `internal/shadow` | 期望/上报双版本影子、离线暂存、三方合并冲突裁决 |
| `internal/firmware` | 分批灰度、**全局并发治理**、幂等/已就绪安装、版本裁决与接管、取消/取代、失败回滚 |
| `internal/runtime` | 把上述组件串成一条自洽链路，含模拟设备行为 |
| `cmd/gateway` | 本机可运行的端到端演示入口 |

## 2. 主题模型

- 主题以 `/` 分层，**不允许空层级**：`/a`、`a//b`、`a/` 均判非法。
- 通配符只允许出现在**订阅过滤器**中：
  - `+` 匹配恰好一个层级：`devices/+/up` 匹配 `devices/d1/up`，不匹配 `devices/d1/x/up`。
  - `#` 只能位于末尾，匹配零或多个层级：`devices/#` 可匹配 `devices` 与 `devices/d1/up`。
- 发布主题**禁止任何通配符字符**，`a+`、`a#` 这类混合片段也判非法。

## 3. 权限模型与拒绝原因

设备通过 `auth.ACL.Grant(deviceID, publishFilters, subscribeFilters)` 注册并授权。
每次 `Connect / Publish / Subscribe` 都过 `ACL.Check`，按固定顺序判定，拒绝原因互不混淆：

| RejectReason | 触发条件 |
|---|---|
| `unknown_device` | 设备未注册（连接或任何动作的第一道闸） |
| `invalid_topic` | 主题空层级、`#` 不在末尾、混合非法片段 |
| `wildcard_not_allowed_on_publish` | 发布主题里出现 `+` 或 `#` |
| `unauthorized_publish` | 主题合法但未被任何发布过滤器匹配 |
| `unauthorized_subscribe` | 订阅过滤器未被任何已授予过滤器“覆盖” |

订阅授权采用**过滤器覆盖**语义：只要存在一条已授予过滤器 `g`，使 `want` 能匹配的所有
具体主题都能被 `g` 匹配，则允许订阅 `want`。例如授予 `devices/d1/cmd/#` 后可订阅
`devices/d1/cmd/reboot`；但授予 `devices/d1/cmd` 不允许订阅更宽的 `devices/d1/#`。

连接支持**断线重连顶号**：同设备新连接会把旧 `Session` 标记关闭并清空订阅，
设备需在重连后重新订阅（与真实 MQTT clean session 行为一致）。

## 4. 消息接入与幂等（不重复、不丢失）

- 幂等键为 `(deviceID, messageID)`，记录于 `store` 的去重集合。
- 每次发布有且仅有一个明确结论：
  - 首次：`stored=true, duplicated=false`，写入存储；
  - 重复（同设备重连补投同一 ID）：`stored=false, duplicated=true`，**不重复入库**；
  - 不同设备的相同 `messageID` 互不影响。
- 发布先鉴权、再入库、最后按订阅过滤器层级匹配投递；非法/越权消息在入库前即被拒绝，
  因此“拒绝”既不污染存储，也不会丢消息——客户端能拿到可区分的拒绝原因。
- 去重表与消息列表在同一把互斥锁内更新，16 goroutine × 200 条（含大量重复 ID）的
  压测在 `-race` 下稳定收敛到唯一条数。

## 5. 设备影子与合并规则

每个设备维护两份相互独立的状态：

- **reported**：设备实际上报状态，版本 `reportedVersion` 严格单调递增。
  - 旧版本补投判 `stale` 并忽略，绝不回滚覆盖新版本；
  - 同版本同内容重复上报判幂等；同版本不同内容同样判 stale（防止静默覆盖）。
- **desired**：平台期望状态，每次成功应用使 `desiredVersion +1`。

### 5.1 离线暂存与重连合并

1. 设备离线时调用 `ApplyDesired`：请求进入 `staged` 队列，不立即改 desired。
2. 相同 `requestID` 重复下发一律返回 `duplicate=true`（在线/离线都幂等）。
3. 设备重连后**先补报其最新 reported 版本**（运行时用 `needsFlush` 标志保证顺序），
   再按暂存顺序依次三方合并。

### 5.2 三方合并与冲突裁决（不静默覆盖）

每个暂存请求携带下发时刻的 reported 快照作为 **base**，合并时逐字段比较
`base / desired / 当前reported`：

- desired 与当前 reported 已相同 → 设备已自行达到，无冲突；
- 仅一侧相对 base 变化 → 变化侧生效；
- **两侧都改且值不同 → 冲突**，输出 `ConflictRecord`（含字段、三方取值、获胜方、文字理由）：
  - 可控字段（默认）：`desired` 获胜——平台意图优先（理由
    `controllable field: platform desired wins`）；
  - 只读/传感器字段（通过 `Shadow.MarkReadOnly` 注册，如 `temp`）：`reported` 获胜
    ——传感器事实优先（理由 `read-only/sensor field: device reported wins`）。

冲突结论同时体现在返回值和日志里，可逐条解释。

## 6. 固件发布、灰度与回滚

### 6.1 发布模型

`Config` 包含目标版本、有序批次（首个批次通常标记 `Canary`）、`MaxConcurrency`
与 `FailureThreshold`：

- **分批灰度**：严格按批次顺序执行，前一批未完成/未通过不会开始下一批。
- **单发布并发受控**：每批内用容量为 `MaxConcurrency` 的信号量限流，安装峰值并发不超过上限。
- **全局并发治理**：多个发布单可同时进行，但所有发布单（含回滚安装）真正处于“安装中”的
  设备总数受 Manager 级全局信号量约束，峰值不超过 `WithGlobalMaxConcurrency(n)` 设定值，
  各发布单即使各自把 `MaxConcurrency` 填得很大，叠加后也不会顶穿网关总容量。
- **单设备幂等/已就绪安装**：以 `(deviceID, targetVersion)` 的当前版本判断为准：
  - 设备当前已是目标版本 → 记 `ready`（已就绪、无需安装），不占安装名额、零安装调用；
  - 同设备重复收到同版本（重复 `Offer`、或已由其他路径装好）直接跳过，不会重复安装。
- **终态一致性兜底**：发布单收敛（成功或任一中止路径）后，所有设备记录必须处于确定状态，
  不允许发布单已报完成、设备却仍挂 `pending/installing`；悬挂记录按实际版本归一为
  `installed / ready / kept / skipped`。

#### 6.1.1 容量参数默认值与兼容范围

| 参数 | 作用域 | 异常输入（未填 / `0` / 负数）兜底 | 说明 |
|---|---|---|---|
| `Config.MaxConcurrency` | 单个发布单的批内并发 | `DefaultPerReleaseConcurrency = 1` | 异常时保守串行，绝不 panic（旧实现负数会导致信号量 `make` panic） |
| `Config.FailureThreshold` | 批次失败阈值 | `1` | 异常时“一台失败即止损回滚” |
| `WithGlobalMaxConcurrency(n)` | 网关全局同时安装设备总数 | `DefaultGlobalMaxConcurrency = 8` | 跨所有并行发布单（含回滚）的硬上限 |

兼容范围：任何 `int` 输入均可接受，系统只做下限保护，不设上限截断（由调用方按网关能力给出）；
旧代码中 `NewManager(install)` 无选项调用方式保持不变，自动取全局默认值。

### 6.2 版本先后裁决与接管

版本高低由 `CompareVersions` 全序裁决：去 `v/V/fw-/firmware-` 前缀后按 `.`/`-` 分段，
数字段按数值比较（`1.10 > 1.9`），数字段新于非数字段，带预发布后缀的更旧（`2.0-rc1 < 2.0`），
空串视为最旧。

- **低版本不能顶掉高版本**：`Publish` 准入时只要任一目标设备当前已在**严格更高**的已发布
  版本上，整单拒绝并返回 `ErrLowerVersionRejected`，不产生发布记录、不发生任何安装；
  安装执行前还会再做一次当前版本双检（`DevRejected`），兜住并行发布下的竞态。
- **高版本自动接管**：更高版本发布准入时，设备集合有交集且仍在进行中的低版本发布单会被
  自动标记 `superseded`、协作式停止；显式 `Supersede(old, new)` 也校验 `new` 必须严格更高，
  否则返回 `ErrInvalidVersionOrder`，低版本不能借“接管”名义顶掉高版本。
- **被接管后不可拽回**：被接管发布随后无论重试（终态发布单的 `Offer` 返回
  `ErrAlreadyTerminal`）、`Cancel`（同样被终态拒绝）还是自身回滚，只要设备当前已不在
  该发布目标版本上（已在更高版本），回滚就不执行任何降级安装，设备状态记 `kept`，
  保持在最新的已发布版本。

### 6.3 失败回滚与取消/取代的一致性

任何中止路径都会把状态机收敛到确定终态，并回滚已安装设备：

| 中止来源 | 终态 | 行为 |
|---|---|---|
| 批次内失败数 ≥ 阈值 | `rolled_back` | 停止后续批次，已安装设备回滚到发布前版本 |
| `ctx` 取消 / `Cancel(version)` | `cancelled` | 协作式停止后回滚 |
| `Supersede(old,new)` | `superseded` | 被更高版本取代，停止并回滚 |
| 全部批次成功 | `published` | 设备版本统一推进到新版本 |

- 回滚目标版本在发布开始时按设备快照（`fromVer`）记录，回滚就是 `targetVer -> fromVer`；
  回滚成功的设备状态为 `rolled_back` 且 Manager 内当前版本恢复，未及安装的为 `skipped`，
  安装失败的保留 `failed` 及错误信息，回滚自身的失败也会记录到 `Release.RollbackErrors`。
- **版本裁决优先于回滚动作**：设备当前已不在本发布目标版本（已被更高版本接管）时，回滚不做
  任何降级安装，设备状态记 `kept`，始终保持在最新已发布版本；回滚安装同样占用全局容量名额。
- 设备终态词汇：`ready`（当前已是目标版本，零安装）、`installed`（本次新装成功）、
  `rolled_back`（回退到发布前版本）、`kept`（被更高版本接管，保持新高版本）、
  `rejected`（低版本下发被裁决拒绝）、`skipped`（中止路径上未及安装）、`failed`（安装/回滚失败）。
- 停止标志是协作式的（原子量 + 每批屏障），因此“发布中途取消”不会留下半装版本：
  终态下不存在 `installed` 且当前版本仍是被取消版本的设备。
- 对终态发布单再次 `Cancel/Supersede/Offer` 会返回 `ErrAlreadyTerminal`，
  重复创建同版本发布返回 `ErrAlreadyPublished`。

## 7. 本地复现

前置：Go 1.25+（仅标准库，无第三方依赖）。

```bash
# 端到端演示：并发上报去重 / 离线暂存 / 重连合并冲突 / 灰度发布 /
#             并行发布治理（容量兜底、低版本拒绝）/ 拒绝原因
go run ./cmd/gateway

# 全量单元测试（竞态检测），原有灰度/回滚/取消/取代用例 + 新增并行治理用例
go test -race -count=1 ./...

# 只看某类场景的详细判定日志（关键输入与裁决依据均在日志中）
go test -race -v ./internal/broker     # 并发上报、断线重连补投
go test -race -v ./internal/shadow     # 影子合并冲突、过期版本、幂等
go test -race -v ./internal/firmware   # 灰度成功、失败回滚、取消、取代、并发上限、
                                       # 并行全局容量、参数兜底、已就绪、低版本拒绝、接管后回滚
go test -race -v ./internal/runtime    # 全链路集成
```

### 7.1 并行发布治理用例复现

`internal/firmware/governance_test.go` 与 `version_test.go` 覆盖新增场景，可单独运行：

```bash
go test -race -v ./internal/firmware -run 'TestGovernance|TestCompareVersions'
```

| 用例 | 输入要点 | 判定依据 / 预期 |
|---|---|---|
| `TestGovernanceGlobalConcurrencyAcrossParallelReleases` | 2 个发布单并行，各自 `MaxConcurrency=10`，全局上限 3 | 安装函数内自计在装数，观测峰值 ≤ 3，结束后 `GlobalInflight()=0` |
| `TestGovernanceConcurrencyFallback` | 全局容量填 0/-5；单发布并发与阈值填 0/-3 | 分别兜底为 `DefaultGlobalMaxConcurrency`、并发 1、阈值 1，发布仍成功 |
| `TestGovernanceDeviceAlreadyReady` | d1 已是目标 v6，d2/d3 在 v5 | d1 记 `ready` 且零安装调用；发布 `published` 后无设备挂 `pending` |
| `TestGovernanceOfferReadyDuringRolling` | 滚动期间对就绪设备 `Offer` | 返回 `ready, nil`，不产生安装 |
| `TestGovernanceLowerVersionRejected` | v8 已成功后下发 v7；`Supersede(v8,v7)` | 返回 `ErrLowerVersionRejected` / `ErrInvalidVersionOrder`，设备保持 v8 |
| `TestGovernanceSupersededRollbackKeepsHigherVersion` | v2 滚动中 v3 自动接管 | v2 终态 `superseded`、设备记 `kept` 且停留在 v3；无 `v3->v1/v2` 调用；接管后 `Cancel` 被终态拒绝 |

### 演示程序观察点

1. 阶段1：3 设备 × 20 次并发上报、每设备仅 8 个唯一 ID → 每设备最终入库恰为 8 条。
2. 阶段2：`dev-A` 断线后 `cmd-001` 显示 `staged=true`；重复下发显示 `duplicate=true`。
3. 阶段3：重连补报 `mode=heat,temp=26` 后自动三方合并：
   - `mode`（可控）裁决给 desired=`cool`；
   - `temp`（只读）裁决给 reported=`26`；两条冲突均带 base/desired/reported 与理由。
4. 阶段4：`fw-2.0` 按 canary→rest 两批发布成功。
5. 阶段4b：`fw-2.1`（dev-A/B）与 `fw-2.2`（dev-C/D）两个团队并行发布，
   并发参数填 `0` 被兜底为 1；随后低版本 `fw-1.9` 下发 dev-A 被版本裁决拒绝，
   设备保持 `fw-2.1`；日志可见“发布受理 / 低版本发布被拒 / 发布收敛”等关键输入与结论。
6. 阶段5：依次打印 `unauthorized_publish / invalid_topic /
   wildcard_not_allowed_on_publish / unknown_device` 四种拒绝原因。
7. 失败回滚路径见 `internal/firmware` 与 `internal/runtime` 的测试日志：
   注入 d2 安装失败后，已装上新版本的 d1 回滚到发布前 v1，终态 `rolled_back`、
   原因 `failure_threshold`；接管场景下设备则记 `kept` 不降级。
