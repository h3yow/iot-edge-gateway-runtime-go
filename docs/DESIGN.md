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
| `internal/firmware` | 分批灰度、并发上限、幂等安装、取消/取代、失败回滚 |
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
- **并发受控**：每批内用容量为 `MaxConcurrency` 的信号量限流，安装峰值并发不超过上限。
- **单设备幂等安装**：以 `(deviceID, targetVersion)` 的当前版本判断为准，
  同设备重复收到同版本（重复 `Offer`、或已由其他路径装好）直接跳过，不会重复安装。

### 6.2 失败回滚与取消/取代的一致性

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
- 停止标志是协作式的（原子量 + 每批屏障），因此“发布中途取消”不会留下半装版本：
  终态下不存在 `installed` 且当前版本仍是被取消版本的设备。
- 对终态发布单再次 `Cancel/Supersede/Offer` 会返回 `ErrAlreadyTerminal`，
  重复创建同版本发布返回 `ErrAlreadyPublished`。

## 7. 本地复现

前置：Go 1.25+（仅标准库，无第三方依赖）。

```bash
# 端到端演示：并发上报去重 / 离线暂存 / 重连合并冲突 / 灰度发布 / 拒绝原因
go run ./cmd/gateway

# 全量单元测试（竞态检测），覆盖需求点名的四类场景
go test -race -count=1 ./...

# 只看某类场景的详细判定日志
go test -race -v ./internal/broker     # 并发上报、断线重连补投
go test -race -v ./internal/shadow     # 影子合并冲突、过期版本、幂等
go test -race -v ./internal/firmware   # 灰度成功、失败回滚、取消、取代、并发上限
go test -race -v ./internal/runtime    # 全链路集成
```

### 演示程序观察点

1. 阶段1：3 设备 × 20 次并发上报、每设备仅 8 个唯一 ID → 每设备最终入库恰为 8 条。
2. 阶段2：`dev-A` 断线后 `cmd-001` 显示 `staged=true`；重复下发显示 `duplicate=true`。
3. 阶段3：重连补报 `mode=heat,temp=26` 后自动三方合并：
   - `mode`（可控）裁决给 desired=`cool`；
   - `temp`（只读）裁决给 reported=`26`；两条冲突均带 base/desired/reported 与理由。
4. 阶段4：`fw-2.0` 按 canary→rest 两批发布成功。
5. 阶段5：依次打印 `unauthorized_publish / invalid_topic /
   wildcard_not_allowed_on_publish / unknown_device` 四种拒绝原因。
6. 失败回滚路径见 `internal/firmware` 与 `internal/runtime` 的测试日志：
   注入 d2 安装失败后，已装上新版本的 d1 回滚到发布前 v1，终态 `rolled_back`、
   原因 `failure_threshold`。
