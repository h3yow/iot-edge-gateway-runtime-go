# IoT 边缘网关运行时（Go，进程内模拟）

一个零外部依赖、可直接在本机跑起来的 IoT 边缘网关运行时。设备连接、消息上报、
设备影子同步、固件分发全部在**单进程内模拟**，串成一条自洽链路，可用于验证：

- 按主题层级订阅、MQTT 通配符（`+` / `#`）匹配；
- 多设备并发上报、同设备重复上报、断线重连补投时，**不重复入库、不丢消息**；
- 非法主题与未授权访问被拒绝，且**拒绝原因严格可区分**；
- 设备期望状态（desired）与上报状态（reported）分开维护，离线暂存、重连按版本合并，
  冲突不静默覆盖、裁决可解释，重复下发请求幂等；
- 固件分批 + 灰度发布，同版本重复安装幂等，失败率超阈值整体回滚，
  并发发布/安装受控，取消与高版本取代时状态保持一致。

## 目录结构

```
cmd/gateway            本地一键演示入口（6 个场景，打印关键输入与判定依据）
internal/mqtt          主题校验/通配符匹配 + 进程内 broker（鉴权、原子去重、扇出）
internal/auth          按设备、按动作（pub/sub）、按过滤器的主题权限模型
internal/store         消息幂等入库（原子"检查并登记"，实现 mqtt.Deduplicator）
internal/shadow        设备影子：desired/reported 分离、版本合并、冲突裁决
internal/firmware      固件分批灰度、失败回滚、取消、高版本取代、并发闸门
internal/device        模拟设备会话：上下线、离线指令 FIFO 暂存、重连补投
internal/gateway       组装以上模块的网关外观 + 模拟安装器
```

## 快速开始

要求 Go 1.25+（开发环境 Go 1.26 验证通过），无第三方依赖。

```bash
# 端到端演示（6 个场景，全部打印输入与结论原因）
go run ./cmd/gateway

# 全部单测（建议带竞态检测，多跑几轮）
go test ./... -race -count=3
go test ./... -race -v          # -v 可看到每个用例打印的关键输入和判定依据
```

演示场景依次为：

1. 多设备并发上报 + 同设备并发重复 + 断线重连补投 → 入库 6 条、dev-1 仅 1 条；
2. 非法主题 / 越权发布 / 越权订阅 → 三种不同拒绝原因；
3. 影子离线暂存 → 重连按版本合并 → 同版本冲突显式裁决 → 请求幂等；
4. 固件灰度波失败率超阈值 → 成功设备回滚、未开始设备取消、设备版本恢复；
5. 回滚后重新发布成功，同版本再发 → 6 台全部 skipped、安装器零调用；
6. 更高版本发布取代旧发布 → 状态一致（设备已在新版本则保留成功事实）。

## 1. 主题与权限模型

### 1.1 主题规则

- 主题按 `/` 分层；空层级合法（`a/` 与 `a` 是不同主题）。
- **发布主题**（`ValidateTopic`）：非空，且任何层级不得包含 `+`、`#`。
- **订阅过滤器**（`ValidateFilter`）：
  - `+` 必须独占一整层，恰好匹配一层（`a/+` 匹配 `a/b`，不匹配 `a/b/c`）；
  - `#` 必须独占最后一层，匹配零层或多层（`a/#` 同时匹配 `a` 与 `a/b/c`）。

### 1.2 权限模型（最小权限、默认拒绝）

授权表为三元组 `(设备ID, 动作 pub|sub, 过滤器)`，由 `auth.Policy.Grant` 显式放行；
未匹配到任何授权一律拒绝（`internal/auth/authz.go`）。

- `CanPublish`：任一 pub 授权过滤器与具体主题匹配即放行。
- `CanSubscribe`：采用**覆盖判定**（`filterCoveredBy`）——请求订阅可见的每个主题，
  必须都落在某条 sub 授权的可见集合内。因此授权 `a/#` 可以订阅更窄的 `a/b`，
  但授权 `a/b` 不能订阅更宽的 `a/#` 或 `a/+`，杜绝越权扩大可见范围。

网关注册设备时的默认授权（`gateway.RegisterDevice`）：

| 设备 | 允许发布 | 允许订阅 |
|---|---|---|
| `<id>` | `devices/<id>/telemetry/#` | `devices/<id>/cmd/#` |

### 1.3 拒绝原因如何区分

`Broker.Publish` 的判定顺序固定，结论放在 `PublishResult{Status, Reason}`：

| 顺序 | 情形 | Status | Reason / error |
|---|---|---|---|
| 1 | 发布主题非法 | `rejected` | `invalid_topic` |
| 2 | 发布未授权 | `rejected` | `unauthorized_publish` |
| 3 | 消息重复 | `duplicate` | `duplicate`（不扇出、不入库） |
| 4 | 正常 | `delivered` | `delivered` |

订阅侧返回**哨兵错误**便于 `errors.Is` 判定：
`mqtt.ErrInvalidFilter`（非法过滤器）与 `mqtt.ErrUnauthorizedSubscribe`（未授权）。

### 1.4 不重复、不丢失如何保证

- 去重键为 `(设备ID, 消息ID)`，去重器（`store.Store.Seen`）在**同一把互斥锁内
  完成"查重 + 登记 + 落库"**；broker 在扇出之前调用它。
- 并发重复上报：临界区串行化，只有第一个调用者落库并扇出，其余返回 `duplicate`。
- 断线重连补投：消息 ID 不变，补投走同一条去重路径（QoS1 至少一次 + 接收端幂等）。
- 不同设备使用相同消息 ID 互不影响；其他消息正常入库，验证无丢失。

## 2. 设备影子与合并规则

每个设备维护字段级的双侧状态：`desired`（期望值、版本、时间）与
`reported`（上报值、版本、时间），存于 `internal/shadow`。

### 2.1 期望下发（`DesiredUpdate(reqID, device, version, fields)`）

- **请求幂等**：`reqID` 已处理过则直接返回首次结论（`idempotent/duplicate_request`），
  不改任何状态。
- **设备离线**：请求按到达顺序进暂存队列（`stashed`）。
- **设备在线 / 重连合并**（`MergePending`）：逐字段按版本裁决：

| 判定 | 条件 | Verdict | 处理 |
|---|---|---|---|
| 采用 | 新版本 > 当前 desired 版本 | `applied` | 覆盖，随后追加与 reported 的对账裁决 |
| 幂等忽略 | 版本相同且值相同 | `idempotent` | 不变 |
| 冲突 | 版本相同但值不同 | `conflict` | **绝不静默覆盖**，按冲突策略裁决并记录双方值与依据 |
| 拒绝 | 版本倒退 | `rejected` | 保留现值 |

- 冲突策略（`NewWithConflictPolicy`）：
  - `reject`（默认，最保守）：保留现值；
  - `prefer_desired` / `prefer_reported`：仅用于 desired/reported 对账冲突。
  任何策略下，`Decision{Field, Verdict, Detail}` 都会写明冲突双方的值、
  胜出方与策略名，裁决可解释。

### 2.2 上报与对账（`ReportState`）

上报只写 reported 侧，然后与 desired 对账：`desired_newer`（设备尚需执行）、
`reported_newer`（保留上报）、`equal`（已同步）、`conflict`（同版本值不一致）。
上报版本倒退同样拒绝。

### 2.3 上线合并

`Gateway.Connect` 先让设备会话重放离线下行指令，再 `SetOnline` + `MergePending`。
暂存请求按 FIFO 逐个合并；每个请求的逐字段裁决都在返回的 `Result.Decisions` 中。
再次合并无暂存内容时返回 `idempotent/nothing_pending`。

## 3. 固件发布、灰度与回滚策略

实现位于 `internal/firmware`，设备侧安装由 `firmware.Installer` 接口抽象，
网关提供可注入失败/阻塞的模拟实现（`gateway.SimInstaller`）。

### 3.1 调度模型

- **灰度波**：首批取 `ceil(目标数 × canaryPct / 100)` 台（canaryPct>0 时至少 1 台）；
- **常规批次**：灰度波之后的设备按 `batchSize` 逐批推进；
- **并发受控**：两道信号量——并发发布数、批内并发安装数，均由 `maxConcurrent` 限制；
- **设备级互斥**：同一设备同一时刻只能被一个发布安装，杜绝跨发布重复/交叉安装。

### 3.2 安装幂等

`Manager` 记录每台设备当前生效版本。下发前若发现该设备**已在目标版本**，
直接标记 `skipped`，不调用安装器。因此：

- 同一发布重发、灰度/常规波覆盖到同一设备、重连补投同一条安装指令，
  都不会重复安装（单测断言安装器真实调用次数不增长）。
- 回执接口 `ReportInstall` 对已终态记录同样幂等。

### 3.3 失败判定与回滚（在波次边界统一执行）

- 每波安装结束后统一计算失败率（避免波内先完成的设备提前中止、饿死同波其他设备）；
- 当 `failed / processed × 100 > 阈值` 时整体中止：
  - 本波及之前**成功**的设备 → `rolled_back`，当前生效版本恢复到发布前版本；
  - **尚未下发 / 在途未回执**的设备 → `cancelled`；
  - 失败设备保留 `failed` 事实；
  - 发布终态 `failed`，`FailReason` 记录失败率算式（可解释）。

### 3.4 取消与高版本取代

- `Cancel`：
  - 发布未开始：同步收敛为 `cancelled`；
  - 发布进行中：**只设置取消信号并立即返回**，Publish 在当前波次边界完成回滚，
    调用方用 `WaitTerminal` 会合（这样不会与"安装器尚未释放"形成等待环）；
  - 已终态：返回 `ErrAlreadyTerminal`，终态不可被破坏。
- `Supersede(old, new)`：新版本号必须**严格更高**（点分数字版本比较），
  否则 `ErrVersionConflict`。允许取代进行中或已完成的发布；
  `failed/cancelled/superseded` 不可逆：
  - 旧发布未下发设备 → `cancelled`；
  - 旧发布成功的设备若已被新版本发布接管（当前版本≠旧版本）→ **保留 success**，
    不做跨发布错误回滚；若仍停留在被取代版本才回滚；
  - 旧发布整体标记 `superseded`，与新发布并存且状态一致。

## 4. 本地复现与验证

```bash
# 端到端演示
go run ./cmd/gateway

# 指定场景相关单测，并查看关键输入/判定日志
go test ./internal/mqtt/     -race -v   # 通配符、拒绝原因区分、并发去重
go test ./internal/auth/     -race -v   # 权限覆盖模型
go test ./internal/store/    -race -v   # 50 并发补投 + 5 条新消息
go test ./internal/shadow/   -race -v   # 影子合并/冲突/离线暂存
go test ./internal/firmware/ -race -v   # 回滚、取消、幂等、并发闸门、取代
go test ./internal/device/   -race -v   # 离线 FIFO 与重连补投
go test ./internal/gateway/  -race -v   # 全链路：并发上报/重连补投/固件回滚
```

关键断言速查：

| 场景 | 入口 | 期望结论 |
|---|---|---|
| 并发上报不重不丢 | `gateway.TestConcurrentTelemetryAcrossDevices` | 101 入库，29 条 duplicate |
| 断线重连补投 | `gateway.TestReconnectReplayIdempotent` | 补投 duplicate，新消息 delivered，期望合并 v7 |
| 影子合并冲突 | `shadow.TestDesiredVersionMergeAndConflict` | conflict 保留 blue，倒退 rejected |
| 发布失败回滚 | `firmware.TestFailureRollback` | 1 failed / 1 rolled_back / 2 cancelled，版本恢复 |
| 取消一致性 | `firmware.TestCancelDuringPublish` | 2 rolled_back / 2 cancelled |
| 同版本幂等 | `firmware.TestSameVersionIdempotent` | 全 skipped，安装器每设备仅 1 次 |
| 并发受控 | `firmware.TestConcurrentPublishControlled` | 第二个进行中发布被拒绝 |
