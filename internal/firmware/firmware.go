// Package firmware 实现固件分批灰度发布、去重安装、失败回滚与并发控制。
package firmware

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Status 是发布单生命周期状态。
type Status string

const (
	StatusPending     Status = "pending"
	StatusRolling     Status = "rolling"
	StatusPublished   Status = "published"
	StatusRollingBack Status = "rolling_back"
	StatusRolledBack  Status = "rolled_back"
	StatusCancelled   Status = "cancelled"
	StatusSuperseded  Status = "superseded"
)

// DeviceStatus 是单设备维度的安装状态。
type DeviceStatus string

const (
	DevPending    DeviceStatus = "pending"
	DevInstalling DeviceStatus = "installing"
	DevInstalled  DeviceStatus = "installed"
	DevFailed     DeviceStatus = "failed"
	DevRolledBack DeviceStatus = "rolled_back"
	DevSkipped    DeviceStatus = "skipped"
	// DevReady：设备当前版本已经是本次发布目标版本，无需安装即视为就绪。
	DevReady DeviceStatus = "ready"
	// DevRejected：下发版本不高于设备当前版本，按“低版本下发被拒”处理。
	DevRejected DeviceStatus = "rejected"
	// DevKept：中止回滚时设备已被更高版本接管，保留在最新版本上不被拽回。
	DevKept DeviceStatus = "kept"
)

// 容量参数的默认值与兼容范围。
// 并发参数缺失（0）或非法（负数）时不会卡住发布，而是回落到安全默认值；
// 超出硬上限的值会被收敛，避免异常输入把网关自身压垮。
const (
	DefaultMaxConcurrency   = 4   // 单个发布单缺省并发
	DefaultGlobalConcurrent = 16  // 所有发布单合计缺省并发
	HardConcurrencyCap      = 256 // 并发参数兼容范围上限
)

// Batch 是一个灰度批次。
type Batch struct {
	Name      string
	Canary    bool
	DeviceIDs []string
}

// Config 描述一次发布。
type Config struct {
	Version          string
	Batches          []Batch
	MaxConcurrency   int
	FailureThreshold int // 批次内失败数达到该阈值即整体回滚；<=0 表示 1
}

// InstallFunc 由设备侧模拟：把设备从 fromVersion 安装到 toVersion。
type InstallFunc func(ctx context.Context, deviceID, fromVersion, toVersion string) error

// InstallRecord 记录单设备在某次发布中的安装轨迹。
type InstallRecord struct {
	DeviceID  string
	State     DeviceStatus
	FromVer   string
	TargetVer string
	Attempts  int
	LastError string
}

// Release 是一次发布的运行时记录。
type Release struct {
	Version        string
	Status         Status
	StartedAt      time.Time
	FailureCount   int
	RollbackErrors []string
	Reason         string // 终态原因（cancelled / superseded / threshold / ctx / done）
	// DeviceCounts 按终态统计设备数，用于发布单状态与设备状态对账。
	DeviceCounts map[DeviceStatus]int
}

// Errors
var (
	ErrAlreadyPublished = errors.New("firmware: version already exists")
	ErrNoBatches        = errors.New("firmware: no batches in config")
	ErrReleaseNotFound  = errors.New("firmware: release not found")
	ErrAlreadyTerminal  = errors.New("firmware: release already in terminal state")
	ErrNotNewerVersion  = errors.New("firmware: superseding version is not newer")
)

type installRecord struct {
	mu        sync.Mutex
	state     DeviceStatus
	fromVer   string
	targetVer string
	attempts  int
	lastError string
}

type releaseInfo struct {
	mu        sync.Mutex
	cfg       Config
	status    Status
	startedAt time.Time
	reason    string
	devices   map[string]*installRecord
	failures  int
	// stop 用于发布过程协作式取消（Cancel / Supersede / ctx 取消）。
	stop       atomic.Bool
	stopReason string
	rbErrs     []string
}

// Manager 管理所有发布单。
type Manager struct {
	mu      sync.Mutex
	install InstallFunc
	rel     map[string]*releaseInfo
	current map[string]string // deviceID -> 当前安装版本

	// 并行发布治理：所有发布单的安装/回滚共享 globalSem，
	// 保证同一时刻真正在安装的设备总数不超过全局上限。
	globalSem chan struct{}
	globalCap int
	// 每设备安装锁：序列化同设备的安装与回滚，
	// 杜绝两个发布并行下发同设备时 check-then-install 的 TOCTOU 竞态。
	devMu map[string]*deviceGate

	logf func(format string, args ...any)
}

// deviceGate 是单设备维度的安装互斥门。
type deviceGate struct {
	mu sync.Mutex
}

// ManagerOption 配置 Manager 级治理参数。
type ManagerOption func(*Manager)

// WithGlobalConcurrent 设置所有发布单合计的在装设备上限。
// <=0 时使用 DefaultGlobalConcurrent，超过 HardConcurrencyCap 会被收敛。
func WithGlobalConcurrent(n int) ManagerOption {
	return func(m *Manager) {
		m.globalCap = normalizeCapacity(n, DefaultGlobalConcurrent)
	}
}

// WithLogger 注入治理决策日志（关键输入与判定依据）。
func WithLogger(f func(format string, args ...any)) ManagerOption {
	return func(m *Manager) {
		if f != nil {
			m.logf = f
		}
	}
}

// NewManager 创建发布管理器。
func NewManager(install InstallFunc, opts ...ManagerOption) *Manager {
	m := &Manager{
		install:   install,
		rel:       make(map[string]*releaseInfo),
		current:   make(map[string]string),
		globalCap: DefaultGlobalConcurrent,
		devMu:     make(map[string]*deviceGate),
		logf:      func(string, ...any) {},
	}
	for _, opt := range opts {
		opt(m)
	}
	m.globalSem = make(chan struct{}, m.globalCap)
	m.logf("firmware manager 初始化: 全局在装上限=%d", m.globalCap)
	return m
}

// normalizeCapacity 兜底并发参数：缺失/零/负数回落默认值，过大收敛到硬上限。
func normalizeCapacity(n, def int) int {
	if n <= 0 {
		return def
	}
	if n > HardConcurrencyCap {
		return HardConcurrencyCap
	}
	return n
}

// gateFor 返回设备对应的安装门（调用时需持有 m.mu）。
func (m *Manager) gateFor(deviceID string) *deviceGate {
	g := m.devMu[deviceID]
	if g == nil {
		g = &deviceGate{}
		m.devMu[deviceID] = g
	}
	return g
}

func (m *Manager) log(format string, args ...any) {
	m.logf(format, args...)
}

// SetLogger 在构造后注入/替换治理决策日志。
func (m *Manager) SetLogger(f func(format string, args ...any)) {
	if f != nil {
		m.logf = f
	}
}

// SetCurrentVersion 预置/登记设备当前运行的固件版本（设备注册时使用）。
func (m *Manager) SetCurrentVersion(deviceID, version string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if version == "" {
		delete(m.current, deviceID)
		return
	}
	m.current[deviceID] = version
}

// CurrentVersion 返回设备当前版本。
func (m *Manager) CurrentVersion(deviceID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current[deviceID]
}

// Publish 按批次、受控并发地执行灰度发布。
// 任何中止路径（失败阈值、ctx 取消、Cancel、被取代）都会回滚已安装设备，
// 保证版本状态一致；全部批次成功才标记 published。
func (m *Manager) Publish(ctx context.Context, cfg Config) (*Release, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if _, exists := m.rel[cfg.Version]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrAlreadyPublished, cfg.Version)
	}
	// 记录每个设备进入本次发布前的版本，用于幂等与回滚。
	devs := make(map[string]*installRecord)
	for _, b := range cfg.Batches {
		for _, id := range b.DeviceIDs {
			if _, dup := devs[id]; dup {
				continue
			}
			devs[id] = &installRecord{
				state:     DevPending,
				fromVer:   m.current[id],
				targetVer: cfg.Version,
			}
		}
	}
	info := &releaseInfo{
		cfg:       cfg,
		status:    StatusRolling,
		startedAt: time.Now(),
		devices:   devs,
	}
	m.rel[cfg.Version] = info
	m.mu.Unlock()

	threshold := cfg.FailureThreshold
	if threshold <= 0 {
		threshold = 1
	}
	perCap := normalizeCapacity(cfg.MaxConcurrency, DefaultMaxConcurrency)
	if cfg.MaxConcurrency != perCap {
		m.log("发布 %s 并发参数兜底: 输入 MaxConcurrency=%d => 生效=%d（0/负数回落默认%d，超上限%d收敛）",
			cfg.Version, cfg.MaxConcurrency, perCap, DefaultMaxConcurrency, HardConcurrencyCap)
	}
	m.log("发布开始 version=%s 批次数=%d 单发布并发=%d 全局在装上限=%d 失败阈值=%d",
		cfg.Version, len(cfg.Batches), perCap, m.globalCap, threshold)
	// 发布内再做一层限流：多个发布并行时，每个发布最多持有 perCap 个全局名额，
	// 避免单个大发布占满全局容量。真正的硬闸仍是 m.globalSem。
	sem := make(chan struct{}, perCap)

	// ctx 取消 -> 协作式停止
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			info.stop.Store(true)
			info.setStopReason("ctx")
		case <-watchDone:
		}
	}()

	var batchErr error
batchLoop:
	for bi, batch := range cfg.Batches {
		var wg sync.WaitGroup
		var failCnt atomic.Int32
		for _, id := range batch.DeviceIDs {
			if info.stop.Load() {
				break
			}
			rec := info.devices[id]
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				m.doInstall(ctx, info, id, rec)
				rec.mu.Lock()
				failed := rec.state == DevFailed
				rec.mu.Unlock()
				if failed {
					failCnt.Add(1)
				}
			}()
		}
		wg.Wait()

		info.mu.Lock()
		info.failures += int(failCnt.Load())
		stopped := info.stop.Load()
		info.mu.Unlock()

		if stopped {
			batchErr = fmt.Errorf("firmware: publish %s stopped: %s", cfg.Version, info.stopReason)
			break batchLoop
		}
		if int(failCnt.Load()) >= threshold {
			info.setStopReason("failure_threshold")
			batchErr = fmt.Errorf("firmware: batch %q failures %d >= threshold %d", batch.Name, failCnt.Load(), threshold)
			break batchLoop
		}
		_ = bi
	}
	close(watchDone)

	terminal := StatusPublished
	outReason := "done"
	if batchErr == nil {
		// 成功路径也要保证设备状态全部收敛：
		// 不允许发布单 published 却还有设备挂在 pending/installing。
		for _, rec := range info.devices {
			rec.mu.Lock()
			switch rec.state {
			case DevPending, DevInstalling:
				rec.state = DevSkipped
			}
			rec.mu.Unlock()
		}
	}
	if batchErr != nil {
		switch info.stopReason {
		case "cancelled":
			terminal = StatusCancelled
		case "superseded":
			terminal = StatusSuperseded
		default:
			terminal = StatusRolledBack
		}
		outReason = info.stopReason
		if outReason == "" {
			outReason = "failure_threshold"
		}
		m.rollback(info)
	}

	info.mu.Lock()
	info.status = terminal
	info.reason = outReason
	r := m.snapshotLocked(info)
	info.mu.Unlock()
	return r, batchErr
}

// doInstall 完成单设备安装，内置同版本幂等、降级拒绝与设备锁串行化。调用方无需持 info 锁。
//
// 判定顺序（每一步都会落到明确的设备终态，杜绝“发布单完成但设备还挂着 pending”）：
//  1. 记录已终态（installed/ready/rejected）-> 幂等返回；
//  2. 申请全局在装名额（ctx 取消则放弃，保持 pending 由上层回滚收敛）；
//  3. 锁定该设备的安装门（与其他发布单互斥），锁内重读当前版本：
//     - 已是目标版本 -> ready（无需安装）；
//     - 当前版本新于目标版本 -> rejected（低版本不得顶掉高版本）；
//     - 否则执行安装，成功后在锁内推进当前版本。
func (m *Manager) doInstall(ctx context.Context, info *releaseInfo, deviceID string, rec *installRecord) {
	rec.mu.Lock()
	switch rec.state {
	case DevInstalled, DevReady, DevRejected, DevKept:
		rec.mu.Unlock()
		return
	}
	rec.mu.Unlock()

	// 快速路径：进入发布时记录的 fromVer 已等于目标版本，先标 ready；
	// 锁内仍会以 m.current 权威值复核，避免并发期间版本变化。
	slot, ok := m.acquireSlot(ctx)
	if !ok {
		rec.mu.Lock()
		rec.lastError = ctx.Err().Error()
		rec.mu.Unlock()
		return
	}
	defer slot.release()

	// 锁序固定为 deviceGate -> m.mu：先在 m.mu 下取 gate 指针随即释放，
	// 再持有 gate 锁；gate 锁内若需读写 m.current 再短暂获取 m.mu。
	m.mu.Lock()
	gate := m.gateFor(deviceID)
	m.mu.Unlock()

	gate.mu.Lock()
	defer gate.mu.Unlock()

	m.mu.Lock()
	cur := m.current[deviceID]
	m.mu.Unlock()

	rec.mu.Lock()
	// 拿到设备锁后再次幂等检查（可能已被其他路径装好）。
	if rec.state == DevInstalled || rec.state == DevReady ||
		rec.state == DevRejected || rec.state == DevKept {
		rec.mu.Unlock()
		return
	}
	// 已被取消/接管：不再发起安装，保持 pending，由回滚阶段收敛为 skipped，
	// 避免“抢装成功又立刻回滚”的无效动作。
	if info.stop.Load() {
		rec.mu.Unlock()
		return
	}
	rec.state = DevInstalling
	rec.attempts++
	target := rec.targetVer
	rec.mu.Unlock()

	switch CompareVersions(cur, target) {
	case 0:
		// 设备本来就已经是目标版本：按“已就绪、无需安装”处理。
		rec.mu.Lock()
		rec.state = DevReady
		rec.mu.Unlock()
		m.log("设备已就绪 release=%s device=%s version=%s => state=ready（当前版本即目标版本，无需安装）",
			info.cfg.Version, deviceID, target)
		return
	case 1:
		// 当前版本新于目标版本：低版本下发被拒，绝不能把设备降级。
		rec.mu.Lock()
		rec.state = DevRejected
		rec.lastError = fmt.Sprintf("firmware: downgrade rejected: device %s on %s, target %s is older", deviceID, cur, target)
		rec.mu.Unlock()
		m.log("低版本下发被拒 release=%s device=%s current=%s target=%s => state=rejected（当前版本更高，禁止降级）",
			info.cfg.Version, deviceID, cur, target)
		return
	}

	from := cur
	if from == "" {
		from = rec.fromVer
	}
	err := m.install(ctx, deviceID, from, target)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if err != nil {
		rec.state = DevFailed
		rec.lastError = err.Error()
		return
	}
	rec.state = DevInstalled
	// 仍在 deviceGate 锁内：只有当前版本确实没被别的路径推走才推进。
	// 与 rollbackOne 遵循同一把设备锁，互斥串行，绝不覆盖更新的版本。
	m.mu.Lock()
	if curNow := m.current[deviceID]; CompareVersions(curNow, target) < 0 &&
		(curNow == "" || curNow == from) {
		m.current[deviceID] = target
	}
	m.mu.Unlock()
}

// rollback 将本次发布中已安装设备回滚到发布前版本。
//
// 接管保护（关键）：回滚前在设备锁内复核当前版本，只有“当前版本仍是本次目标版本”
// 才真正执行 target->from。设备若已被更高版本接管（并行高版本发布成功、或重试期间
// 其他路径升级），标记 kept 并直接返回——后续重试/取消/回滚都不能把它拽回旧版本。
func (m *Manager) rollback(info *releaseInfo) {
	info.mu.Lock()
	info.status = StatusRollingBack
	info.mu.Unlock()

	var wg sync.WaitGroup
	rbCap := normalizeCapacity(info.cfg.MaxConcurrency, DefaultMaxConcurrency)
	if rbCap > m.globalCap {
		rbCap = m.globalCap
	}
	sem := make(chan struct{}, rbCap)
	m.log("发布 %s 进入回滚: 回滚并发=%d（受全局上限%d约束）", info.cfg.Version, rbCap, m.globalCap)
	for id, rec := range info.devices {
		rec.mu.Lock()
		installed := rec.state == DevInstalled
		rec.mu.Unlock()
		if !installed {
			// 未装上的设备标记 skipped/kept，保持终态一致。
			rec.mu.Lock()
			switch rec.state {
			case DevPending, DevInstalling:
				rec.state = DevSkipped
			}
			rec.mu.Unlock()
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			m.rollbackOne(info, id, rec)
		}()
	}
	wg.Wait()
}

// rollbackOne 回滚单台设备，内含全局名额、设备锁与接管保护。
func (m *Manager) rollbackOne(info *releaseInfo, id string, rec *installRecord) {
	ctx := context.Background()
	slot, ok := m.acquireSlot(ctx)
	if !ok {
		return // ctx 是 background，正常不会到这里
	}
	defer slot.release()

	m.mu.Lock()
	gate := m.gateFor(id)
	cur := m.current[id]
	m.mu.Unlock()

	gate.mu.Lock()
	defer gate.mu.Unlock()

	m.mu.Lock()
	cur = m.current[id]
	m.mu.Unlock()

	rec.mu.Lock()
	target := rec.targetVer
	from := rec.fromVer
	rec.mu.Unlock()

	// 接管保护：设备已不在被回滚版本上，说明被更高版本接管，保留现状。
	if cur != target {
		rec.mu.Lock()
		rec.state = DevKept
		rec.lastError = ""
		rec.mu.Unlock()
		m.log("回滚保留 release=%s device=%s 当前=%s 被回滚版本=%s => state=kept（设备已被更高版本接管，禁止拽回 %s）",
			info.cfg.Version, id, cur, target, from)
		return
	}

	rberr := m.install(ctx, id, target, from)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rberr != nil {
		rec.state = DevFailed
		rec.lastError = "rollback: " + rberr.Error()
		info.mu.Lock()
		info.rbErrs = append(info.rbErrs, fmt.Sprintf("%s: %v", id, rberr))
		info.mu.Unlock()
		return
	}
	rec.state = DevRolledBack
	m.mu.Lock()
	// 双保险：只有当前版本确实仍等于被回滚版本才写回旧版本。
	if m.current[id] == target {
		if from == "" {
			delete(m.current, id)
		} else {
			m.current[id] = from
		}
	}
	m.mu.Unlock()
}

// Offer 向单设备投递安装；同设备同版本重复投递幂等跳过。
// 仅当发布单存在且处于 rolling 状态时可投递。
func (m *Manager) Offer(version, deviceID string) (DeviceStatus, error) {
	m.mu.Lock()
	info := m.rel[version]
	m.mu.Unlock()
	if info == nil {
		return DevSkipped, fmt.Errorf("%w: %s", ErrReleaseNotFound, version)
	}
	info.mu.Lock()
	if info.status != StatusRolling {
		st := info.status
		info.mu.Unlock()
		return DevSkipped, fmt.Errorf("%w: release=%s status=%s", ErrAlreadyTerminal, version, st)
	}
	rec := info.devices[deviceID]
	info.mu.Unlock()
	if rec == nil {
		return DevSkipped, fmt.Errorf("device %s not part of release %s", deviceID, version)
	}
	rec.mu.Lock()
	state := rec.state
	rec.mu.Unlock()
	switch state {
	case DevInstalled, DevReady, DevKept:
		return state, nil // 幂等：已完成/已就绪/被更高版本保留
	case DevRejected:
		// 低版本被拒是确定性裁决，重复 Offer 不重试安装。
		return DevRejected, nil
	}
	m.doInstall(context.Background(), info, deviceID, rec)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.state, nil
}

// Cancel 请求取消一次进行中的发布（协作式，已安装部分随后回滚）。
func (m *Manager) Cancel(version string) error {
	m.mu.Lock()
	info := m.rel[version]
	m.mu.Unlock()
	if info == nil {
		return fmt.Errorf("%w: %s", ErrReleaseNotFound, version)
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if isTerminal(info.status) {
		return fmt.Errorf("%w: %s", ErrAlreadyTerminal, info.status)
	}
	info.stop.Store(true)
	info.stopReason = "cancelled"
	info.status = StatusRollingBack
	return nil
}

// Supersede 将旧版本发布单标记为被更高版本取代并停止、回滚。
// newVersion 必须确实新于 oldVersion，否则返回 ErrNotNewerVersion。
// 回滚阶段对已被新发布安装成功的设备执行接管保护（kept），不拽回旧版。
func (m *Manager) Supersede(oldVersion, newVersion string) error {
	if c := CompareVersions(newVersion, oldVersion); c <= 0 {
		m.log("接管裁决拒绝 supersede old=%s new=%s => 比较结果=%d（新版本必须更高）",
			oldVersion, newVersion, c)
		return fmt.Errorf("%w: %s does not supersede %s", ErrNotNewerVersion, newVersion, oldVersion)
	}
	m.mu.Lock()
	info := m.rel[oldVersion]
	m.mu.Unlock()
	if info == nil {
		return fmt.Errorf("%w: %s", ErrReleaseNotFound, oldVersion)
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if isTerminal(info.status) {
		return fmt.Errorf("%w: %s", ErrAlreadyTerminal, info.status)
	}
	info.stop.Store(true)
	info.stopReason = "superseded"
	info.status = StatusRollingBack
	info.reason = "superseded_by:" + newVersion
	m.log("接管生效 release=%s 被 %s 取代 => 停止下发并回滚（已接管设备回滚时保留新版）",
		oldVersion, newVersion)
	return nil
}

// StatusOf 查询发布单状态。
func (m *Manager) StatusOf(version string) (Status, bool) {
	m.mu.Lock()
	info := m.rel[version]
	m.mu.Unlock()
	if info == nil {
		return StatusPending, false
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.status, true
}

// DeviceState 查询某设备在某发布单上的安装状态与当前版本。
func (m *Manager) DeviceState(version, deviceID string) (DeviceStatus, string, bool) {
	m.mu.Lock()
	info := m.rel[version]
	cur := m.current[deviceID]
	m.mu.Unlock()
	if info == nil {
		return DevSkipped, cur, false
	}
	rec := info.devices[deviceID]
	if rec == nil {
		return DevSkipped, cur, false
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.state, cur, true
}

func (m *Manager) snapshotLocked(info *releaseInfo) *Release {
	counts := make(map[DeviceStatus]int)
	for _, rec := range info.devices {
		rec.mu.Lock()
		counts[rec.state]++
		rec.mu.Unlock()
	}
	return &Release{
		Version:        info.cfg.Version,
		Status:         info.status,
		StartedAt:      info.startedAt,
		FailureCount:   info.failures,
		RollbackErrors: append([]string(nil), info.rbErrs...),
		Reason:         info.reason,
		DeviceCounts:   counts,
	}
}

func (info *releaseInfo) setStopReason(r string) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.stopReason == "" {
		info.stopReason = r
	}
}

func validate(cfg Config) error {
	if cfg.Version == "" {
		return errors.New("firmware: empty version")
	}
	if len(cfg.Batches) == 0 {
		return ErrNoBatches
	}
	// MaxConcurrency / FailureThreshold 的兜底由 Publish 统一处理：
	// 缺失/0/负数不会拒绝发布，而是回落到安全默认值。
	return nil
}

func isTerminal(s Status) bool {
	switch s {
	case StatusPublished, StatusRolledBack, StatusCancelled, StatusSuperseded:
		return true
	}
	return false
}
