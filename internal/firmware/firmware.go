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
}

// Errors
var (
	ErrAlreadyPublished = errors.New("firmware: version already exists")
	ErrNoBatches        = errors.New("firmware: no batches in config")
	ErrReleaseNotFound  = errors.New("firmware: release not found")
	ErrAlreadyTerminal  = errors.New("firmware: release already in terminal state")
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
	// 发布治理：全局安装容量（跨所有并行发布单）与关键判定日志。
	globalSem      chan struct{}
	globalInflight atomic.Int32
	logf           func(format string, args ...any)
}

// NewManager 创建发布管理器（全局并发取默认安全值 DefaultGlobalMaxConcurrency）。
func NewManager(install InstallFunc, opts ...ManagerOption) *Manager {
	o := managerOptions{globalMaxConcurrency: DefaultGlobalMaxConcurrency}
	for _, opt := range opts {
		opt(&o)
	}
	g := normalizeConcurrency(o.globalMaxConcurrency, DefaultGlobalMaxConcurrency)
	logf := o.logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Manager{
		install:   install,
		rel:       make(map[string]*releaseInfo),
		current:   make(map[string]string),
		globalSem: make(chan struct{}, g),
		logf:      logf,
	}
}

func (m *Manager) log(format string, args ...any) { m.logf(format, args...) }

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
	// 容量参数兜底：未填/0/负数统一归一化为安全值，避免负容量 panic 或无界并发。
	cfg.MaxConcurrency = normalizeConcurrency(cfg.MaxConcurrency, DefaultPerReleaseConcurrency)
	cfg.FailureThreshold = normalizeConcurrency(cfg.FailureThreshold, 1)
	m.mu.Lock()
	if _, exists := m.rel[cfg.Version]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrAlreadyPublished, cfg.Version)
	}
	// 准入裁决 1：任何目标设备当前已在严格更高的已发布版本上 -> 整单拒绝，
	// 低版本发布不允许顶掉高版本（已就绪的同版本设备不受影响，下面按 ready 处理）。
	targetDevices := make(map[string]bool)
	for _, b := range cfg.Batches {
		for _, id := range b.DeviceIDs {
			targetDevices[id] = true
		}
	}
	for id := range targetDevices {
		cur := m.current[id]
		if CompareVersions(cur, cfg.Version) > 0 {
			m.mu.Unlock()
			m.log("低版本发布被拒 version=%s device=%s current=%s => %v",
				cfg.Version, id, cur, ErrLowerVersionRejected)
			return nil, fmt.Errorf("%w: device %s on %s, target %s",
				ErrLowerVersionRejected, id, cur, cfg.Version)
		}
	}
	// 准入裁决 2：高版本发布自动接管仍在进行中的低版本发布单（设备集合有交集即可）。
	for _, other := range m.rel {
		if other.cfg.Version == cfg.Version || CompareVersions(other.cfg.Version, cfg.Version) >= 0 {
			continue
		}
		other.mu.Lock()
		active := !isTerminal(other.status)
		overlaps := false
		if active {
			for id := range other.devices {
				if targetDevices[id] {
					overlaps = true
					break
				}
			}
		}
		if overlaps {
			other.stop.Store(true)
			other.stopReason = "superseded"
			other.status = StatusRollingBack
			other.reason = "superseded_by:" + cfg.Version
		}
		other.mu.Unlock()
		if overlaps {
			m.log("高版本接管 old=%s new=%s => 旧发布停止，回滚时设备若已在 %s 则保持",
				other.cfg.Version, cfg.Version, cfg.Version)
		}
	}
	// 记录每个设备进入本次发布前的版本，用于幂等与回滚。
	devs := make(map[string]*installRecord)
	for _, b := range cfg.Batches {
		for _, id := range b.DeviceIDs {
			if _, dup := devs[id]; dup {
				continue
			}
			state := DevPending
			if m.current[id] == cfg.Version {
				// 设备已是目标版本：已就绪，无需安装。
				state = DevReady
			}
			devs[id] = &installRecord{
				state:     state,
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
	sem := make(chan struct{}, cfg.MaxConcurrency)
	m.log("发布受理 version=%s 设备数=%d 批次数=%d 单发布并发=%d 失败阈值=%d",
		cfg.Version, len(devs), len(cfg.Batches), cfg.MaxConcurrency, threshold)

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
	for _, batch := range cfg.Batches {
		var wg sync.WaitGroup
		var failCnt, rejectCnt atomic.Int32
		for _, id := range batch.DeviceIDs {
			if info.stop.Load() {
				break
			}
			rec := info.devices[id]
			// 准入时已就绪的设备无需安装，直接跳过（不占并发名额）。
			if rec.snapshotState() == DevReady {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				m.doInstall(ctx, info, id, rec)
				switch rec.snapshotState() {
				case DevFailed:
					failCnt.Add(1)
				case DevRejected:
					failCnt.Add(1)
					rejectCnt.Add(1)
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
			if rejectCnt.Load() > 0 {
				info.setStopReason("version_rejected")
				batchErr = fmt.Errorf("firmware: batch %q rejected: %d devices already on higher version", batch.Name, rejectCnt.Load())
			} else {
				info.setStopReason("failure_threshold")
				batchErr = fmt.Errorf("firmware: batch %q failures %d >= threshold %d", batch.Name, failCnt.Load(), threshold)
			}
			break batchLoop
		}
	}
	close(watchDone)

	terminal := StatusPublished
	outReason := "done"
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

	// 终态一致性兜底：不允许发布单完成后仍有设备挂在 pending/installing。
	m.reconcileTerminal(info)
	m.log("发布收敛 version=%s => status=%s reason=%s failures=%d 回滚错误=%d",
		cfg.Version, terminal, outReason, info.failures, len(info.rbErrs))

	info.mu.Lock()
	info.status = terminal
	info.reason = outReason
	r := m.snapshotLocked(info)
	info.mu.Unlock()
	return r, batchErr
}

// doInstall 完成单设备安装，内置同版本幂等与状态流转。调用方无需持 info 锁。
func (m *Manager) doInstall(ctx context.Context, info *releaseInfo, deviceID string, rec *installRecord) {
	rec.mu.Lock()
	// 幂等：设备记录已为 installed，直接跳过。
	if rec.state == DevInstalled {
		rec.mu.Unlock()
		return
	}
	rec.mu.Unlock()

	m.mu.Lock()
	cur := m.current[deviceID]
	m.mu.Unlock()
	if cur == rec.targetVer {
		// 设备当前已是目标版本：按“已就绪、无需安装”处理，不占安装名额。
		rec.mu.Lock()
		rec.state = DevReady
		rec.mu.Unlock()
		m.log("设备已就绪 device=%s version=%s => 无需安装", deviceID, rec.targetVer)
		return
	}

	// 版本裁决：只能向前，不能把设备从更高的已发布版本拽回旧版本。
	if CompareVersions(cur, rec.targetVer) > 0 {
		rec.mu.Lock()
		rec.state = DevRejected
		rec.lastError = fmt.Sprintf("governance: current %s higher than target %s", cur, rec.targetVer)
		rec.mu.Unlock()
		m.log("低版本下发被拒 device=%s current=%s target=%s => 保持高版本", deviceID, cur, rec.targetVer)
		return
	}

	rec.mu.Lock()
	rec.state = DevInstalling
	rec.attempts++
	rec.mu.Unlock()

	// 真正开始安装前占用全局名额，保证跨并行发布单的安装总数不被顶穿。
	if err := m.acquireGlobalSlot(ctx); err != nil {
		rec.mu.Lock()
		rec.state = DevFailed
		rec.lastError = err.Error()
		rec.mu.Unlock()
		return
	}
	defer m.releaseGlobalSlot()

	from := cur
	if from == "" {
		from = rec.fromVer
	}
	err := m.install(ctx, deviceID, from, rec.targetVer)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if err != nil {
		rec.state = DevFailed
		rec.lastError = err.Error()
		return
	}
	rec.state = DevInstalled
	m.mu.Lock()
	// 双检：安装函数返回后若设备已被其他路径更新到更高版本，不覆盖。
	if CompareVersions(m.current[deviceID], rec.targetVer) > 0 {
		rec.state = DevKept
	} else {
		m.current[deviceID] = rec.targetVer
	}
	m.mu.Unlock()
}

// rollback 将本次发布中已安装、且当前仍停留在本发布目标版本上的设备回滚到发布前版本。
// 关键裁决：若设备当前已经在更高的已发布版本上（被更高版本接管），则保持现状，
// 状态记 kept，绝不执行旧版本回滚安装。
func (m *Manager) rollback(info *releaseInfo) {
	info.mu.Lock()
	info.status = StatusRollingBack
	cfg := info.cfg
	info.mu.Unlock()

	var wg sync.WaitGroup
	sem := make(chan struct{}, normalizeConcurrency(cfg.MaxConcurrency, DefaultPerReleaseConcurrency))
	for id, rec := range info.devices {
		rec.mu.Lock()
		installed := rec.state == DevInstalled
		rec.mu.Unlock()
		if !installed {
			// 未装上的设备标记 skipped，保持终态一致
			rec.mu.Lock()
			if rec.state == DevPending || rec.state == DevInstalling {
				rec.state = DevSkipped
			}
			rec.mu.Unlock()
			continue
		}

		m.mu.Lock()
		cur := m.current[id]
		m.mu.Unlock()
		// 版本裁决：设备已不在本发布目标版本上（通常已被更高版本接管），
		// 不做任何可能降级的安装，保持当前已发布版本。
		if cur != rec.targetVer {
			rec.mu.Lock()
			rec.state = DevKept
			rec.mu.Unlock()
			m.log("回滚被版本裁决拦住 device=%s current=%s 目标版本=%s 发布前=%s => 保持 current",
				id, cur, rec.targetVer, rec.fromVer)
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			// 回滚安装同样占用全局名额，避免多个发布单同时回滚顶穿容量。
			if err := m.acquireGlobalSlot(context.Background()); err != nil {
				rec.mu.Lock()
				rec.state = DevFailed
				rec.lastError = "rollback: " + err.Error()
				rec.mu.Unlock()
				return
			}
			defer m.releaseGlobalSlot()

			rberr := m.install(context.Background(), id, rec.targetVer, rec.fromVer)
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
			if rec.fromVer == "" {
				delete(m.current, id)
			} else {
				m.current[id] = rec.fromVer
			}
			m.mu.Unlock()
		}()
	}
	wg.Wait()
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
	case DevInstalled:
		return DevInstalled, nil // 幂等：已安装
	case DevReady:
		return DevReady, nil // 已就绪：当前版本即目标版本，无需安装
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
// 版本裁决：newVersion 必须严格高于 oldVersion，否则拒绝，
// 避免低版本借“接管”名义顶掉高版本。
func (m *Manager) Supersede(oldVersion, newVersion string) error {
	if CompareVersions(newVersion, oldVersion) <= 0 {
		m.log("接管被版本裁决拒绝 old=%s new=%s => %v", oldVersion, newVersion, ErrInvalidVersionOrder)
		return fmt.Errorf("%w: %s is not higher than %s", ErrInvalidVersionOrder, newVersion, oldVersion)
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
	return &Release{
		Version:        info.cfg.Version,
		Status:         info.status,
		StartedAt:      info.startedAt,
		FailureCount:   info.failures,
		RollbackErrors: append([]string(nil), info.rbErrs...),
		Reason:         info.reason,
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
	return nil
}

func isTerminal(s Status) bool {
	switch s {
	case StatusPublished, StatusRolledBack, StatusCancelled, StatusSuperseded:
		return true
	}
	return false
}
