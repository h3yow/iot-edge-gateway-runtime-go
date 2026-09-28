package firmware

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// 发布状态。
const (
	ReleasePending    = "pending"    // 已登记未发布
	ReleasePublishing = "publishing" // 发布进行中
	ReleasePartial    = "partial"    // 有设备完成但发布尚未终态
	ReleaseComplete   = "complete"   // 全部目标处理完成
	ReleaseFailed     = "failed"     // 失败率超阈值，已回滚
	ReleaseCancelled  = "cancelled"  // 被取消，已安装设备已回滚
	ReleaseSuperseded = "superseded" // 被更高版本发布取代
)

// 单设备安装状态。
const (
	InstallPending    = "pending"     // 尚未下发
	InstallOffered    = "offered"     // 已下发，等待回执
	InstallSuccess    = "success"     // 安装成功
	InstallFailed     = "failed"      // 安装失败
	InstallSkipped    = "skipped"     // 已装相同版本，幂等跳过
	InstallRolledBack = "rolled_back" // 已安装但随发布终止被回滚
	InstallCancelled  = "cancelled"   // 尚未下发即随发布终止
)

// Progress.Reason 取值。
const (
	ReasonComplete          = "complete"
	ReasonFailureRateAbort  = "failure_rate_exceeded"
	ReasonCancelled         = "cancelled"
	ReasonSuperseded        = "superseded"
	ReasonAlreadyTerminal   = "already_terminal"
	ReasonAlreadyPublishing = "already_publishing"
)

var (
	ErrReleaseNotFound = errors.New("firmware: release not found")
	ErrNoDevices       = errors.New("firmware: no target devices")
	ErrNotPublishing   = errors.New("firmware: release is not publishing")
	ErrInvalidArg      = errors.New("firmware: invalid argument")
	ErrAlreadyTerminal = errors.New("firmware: release already in terminal state")
	ErrVersionConflict = errors.New("firmware: superseding version must be strictly higher")
)

// Installer 是设备侧安装执行器（由网关注入模拟实现）。
// 返回 status 为 InstallSuccess 或 InstallFailed，detail 供日志/审计解释。
type Installer interface {
	Install(deviceID, releaseID, version string) (status string, detail string)
}

// Release 描述一次固件发布。
type Release struct {
	ID         string
	Version    string
	BatchSize  int
	CanaryPct  int // 灰度比例（0-100）：首批仅覆盖该比例的设备
	Status     string
	Targets    []string
	CreatedAt  time.Time
	FinishedAt time.Time
	FailReason string
}

// InstallRecord 记录某设备在某次发布中的安装状态。
type InstallRecord struct {
	DeviceID string
	Version  string
	Status   string
	Detail   string
}

// Progress 是一次发布推进/终止的结果摘要。
type Progress struct {
	ReleaseID  string
	Status     string
	Reason     string
	Offered    int
	Succeeded  int
	Failed     int
	Skipped    int
	RolledBack int
	Cancelled  int
}

type releaseState struct {
	meta     Release
	mu       sync.Mutex // 单发布状态锁，保证取消/回执/推进并发下状态一致
	records  map[string]*InstallRecord
	order    []string
	cancel   chan struct{}
	started  bool
	terminal bool
}

// Manager 管理固件分批灰度发布与回滚。
type Manager struct {
	mu sync.Mutex

	releases map[string]*releaseState

	// installed 记录每个设备“当前生效版本”，用于跨发布的安装幂等判定；
	// previous 记录上一版本，回滚时恢复，保证设备版本与发布状态一致。
	installed map[string]string
	previous  map[string]string

	// 设备级串行锁：同一设备同一时刻只能被一个发布安装，
	// 即使两个发布并发推进，也不会出现重复/交叉安装。
	deviceLocks map[string]*sync.Mutex

	installSem chan struct{} // 并发安装上限
	publishSem chan struct{} // 并发发布上限
	installer  Installer
}

// New 创建发布管理器；maxConcurrent 同时限制并发发布数与单批发并发安装数。
func New(maxConcurrent int, installer Installer) *Manager {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &Manager{
		releases:    make(map[string]*releaseState),
		installed:   make(map[string]string),
		previous:    make(map[string]string),
		deviceLocks: make(map[string]*sync.Mutex),
		installSem:  make(chan struct{}, maxConcurrent),
		publishSem:  make(chan struct{}, maxConcurrent),
		installer:   installer,
	}
}

// CreateRelease 登记一次发布（目标去重、参数校验）。
func (m *Manager) CreateRelease(id, version string, batchSize, canaryPct int, targets []string) (*Release, error) {
	if id == "" || version == "" {
		return nil, fmt.Errorf("%w: release id and version are required", ErrInvalidArg)
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("%w: batch size must be positive", ErrInvalidArg)
	}
	if canaryPct < 0 || canaryPct > 100 {
		return nil, fmt.Errorf("%w: canary percent must be in [0,100]", ErrInvalidArg)
	}
	if len(targets) == 0 {
		return nil, ErrNoDevices
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.releases[id]; exists {
		return nil, fmt.Errorf("%w: release %q already exists", ErrInvalidArg, id)
	}
	uniq := dedup(targets)
	rs := &releaseState{
		meta: Release{
			ID: id, Version: version, BatchSize: batchSize,
			CanaryPct: canaryPct, Status: ReleasePending,
			Targets: uniq, CreatedAt: time.Now(),
		},
		records: make(map[string]*InstallRecord, len(uniq)),
		order:   uniq,
		cancel:  make(chan struct{}),
	}
	for _, dev := range uniq {
		rs.records[dev] = &InstallRecord{DeviceID: dev, Version: version, Status: InstallPending}
	}
	m.releases[id] = rs
	out := rs.meta
	return &out, nil
}

// Publish 推进一次发布。
//
// 调度：先发灰度波（ceil(N*canaryPct/100) 台，至少 1 台当 canaryPct>0），
// 灰度波成功后其余设备按 batchSize 逐批推进；批内并发受 maxConcurrent 控制。
// 每台设备安装后都检查：失败率是否超阈值、发布是否被取消/取代。
// 一旦终止：成功设备回滚到上一版本，未下发设备标记 cancelled，状态收敛到终态。
func (m *Manager) Publish(releaseID string, failRateThreshold int) (Progress, error) {
	if failRateThreshold < 0 || failRateThreshold > 100 {
		return Progress{}, fmt.Errorf("%w: fail rate threshold must be in [0,100]", ErrInvalidArg)
	}
	rs, err := m.get(releaseID)
	if err != nil {
		return Progress{}, err
	}

	// 并发发布受控：信号量限制同时进行的发布数量。
	select {
	case m.publishSem <- struct{}{}:
		defer func() { <-m.publishSem }()
	default:
		return Progress{ReleaseID: releaseID, Status: rs.meta.Status, Reason: "concurrency_busy"},
			fmt.Errorf("%w: concurrency limit reached", ErrInvalidArg)
	}

	rs.mu.Lock()
	if rs.terminal {
		status, reason := rs.meta.Status, ReasonAlreadyTerminal
		rs.mu.Unlock()
		return m.snapshotProgress(rs, status, reason), ErrAlreadyTerminal
	}
	if rs.started {
		rs.mu.Unlock()
		return m.snapshotProgress(rs, rs.meta.Status, ReasonAlreadyPublishing),
			fmt.Errorf("%w: release is already publishing", ErrNotPublishing)
	}
	rs.started = true
	rs.meta.Status = ReleasePublishing
	rs.mu.Unlock()

	// 构造灰度波 + 后续批次。
	waves := m.buildWaves(rs.order, rs.meta.BatchSize, rs.meta.CanaryPct)
	for _, wave := range waves {
		if terminated, prog, errp := m.processWave(rs, wave, failRateThreshold); terminated {
			return prog, errp
		}
	}

	rs.mu.Lock()
	rs.meta.Status = ReleaseComplete
	rs.meta.FinishedAt = time.Now()
	rs.terminal = true
	p := m.progressLocked(rs, ReleaseComplete, ReasonComplete)
	rs.mu.Unlock()
	return p, nil
}

// processWave 推进一批设备（批内并发、设备级串行）。
// 终止判定放在波次边界：整波安装结束后统一计算失败率，避免“波内先完成
// 的设备触发中止”饿死同波尚未开始的设备。波进行中若收到取消信号，
// 则未开始的设备不再下发。
func (m *Manager) processWave(rs *releaseState, wave []string, failRateThreshold int) (bool, Progress, error) {
	var wg sync.WaitGroup
	for _, dev := range wave {
		dev := dev
		// 取消/取代在下发前快速失败，避免无谓安装。
		rs.mu.Lock()
		cancelledNow := rs.terminal || isClosed(rs.cancel)
		rs.mu.Unlock()
		if cancelledNow {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.installSem <- struct{}{}
			defer func() { <-m.installSem }()
			m.installOne(rs, dev)
		}()
	}
	wg.Wait()

	// 波次边界统一判定终止原因（优先级：外部取消/取代 > 失败率）。
	rs.mu.Lock()
	if rs.terminal {
		p := m.progressLocked(rs, rs.meta.Status, "converged_during_wave")
		rs.mu.Unlock()
		return true, p, nil
	}
	processed, failed, succeeded := 0, 0, 0
	for _, r := range rs.records {
		switch r.Status {
		case InstallSuccess, InstallFailed, InstallSkipped, InstallRolledBack:
			processed++
		}
		switch r.Status {
		case InstallFailed:
			failed++
		case InstallSuccess:
			succeeded++
		}
	}
	cancelRequested := isClosed(rs.cancel)
	rs.mu.Unlock()

	if cancelRequested {
		p, err := m.abort(rs, ReleaseCancelled, ReasonCancelled, "cancelled by operator")
		return true, p, err
	}
	if processed > 0 && failed*100 > failRateThreshold*processed {
		p, err := m.abort(rs, ReleaseFailed, ReasonFailureRateAbort,
			fmt.Sprintf("failure rate %d failed / %d processed (%d succeeded) exceeded threshold %d%%",
				failed, processed, succeeded, failRateThreshold))
		return true, p, err
	}
	return false, Progress{}, nil
}

// installOne 对单台设备执行一次安装，封装设备级互斥与跨发布幂等。
func (m *Manager) installOne(rs *releaseState, dev string) {
	devLock := m.deviceLock(dev)
	devLock.Lock()
	defer devLock.Unlock()

	version := rs.meta.Version
	rs.mu.Lock()
	rec := rs.records[dev]
	// 已在本次发布中进入终态（例如取消与完成竞争），或发布已被取消/取代：
	// 不重复安装。
	if rs.terminal || (rec.Status != InstallPending && rec.Status != InstallOffered) {
		rs.mu.Unlock()
		return
	}
	current := m.installed[dev]
	if current == version {
		// 同一设备重复收到同一版本：跳过，不调用安装器，杜绝重复安装。
		rec.Status = InstallSkipped
		rec.Detail = fmt.Sprintf("version %s already installed, idempotent skip", version)
		rs.mu.Unlock()
		return
	}
	rec.Status = InstallOffered
	rs.mu.Unlock()

	status, detail := m.installer.Install(dev, rs.meta.ID, version)

	rs.mu.Lock()
	defer rs.mu.Unlock()
	// 安装进行期间发布可能已被另一台设备的失败/取消收敛：
	// 回执不能覆盖终态，成功的设备就地回滚，保证设备版本与发布状态一致。
	if rs.terminal {
		if status == InstallSuccess {
			m.installed[dev] = current
			rec.Status = InstallRolledBack
			rec.Detail = "rolled back after late completion: " + detail
		} else {
			rec.Status = InstallFailed
			rec.Detail = detail
		}
		return
	}
	if status == InstallSuccess {
		m.previous[dev] = current
		m.installed[dev] = version
		rec.Status = InstallSuccess
	} else {
		rec.Status = InstallFailed
	}
	rec.Detail = detail
	if rs.meta.Status == ReleasePublishing {
		rs.meta.Status = ReleasePartial
	}
}

// ReportInstall 供异步回执使用；同一 (发布,设备) 重复回执幂等，
// 已终态的记录不会被二次改写。
func (m *Manager) ReportInstall(releaseID, deviceID, version, status, detail string) error {
	rs, err := m.get(releaseID)
	if err != nil {
		return err
	}
	if status != InstallSuccess && status != InstallFailed {
		return fmt.Errorf("%w: report status must be success or failed", ErrInvalidArg)
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rec, ok := rs.records[deviceID]
	if !ok {
		return fmt.Errorf("%w: device %q is not a target of release %q", ErrInvalidArg, deviceID, releaseID)
	}
	if rec.Status == InstallSuccess || rec.Status == InstallFailed ||
		rec.Status == InstallRolledBack || rec.Status == InstallSkipped {
		return nil // 幂等：重复回执不改变状态
	}
	if version != rs.meta.Version {
		return fmt.Errorf("%w: report version %q does not match release version %q",
			ErrVersionConflict, version, rs.meta.Version)
	}
	rec.Status, rec.Detail = status, detail
	if status == InstallSuccess {
		m.installed[deviceID] = version
	}
	return nil
}

// Cancel 请求取消发布：
//   - 尚未开始：同步收敛为 cancelled 终态；
//   - 进行中：仅设置取消信号并立即返回（inProgress=true）。正在推进的
//     Publish 会在当前批次结束后的波次边界完成回滚收敛，避免与安装器
//     形成等待环。调用方可用 WaitTerminal 等待最终状态；
//   - 已终态：返回 ErrAlreadyTerminal，状态不被破坏。
func (m *Manager) Cancel(releaseID, reason string) (progress Progress, inProgress bool, err error) {
	rs, err := m.get(releaseID)
	if err != nil {
		return Progress{}, false, err
	}
	rs.mu.Lock()
	if rs.terminal {
		p := m.progressLocked(rs, rs.meta.Status, ReasonAlreadyTerminal)
		rs.mu.Unlock()
		return p, false, ErrAlreadyTerminal
	}
	if !rs.started {
		rs.mu.Unlock()
		p, e := m.abort(rs, ReleaseCancelled, ReasonCancelled, reason)
		return p, false, e
	}
	if !isClosed(rs.cancel) {
		close(rs.cancel)
	}
	p := m.progressLocked(rs, ReleasePublishing, "cancel_signalled")
	rs.mu.Unlock()
	return p, true, nil
}

// WaitTerminal 等待发布进入终态并返回最终进度（供取消/异步场景会合）。
func (m *Manager) WaitTerminal(releaseID string, timeout time.Duration) (Progress, error) {
	rs, err := m.get(releaseID)
	if err != nil {
		return Progress{}, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rs.mu.Lock()
		done := rs.terminal
		p := m.progressLocked(rs, rs.meta.Status, rs.meta.FailReason)
		rs.mu.Unlock()
		if done {
			return p, nil
		}
		time.Sleep(time.Millisecond)
	}
	return Progress{}, fmt.Errorf("firmware: release %s did not settle within %s", releaseID, timeout)
}

// Supersede 用更高版本发布取代旧发布。
// 旧发布必须处于活动态；新版本号必须严格更高；旧发布的成功设备回滚、
// 未下发设备取消，整体标记 superseded，与新发布并存且状态一致。
func (m *Manager) Supersede(oldReleaseID, newReleaseID string) error {
	oldRS, err := m.get(oldReleaseID)
	if err != nil {
		return err
	}
	newRS, err := m.get(newReleaseID)
	if err != nil {
		return err
	}
	oldRS.mu.Lock()
	newRS.mu.Lock()
	defer newRS.mu.Unlock()
	defer oldRS.mu.Unlock()
	// 允许取代进行中或已完成（complete）的发布：完成态发布的成功设备会
	// 随取代动作回滚到上一版本；failed/cancelled 已是终态事实，不可再改。
	if oldRS.meta.Status == ReleaseFailed || oldRS.meta.Status == ReleaseCancelled ||
		oldRS.meta.Status == ReleaseSuperseded {
		return fmt.Errorf("%w: old release in irreversible state %s", ErrAlreadyTerminal, oldRS.meta.Status)
	}
	if compareVersions(newRS.meta.Version, oldRS.meta.Version) <= 0 {
		return fmt.Errorf("%w: new version %s must be higher than %s",
			ErrVersionConflict, newRS.meta.Version, oldRS.meta.Version)
	}
	m.terminateLocked(oldRS, ReleaseSuperseded, ReasonSuperseded,
		"superseded by release "+newReleaseID)
	if !isClosed(oldRS.cancel) {
		close(oldRS.cancel)
	}
	return nil
}

// abort 在发布中途终止并执行回滚（失败/取消的统一收敛路径）。
func (m *Manager) abort(rs *releaseState, status, reason, detail string) (Progress, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.terminal {
		return m.progressLocked(rs, rs.meta.Status, "already_terminal"), nil
	}
	m.terminateLocked(rs, status, reason, detail)
	if !isClosed(rs.cancel) {
		close(rs.cancel)
	}
	return m.progressLocked(rs, status, reason), nil
}

// terminateLocked 收敛单发布状态（调用方持有 rs.mu）。
//
// 状态一致性规则：
//   - failed/cancelled：成功设备回滚到发布前版本（同步恢复 installed），
//     未下发/在途设备标记 cancelled，失败设备保留 failed 事实；
//   - superseded：成功设备若当前已运行“取代它的新版本”，保持 success
//     （升级目标已达成，跨发布回滚是错误的）；若仍停留在被取代版本，
//     才回滚到发布前版本；未下发设备标记 cancelled。
func (m *Manager) terminateLocked(rs *releaseState, status, reason, detail string) {
	for _, dev := range rs.order {
		rec := rs.records[dev]
		switch rec.Status {
		case InstallSuccess:
			rollback := true
			if status == ReleaseSuperseded && m.installed[dev] != rs.meta.Version {
				// 设备已被新版本发布接管，保留成功事实。
				rollback = false
				rec.Detail = "kept: device already on newer version after supersede (" + detail + ")"
			}
			if rollback {
				rec.Status = InstallRolledBack
				rec.Detail = "rolled back after release " + status + ": " + detail
				if m.installed[dev] == rs.meta.Version {
					m.installed[dev] = m.previous[dev]
				}
			}
		case InstallPending, InstallOffered:
			rec.Status = InstallCancelled
			rec.Detail = "release " + status + " before install: " + detail
		}
	}
	rs.meta.Status = status
	rs.meta.FailReason = detail
	rs.meta.FinishedAt = time.Now()
	rs.terminal = true
}

// InstalledVersion 返回设备当前生效版本（测试/演示可观测）。
func (m *Manager) InstalledVersion(deviceID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.installed[deviceID]
}

// Get 查询发布元数据副本。
func (m *Manager) Get(releaseID string) (*Release, error) {
	rs, err := m.get(releaseID)
	if err != nil {
		return nil, err
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := rs.meta
	return &out, nil
}

// Installs 查询发布内全部安装记录（按目标顺序）。
func (m *Manager) Installs(releaseID string) []InstallRecord {
	rs, err := m.get(releaseID)
	if err != nil {
		return nil
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]InstallRecord, 0, len(rs.order))
	for _, dev := range rs.order {
		out = append(out, *rs.records[dev])
	}
	return out
}

// ReleaseCount 返回发布总数。
func (m *Manager) ReleaseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.releases)
}

func (m *Manager) get(id string) (*releaseState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.releases[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReleaseNotFound, id)
	}
	return rs, nil
}

func (m *Manager) deviceLock(dev string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.deviceLocks[dev]
	if !ok {
		l = &sync.Mutex{}
		m.deviceLocks[dev] = l
	}
	return l
}

// buildWaves 切分灰度波与常规批次。
func (m *Manager) buildWaves(targets []string, batchSize, canaryPct int) [][]string {
	waves := [][]string{}
	rest := targets
	if canaryPct > 0 {
		n := (len(targets)*canaryPct + 99) / 100
		if n < 1 {
			n = 1
		}
		if n > len(targets) {
			n = len(targets)
		}
		waves = append(waves, targets[:n])
		rest = targets[n:]
	}
	for len(rest) > 0 {
		end := batchSize
		if end > len(rest) {
			end = len(rest)
		}
		waves = append(waves, rest[:end])
		rest = rest[end:]
	}
	return waves
}

func (m *Manager) snapshotProgress(rs *releaseState, status, reason string) Progress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return m.progressLocked(rs, status, reason)
}

func (m *Manager) progressLocked(rs *releaseState, status, reason string) Progress {
	p := Progress{ReleaseID: rs.meta.ID, Status: status, Reason: reason}
	for _, dev := range rs.order {
		switch rs.records[dev].Status {
		case InstallOffered, InstallSuccess:
			p.Offered++
		}
		switch rs.records[dev].Status {
		case InstallSuccess:
			p.Succeeded++
		case InstallFailed:
			p.Failed++
		case InstallSkipped:
			p.Skipped++
		case InstallRolledBack:
			p.RolledBack++
		case InstallCancelled:
			p.Cancelled++
		}
	}
	return p
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func dedup(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
