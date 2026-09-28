package shadow

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// 期望更新的总体原因。
const (
	DesiredApplied   = "applied"           // 版本更新，已采用
	DesiredSameValue = "same_value"        // 版本相同且值一致，幂等忽略
	DesiredConflict  = "conflict"          // 版本相同但值不同，冲突
	DesiredOlder     = "older_version"     // 版本倒退，拒绝
	DesiredStashed   = "stashed"           // 设备离线，已暂存待上线合并
	DesiredDuplicate = "duplicate_request" // 同一 reqID 重复下发，幂等
)

// 逐字段合并裁决（desired vs reported 对账，以及期望写入判定）。
const (
	VerdictDesiredNewer  = "desired_newer"  // 期望版本更高：设备尚需执行
	VerdictReportedNewer = "reported_newer" // 上报版本更高：保留上报
	VerdictEqual         = "equal"          // 版本相同且值一致：已同步
	VerdictConflict      = "conflict"       // 版本相同值不一致：按策略裁决
	VerdictApplied       = "applied"        // 期望采用（新版本）
	VerdictRejected      = "rejected"       // 期望版本倒退，拒绝
	VerdictIdempotent    = "idempotent"     // 同版本同值，忽略
)

// ConflictPolicy 指定“同版本不同值”冲突时的裁决策略。
// 冲突永远不会静默：无论哪种策略，Decision.Verdict 都是 conflict，
// Detail 中记录胜出方、双方值与裁决依据。
const (
	ConflictReject         = "reject"          // 拒绝新值，保留现值（默认，最保守）
	ConflictPreferDesired  = "prefer_desired"  // 对账冲突时以期望为准
	ConflictPreferReported = "prefer_reported" // 对账冲突时以上报为准
)

var (
	ErrDeviceNotFound = errors.New("shadow: device not found")
	ErrBadVersion     = errors.New("shadow: version must be positive")
	ErrNoFields       = errors.New("shadow: empty field set")
)

// FieldState 是单个字段的 desired/reported 双版本值。
type FieldState struct {
	Key           string
	DesiredValue  string
	DesiredVer    int64
	DesiredAt     time.Time
	ReportedValue string
	ReportedVer   int64
	ReportedAt    time.Time
}

// Decision 记录一次下发/合并中单个字段的可解释裁决。
type Decision struct {
	Field   string
	Verdict string
	Detail  string
}

// Result 是 DesiredUpdate / MergePending / ReportState 的结果。
type Result struct {
	Status    string // applied | idempotent | rejected | stashed | merged
	Reason    string
	Decisions []Decision
}

// pendingReq 是离线期间暂存的一次期望下发。
type pendingReq struct {
	reqID   string
	version int64
	fields  map[string]string
	at      time.Time
}

type deviceShadow struct {
	online  bool
	fields  map[string]*FieldState
	pending []pendingReq
	seenReq map[string]string // reqID -> 上次处理结论原因
}

// Service 维护全部设备的影子。
type Service struct {
	mu             sync.Mutex
	devices        map[string]*deviceShadow
	conflictPolicy string
}

// New 创建影子服务，冲突默认采用最保守的 reject 策略。
func New() *Service {
	return &Service{devices: make(map[string]*deviceShadow), conflictPolicy: ConflictReject}
}

// NewWithConflictPolicy 用指定冲突策略创建服务。
func NewWithConflictPolicy(policy string) *Service {
	s := New()
	s.conflictPolicy = policy
	return s
}

// Register 注册设备（初始离线）。重复注册幂等。
func (s *Service) Register(deviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.devices[deviceID]; !ok {
		s.devices[deviceID] = &deviceShadow{fields: make(map[string]*FieldState), seenReq: make(map[string]string)}
	}
}

func (s *Service) mustDevice(deviceID string) *deviceShadow {
	d, ok := s.devices[deviceID]
	if !ok {
		return nil
	}
	return d
}

// SetOnline 标记设备在线，但不自动合并；由调用方随后调用 MergePending，
// 使“上线补投暂存期望”的裁决结果可被显式观测。
func (s *Service) SetOnline(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return ErrDeviceNotFound
	}
	d.online = true
	return nil
}

// SetOffline 标记设备离线。
func (s *Service) SetOffline(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return ErrDeviceNotFound
	}
	d.online = false
	return nil
}

// IsOnline 查询在线状态。
func (s *Service) IsOnline(deviceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	return d != nil && d.online
}

// ReportState 接收设备上报，更新 reported 视图，并与 desired 做版本对账。
// version 是该批字段的上报版本；同一设备并发上报按临界区串行化。
func (s *Service) ReportState(deviceID string, version int64, fields map[string]string) (Result, error) {
	if version <= 0 {
		return Result{}, ErrBadVersion
	}
	if len(fields) == 0 {
		return Result{}, ErrNoFields
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return Result{}, ErrDeviceNotFound
	}
	decisions := make([]Decision, 0, len(fields))
	rejected := 0
	for _, key := range sortedKeys(fields) {
		val := fields[key]
		f := d.fields[key]
		if f == nil {
			f = &FieldState{Key: key}
			d.fields[key] = f
		}
		if version < f.ReportedVer {
			rejected++
			decisions = append(decisions, Decision{
				Field: key, Verdict: VerdictRejected,
				Detail: detailf("reported version %d older than current %d, keep value=%q",
					version, f.ReportedVer, f.ReportedValue),
			})
			continue
		}
		f.ReportedValue, f.ReportedVer, f.ReportedAt = val, version, time.Now()
		decisions = append(decisions, s.reconcile(f))
	}
	status := "applied"
	if rejected == len(fields) {
		status = "rejected"
	}
	return Result{Status: status, Reason: "reported", Decisions: decisions}, nil
}

// DesiredUpdate 下发期望值。
//   - reqID 用于请求幂等：同一 reqID 重复下发直接返回首次结论，不再改动状态；
//   - 设备离线：暂存（stashed），待 MergePending 按版本合并；
//   - 设备在线：立即按版本逐字段裁决（新版本采用 / 同值幂等 / 倒退拒绝 / 同版本冲突）。
func (s *Service) DesiredUpdate(reqID, deviceID string, version int64, fields map[string]string) (Result, error) {
	if version <= 0 {
		return Result{}, ErrBadVersion
	}
	if len(fields) == 0 {
		return Result{}, ErrNoFields
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return Result{}, ErrDeviceNotFound
	}
	if reason, ok := d.seenReq[reqID]; ok {
		return Result{
			Status: "idempotent", Reason: DesiredDuplicate,
			Decisions: []Decision{{Field: "*", Verdict: VerdictIdempotent,
				Detail: detailf("request %q already processed as %q, no state change", reqID, reason)}},
		}, nil
	}
	if !d.online {
		d.pending = append(d.pending, pendingReq{
			reqID: reqID, version: version,
			fields: cloneFields(fields), at: time.Now(),
		})
		d.seenReq[reqID] = DesiredStashed
		return Result{
			Status: "stashed", Reason: DesiredStashed,
			Decisions: []Decision{{Field: "*", Verdict: "stashed",
				Detail: detailf("device offline, %d field(s) stashed for reconnect merge", len(fields))}},
		}, nil
	}
	res := s.applyDesired(d, version, fields)
	d.seenReq[reqID] = res.Reason
	res.Decisions = append([]Decision{{Field: "*", Verdict: "request",
		Detail: detailf("request %q processed", reqID)}}, res.Decisions...)
	return res, nil
}

// MergePending 设备上线后调用：把暂存期望按到达顺序逐个按版本合并，
// 返回所有请求的逐字段裁决。若调用时设备不在线则拒绝（语义清晰）。
func (s *Service) MergePending(deviceID string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return Result{}, ErrDeviceNotFound
	}
	if !d.online {
		return Result{}, errors.New("shadow: device must be online before merging pending desired")
	}
	if len(d.pending) == 0 {
		return Result{Status: "idempotent", Reason: "nothing_pending",
			Decisions: []Decision{{Field: "*", Verdict: VerdictIdempotent, Detail: "no pending desired"}}}, nil
	}
	all := make([]Decision, 0)
	applied := 0
	for len(d.pending) > 0 {
		req := d.pending[0]
		d.pending = d.pending[1:]
		all = append(all, Decision{Field: "*", Verdict: "stashed_request",
			Detail: detailf("merge stashed request %q (version %d, %d fields)", req.reqID, req.version, len(req.fields))})
		res := s.applyDesired(d, req.version, req.fields)
		d.seenReq[req.reqID] = res.Reason
		all = append(all, res.Decisions...)
		if res.Status == "applied" {
			applied++
		}
	}
	status := "merged"
	if applied == 0 {
		status = "idempotent"
	}
	return Result{Status: status, Reason: "merge_complete", Decisions: all}, nil
}

// applyDesired 对 desired 侧逐字段做版本裁决（调用方持锁）。
func (s *Service) applyDesired(d *deviceShadow, version int64, fields map[string]string) Result {
	decisions := make([]Decision, 0, len(fields))
	applied, rejected, conflicts := 0, 0, 0
	for _, key := range sortedKeys(fields) {
		val := fields[key]
		f := d.fields[key]
		if f == nil {
			f = &FieldState{Key: key}
			d.fields[key] = f
		}
		switch {
		case version > f.DesiredVer:
			oldVer, oldVal := f.DesiredVer, f.DesiredValue
			f.DesiredValue, f.DesiredVer, f.DesiredAt = val, version, time.Now()
			applied++
			decisions = append(decisions, Decision{Field: key, Verdict: VerdictApplied,
				Detail: detailf("new desired version %d > %d (old value=%q) applied, new value=%q",
					version, oldVer, oldVal, val)})
			// 追加一条 desired/reported 对账裁决，不覆盖上面的写入裁决。
			if rc := s.reconcile(f); rc.Verdict != "" {
				decisions = append(decisions, rc)
			}
		case version == f.DesiredVer && val == f.DesiredValue:
			decisions = append(decisions, Decision{Field: key, Verdict: VerdictIdempotent,
				Detail: detailf("same version %d and same value=%q, idempotent ignore", version, val)})
		case version == f.DesiredVer && val != f.DesiredValue:
			// 同版本不同值：冲突。任何策略下都绝不静默覆盖，裁决必须显式可解释。
			conflicts++
			existingForLog := f.DesiredValue
			kept := f.DesiredValue
			if s.conflictPolicy == ConflictPreferDesired {
				f.DesiredValue = val
				f.DesiredAt = time.Now()
				kept = val
			}
			decisions = append(decisions, Decision{Field: key, Verdict: VerdictConflict,
				Detail: detailf("CONFLICT same desired version %d: incoming=%q existing=%q; policy=%s -> kept=%q (not silently overwritten)",
					version, val, existingForLog, s.conflictPolicy, kept)})
		default:
			rejected++
			decisions = append(decisions, Decision{Field: key, Verdict: VerdictRejected,
				Detail: detailf("incoming version %d older than desired %d, keep value=%q",
					version, f.DesiredVer, f.DesiredValue)})
		}
	}
	// 总体结论原因可区分：有字段采用即 applied；全部未采用时，
	// 区分是“同版本冲突”还是“版本倒退”。
	status, reason := "applied", DesiredApplied
	if applied == 0 {
		status = "rejected"
		switch {
		case conflicts > 0:
			reason = DesiredConflict
		default:
			reason = DesiredOlder
		}
	}
	return Result{Status: status, Reason: reason, Decisions: decisions}
}

// reconcile 在一次写入后对 desired/reported 两侧做版本对账并补充裁决说明。
func (s *Service) reconcile(f *FieldState) Decision {
	return s.reconcileWith(f, Decision{Field: f.Key, Verdict: VerdictApplied,
		Detail: detailf("reported value=%q version %d recorded", f.ReportedValue, f.ReportedVer)})
}

func (s *Service) reconcileWith(f *FieldState, base Decision) Decision {
	switch {
	case f.DesiredVer == 0 && f.ReportedVer == 0:
		return base
	case f.DesiredVer > f.ReportedVer:
		return Decision{Field: f.Key, Verdict: VerdictDesiredNewer,
			Detail: detailf("desired v%d (%q) newer than reported v%d (%q): pending device action",
				f.DesiredVer, f.DesiredValue, f.ReportedVer, f.ReportedValue)}
	case f.ReportedVer > f.DesiredVer:
		return Decision{Field: f.Key, Verdict: VerdictReportedNewer,
			Detail: detailf("reported v%d (%q) newer than desired v%d (%q): keep reported",
				f.ReportedVer, f.ReportedValue, f.DesiredVer, f.DesiredValue)}
	case f.DesiredValue == f.ReportedValue:
		return Decision{Field: f.Key, Verdict: VerdictEqual,
			Detail: detailf("desired==reported v%d value=%q: in sync", f.DesiredVer, f.DesiredValue)}
	default:
		// 同版本、不同值：真实冲突，按策略裁决并显式说明，绝不静默。
		winner, kept := "reported", f.ReportedValue
		if s.conflictPolicy == ConflictPreferDesired {
			winner, kept = "desired", f.DesiredValue
		}
		return Decision{Field: f.Key, Verdict: VerdictConflict,
			Detail: detailf("CONFLICT same version %d: desired=%q reported=%q; policy=%s -> keep %s value=%q",
				f.DesiredVer, f.DesiredValue, f.ReportedValue, s.conflictPolicy, winner, kept)}
	}
}

// Get 读取设备影子字段快照（按 key 排序）。
func (s *Service) Get(deviceID string) ([]FieldState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return nil, ErrDeviceNotFound
	}
	out := make([]FieldState, 0, len(d.fields))
	for _, key := range sortedFieldKeys(d.fields) {
		out = append(out, *d.fields[key])
	}
	return out, nil
}

// PendingCount 返回设备暂存期望请求数量。
func (s *Service) PendingCount(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.mustDevice(deviceID)
	if d == nil {
		return 0
	}
	return len(d.pending)
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedFieldKeys(m map[string]*FieldState) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func cloneFields(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
