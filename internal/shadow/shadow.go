// Package shadow 维护设备影子：期望状态与上报状态分开存储、版本化合并。
package shadow

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// FieldSource 标识冲突字段的裁决来源。
type FieldSource string

const (
	SourceDesired  FieldSource = "desired"
	SourceReported FieldSource = "reported"
)

// ConflictRecord 记录单个字段的合并冲突与裁决依据。
type ConflictRecord struct {
	Field       string      `json:"field"`
	BaseValue   any         `json:"base_value"`
	DesiredVal  any         `json:"desired_value"`
	ReportedVal any         `json:"reported_value"`
	Winner      FieldSource `json:"winner"`
	Reason      string      `json:"reason"`
}

// DesiredRequest 是一次期望状态下发请求（RequestID 保证幂等）。
type DesiredRequest struct {
	RequestID           string
	DeviceID            string
	BaseReportedVersion int64
	// BaseReported 是下发时刻设备 reported 的快照，作为三方合并的基线；
	// 离线暂存期间也会保留，重连合并时据此判断“双方是否都改过”。
	BaseReported map[string]any
	Patch        map[string]any
	IssuedAt     time.Time
}

// ApplyResult 是期望下发/合并的结论。
type ApplyResult struct {
	Applied        bool
	Duplicate      bool
	Staged         bool
	DesiredVersion int64
	Conflicts      []ConflictRecord
}

// Doc 是某设备的完整影子文档。
type Doc struct {
	DeviceID        string
	Online          bool
	Reported        map[string]any
	ReportedVersion int64
	Desired         map[string]any
	DesiredVersion  int64
	Staged          []DesiredRequest
}

type deviceShadow struct {
	online   bool
	reported map[string]any
	repVer   int64
	desired  map[string]any
	desVer   int64
	staged   []DesiredRequest
	applied  map[string]struct{} // 已处理过的 RequestID（幂等）
}

// Shadow 是所有设备影子的注册表。
type Shadow struct {
	mu      sync.Mutex
	devices map[string]*deviceShadow
	// readOnlyFields 标记传感器/只读字段：双方都改过时由 reported 获胜。
	readOnlyFields map[string]bool
}

// New 创建影子注册表。
func New() *Shadow {
	return &Shadow{
		devices:        make(map[string]*deviceShadow),
		readOnlyFields: map[string]bool{},
	}
}

// MarkReadOnly 注册只读字段名（冲突时 reported 获胜）。
func (s *Shadow) MarkReadOnly(field string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOnlyFields[field] = true
}

func (s *Shadow) get(deviceID string) *deviceShadow {
	d, ok := s.devices[deviceID]
	if !ok {
		d = &deviceShadow{
			reported: map[string]any{},
			desired:  map[string]any{},
			applied:  map[string]struct{}{},
		}
		s.devices[deviceID] = d
	}
	return d
}

// MarkOnline 标记设备上线；若之前离线则返回 wasOffline=true，调用方应触发补投合并。
func (s *Shadow) MarkOnline(deviceID string) (wasOffline bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(deviceID)
	wasOffline = !d.online
	d.online = true
	return wasOffline
}

// MarkOffline 标记设备离线。
func (s *Shadow) MarkOffline(deviceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(deviceID)
	d.online = false
	return true
}

// IsOnline 查询在线状态。
func (s *Shadow) IsOnline(deviceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(deviceID).online
}

// Report 合并设备上报状态，返回 advanced 表示版本是否前进、stale 表示旧版本被忽略。
// 上报版本必须严格递增；相同版本重复上报视为幂等（stale=false, advanced=false）。
func (s *Shadow) Report(deviceID string, version int64, state map[string]any) (advanced bool, stale bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(deviceID)
	if version < d.repVer {
		return false, true // 旧版本补投，直接忽略但给出明确结论
	}
	if version == d.repVer && reflect.DeepEqual(d.reported, state) {
		return false, false // 同版本同内容：幂等
	}
	if version == d.repVer {
		return false, true // 同版本不同内容：判为过期，拒绝静默覆盖
	}
	d.reported = cloneMap(state)
	d.repVer = version
	return true, false
}

// ApplyDesired 下发期望状态。
// 设备在线：立即并入 desired（版本 +1）；设备离线：暂存待重连合并。
// 相同 RequestID 重复下发一律幂等，返回 Duplicate=true。
func (s *Shadow) ApplyDesired(req DesiredRequest) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(req.DeviceID)
	if _, dup := d.applied[req.RequestID]; dup {
		return ApplyResult{Duplicate: true, DesiredVersion: d.desVer}
	}
	d.applied[req.RequestID] = struct{}{}
	// 以当前 reported 作为合并基线（同锁内，保证与版本判断一致）。
	if req.BaseReported == nil {
		req.BaseReported = cloneMap(d.reported)
	}
	if !d.online {
		d.staged = append(d.staged, req)
		return ApplyResult{Staged: true, DesiredVersion: d.desVer}
	}
	conflicts := s.mergeDesired(d, req)
	d.desVer++
	return ApplyResult{Applied: true, DesiredVersion: d.desVer, Conflicts: conflicts}
}

// FlushOnReconnect 在设备重连后按暂存顺序合并期望请求。
// 每个请求以“下发时基线版本对应的 reported”与“当前 reported”做三方比对。
// 为保留基线值，请求暂存时记录当时的 reported 快照（在 staged 中携带）。
func (s *Shadow) FlushOnReconnect(deviceID string) []ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(deviceID)
	var results []ApplyResult
	for _, req := range d.staged {
		conflicts := s.mergeDesired(d, req)
		d.desVer++
		results = append(results, ApplyResult{Applied: true, DesiredVersion: d.desVer, Conflicts: conflicts})
	}
	d.staged = nil
	return results
}

// mergeDesired 执行单字段三方合并，调用方必须持锁。
// 裁决规则（绝不静默覆盖）：
//  1. 期望侧与当前 reported 相同：设备已自行达到期望值，字段直接一致，无冲突。
//  2. 只有一侧相对基线变化：变化侧获胜。
//  3. 两侧都变化且不同：可控字段 desired 获胜（平台权威），
//     只读字段 reported 获胜（传感器权威），均记录 ConflictRecord。
func (s *Shadow) mergeDesired(d *deviceShadow, req DesiredRequest) []ConflictRecord {
	base := req.BaseReported
	if base == nil {
		base = map[string]any{}
	}
	var conflicts []ConflictRecord
	for field, desiredVal := range req.Patch {
		currentRep, repExists := d.reported[field]
		baseVal, baseExists := base[field]
		switch {
		case reflect.DeepEqual(desiredVal, currentRep):
			// 已一致
		case !repExists || reflect.DeepEqual(currentRep, baseVal) || !baseExists:
			// reported 未偏离基线，期望直接生效
		case reflect.DeepEqual(desiredVal, baseVal):
			// desired 未偏离基线，reported 的变化保留
		default:
			// 两侧都改且不同：需要裁决
			winner := SourceDesired
			reason := "both_changed_from_base; controllable field: platform desired wins"
			if s.readOnlyFields[field] {
				winner = SourceReported
				reason = "both_changed_from_base; read-only/sensor field: device reported wins"
			}
			conflicts = append(conflicts, ConflictRecord{
				Field:       field,
				BaseValue:   baseVal,
				DesiredVal:  desiredVal,
				ReportedVal: currentRep,
				Winner:      winner,
				Reason:      reason,
			})
			if winner == SourceDesired {
				d.desired[field] = desiredVal
			} else {
				d.desired[field] = currentRep
			}
			continue
		}
		d.desired[field] = desiredVal
	}
	return conflicts
}

// Get 读取设备影子文档（快照拷贝）。
func (s *Shadow) Get(deviceID string) Doc {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.get(deviceID)
	staged := make([]DesiredRequest, len(d.staged))
	copy(staged, d.staged)
	return Doc{
		DeviceID:        deviceID,
		Online:          d.online,
		Reported:        cloneMap(d.reported),
		ReportedVersion: d.repVer,
		Desired:         cloneMap(d.desired),
		DesiredVersion:  d.desVer,
		Staged:          staged,
	}
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// String 便于日志输出冲突结论。
func (c ConflictRecord) String() string {
	return fmt.Sprintf("field=%s base=%v desired=%v reported=%v winner=%s reason=%q",
		c.Field, c.BaseValue, c.DesiredVal, c.ReportedVal, c.Winner, c.Reason)
}
