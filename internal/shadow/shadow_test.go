package shadow

import (
	"strings"
	"testing"
)

func mustRegister(t *testing.T, s *Service, id string) {
	t.Helper()
	s.Register(id)
	if err := s.SetOnline(id); err != nil {
		t.Fatal(err)
	}
}

// TestDesiredVersionMergeAndConflict 在线期望下发的版本裁决与冲突处理。
func TestDesiredVersionMergeAndConflict(t *testing.T) {
	s := New() // 默认 reject 冲突策略
	mustRegister(t, s, "d1")

	// v2 首次写入
	r, err := s.DesiredUpdate("req-A", "d1", 2, map[string]string{"color": "blue"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入 req-A v2 color=blue -> status=%s reason=%s，判定依据：新版本直接采用", r.Status, r.Reason)
	if r.Status != "applied" {
		t.Fatalf("want applied, got %s/%s", r.Status, r.Reason)
	}

	// 同一 reqID 重复下发：幂等，状态不变
	r2, _ := s.DesiredUpdate("req-A", "d1", 2, map[string]string{"color": "blue"})
	t.Logf("输入 req-A 重复 -> status=%s reason=%s，判定依据：reqID 去重", r2.Status, r2.Reason)
	if r2.Status != "idempotent" || r2.Reason != DesiredDuplicate {
		t.Fatalf("want idempotent/duplicate, got %s/%s", r2.Status, r2.Reason)
	}

	// 同版本不同值：冲突，reject 策略保留旧值 blue，绝不静默覆盖为 green
	r3, _ := s.DesiredUpdate("req-B", "d1", 2, map[string]string{"color": "green"})
	t.Logf("输入 req-B v2 color=green（现值blue）-> status=%s reason=%s", r3.Status, r3.Reason)
	for _, d := range r3.Decisions {
		t.Logf("  裁决 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
	}
	if r3.Reason != DesiredConflict {
		t.Fatalf("want conflict reason, got %s", r3.Reason)
	}
	fields, _ := s.Get("d1")
	got := fields[0].DesiredValue
	if got != "blue" {
		t.Fatalf("冲突被静默覆盖：color=%q, 期望保持 blue", got)
	}
	hasConflictVerdict := false
	for _, d := range r3.Decisions {
		if d.Verdict == VerdictConflict && strings.Contains(d.Detail, "CONFLICT") {
			hasConflictVerdict = true
		}
	}
	if !hasConflictVerdict {
		t.Fatal("缺少可解释的 conflict 裁决记录")
	}

	// 版本倒退：拒绝
	r4, _ := s.DesiredUpdate("req-C", "d1", 1, map[string]string{"color": "red"})
	t.Logf("输入 req-C v1（倒退）-> status=%s reason=%s，判定依据：v1 < 现值 v2", r4.Status, r4.Reason)
	if r4.Status != "rejected" || r4.Reason != DesiredOlder {
		t.Fatalf("want rejected/older, got %s/%s", r4.Status, r4.Reason)
	}

	// 新版本 v3 正常覆盖
	r5, _ := s.DesiredUpdate("req-D", "d1", 3, map[string]string{"color": "red"})
	if r5.Status != "applied" {
		t.Fatalf("v3 want applied, got %s", r5.Status)
	}
}

// TestOfflineStashAndReconnectMerge 设备离线期间期望暂存，
// 上线后按版本合并；同请求重复、版本倒退、冲突在合并路径同样生效。
func TestOfflineStashAndReconnectMerge(t *testing.T) {
	s := New()
	s.Register("d2")
	if err := s.SetOnline("d2"); err != nil {
		t.Fatal(err)
	}
	// 设备先报 v1 状态
	if _, err := s.ReportState("d2", 1, map[string]string{"mode": "eco", "color": "white"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOffline("d2"); err != nil {
		t.Fatal(err)
	}

	// 离线期间：v3 期望先到，v2 后到（乱序/倒退）
	r1, _ := s.DesiredUpdate("off-1", "d2", 3, map[string]string{"mode": "boost"})
	t.Logf("离线下发 off-1 v3 -> status=%s reason=%s pending=%d", r1.Status, r1.Reason, s.PendingCount("d2"))
	if r1.Status != "stashed" {
		t.Fatalf("want stashed, got %s", r1.Status)
	}
	r2, _ := s.DesiredUpdate("off-2", "d2", 2, map[string]string{"mode": "silent"})
	if r2.Status != "stashed" {
		t.Fatalf("want stashed, got %s", r2.Status)
	}
	// 重复请求离线期间同样幂等
	r3, _ := s.DesiredUpdate("off-1", "d2", 3, map[string]string{"mode": "boost"})
	t.Logf("离线期间 off-1 重复 -> status=%s reason=%s（暂存不重复）", r3.Status, r3.Reason)
	if r3.Reason != DesiredDuplicate || s.PendingCount("d2") != 2 {
		t.Fatalf("dup stashed request: reason=%s pending=%d want 2", r3.Reason, s.PendingCount("d2"))
	}

	if err := s.SetOnline("d2"); err != nil {
		t.Fatal(err)
	}
	merged, err := s.MergePending("d2")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重连合并 status=%s reason=%s，逐字段裁决：", merged.Status, merged.Reason)
	for _, d := range merged.Decisions {
		t.Logf("  field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
	}
	fields, _ := s.Get("d2")
	byKey := map[string]FieldState{}
	for _, f := range fields {
		byKey[f.Key] = f
	}
	if byKey["mode"].DesiredValue != "boost" || byKey["mode"].DesiredVer != 3 {
		t.Fatalf("mode 合并错误: %+v", byKey["mode"])
	}
	// v2 倒退请求必须被拒绝，silent 不能出现
	if byKey["mode"].DesiredValue == "silent" {
		t.Fatal("倒退版本 silent 被错误采用")
	}
	// color 从未下发期望，对账应为 reported 侧保留
	if byKey["color"].ReportedValue != "white" {
		t.Fatalf("color reported 丢失: %+v", byKey["color"])
	}

	// 再合并一次：nothing pending，幂等
	again, _ := s.MergePending("d2")
	t.Logf("无暂存再合并 -> status=%s reason=%s", again.Status, again.Reason)
	if again.Status != "idempotent" {
		t.Fatalf("want idempotent, got %s", again.Status)
	}
}

// TestReportedDesiredReconcileConflict 同版本 desired/reported 值不一致时，
// 冲突必须显式裁决，且按策略给出可解释结论。
func TestReportedDesiredReconcileConflict(t *testing.T) {
	s := NewWithConflictPolicy(ConflictPreferDesired)
	mustRegister(t, s, "d3")
	// 期望 v5=on
	if _, err := s.DesiredUpdate("r1", "d3", 5, map[string]string{"switch": "on"}); err != nil {
		t.Fatal(err)
	}
	// 设备上报同版本但值不同：off
	rep, err := s.ReportState("d3", 5, map[string]string{"switch": "off"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range rep.Decisions {
		t.Logf("对账裁决 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
		if d.Verdict == VerdictConflict && strings.Contains(d.Detail, "prefer_desired") {
			found = true
		}
	}
	if !found {
		t.Fatal("同版本 desired/reported 冲突未按策略给出可解释裁决")
	}

	// 新版本上报后冲突解除：reported v6 更高
	rep2, _ := s.ReportState("d3", 6, map[string]string{"switch": "off"})
	ok := false
	for _, d := range rep2.Decisions {
		t.Logf("v6 对账 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
		if d.Verdict == VerdictReportedNewer {
			ok = true
		}
	}
	if !ok {
		t.Fatal("v6 上报应裁决 reported_newer")
	}
}
