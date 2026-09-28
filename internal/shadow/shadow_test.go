package shadow

import (
	"testing"
	"time"
)

func mkReq(id, dev string, baseVer int64, patch map[string]any, base map[string]any) DesiredRequest {
	return DesiredRequest{
		RequestID: id, DeviceID: dev, BaseReportedVersion: baseVer,
		Patch: patch, BaseReported: base, IssuedAt: time.Now(),
	}
}

// 场景：设备离线期间平台下发期望并暂存；设备本地也改了设置，
// 重连上报后合并，可控字段冲突由平台裁决，只读字段由设备裁决。
func TestOfflineStagingAndConflictMerge(t *testing.T) {
	s := New()
	s.MarkReadOnly("temp")
	dev := "d1"

	// v1：设备上线，上报初始状态
	s.MarkOnline(dev)
	adv, stale := s.Report(dev, 1, map[string]any{"mode": "auto", "temp": 20})
	t.Logf("上报 v1 {mode:auto,temp:20} => advanced=%v stale=%v", adv, stale)
	if !adv || stale {
		t.Fatal("v1 应为新版本")
	}

	// 设备断线，平台在离线期间下发两次期望
	s.MarkOffline(dev)
	r1 := s.ApplyDesired(mkReq("req-1", dev, 1, map[string]any{"mode": "cool", "temp": 18},
		map[string]any{"mode": "auto", "temp": 20}))
	t.Logf("离线下发 req-1 => applied=%v staged=%v", r1.Applied, r1.Staged)
	if !r1.Staged {
		t.Fatal("离线期望必须暂存")
	}
	// 重复下发同一请求：幂等
	r1dup := s.ApplyDesired(mkReq("req-1", dev, 1, map[string]any{"mode": "cool"}, nil))
	t.Logf("重复下发 req-1 => duplicate=%v（判定依据: RequestID 去重）", r1dup.Duplicate)
	if !r1dup.Duplicate {
		t.Fatal("重复请求必须幂等拒绝")
	}

	// 设备本地被手动调成 heat、温度升到 26，以 v2 重连补报
	s.MarkOnline(dev)
	adv, stale = s.Report(dev, 2, map[string]any{"mode": "heat", "temp": 26})
	t.Logf("重连上报 v2 {mode:heat,temp:26} => advanced=%v stale=%v", adv, stale)

	results := s.FlushOnReconnect(dev)
	if len(results) != 1 {
		t.Fatalf("应只合并 1 个去重后的暂存请求，实际 %d", len(results))
	}
	for _, c := range results[0].Conflicts {
		t.Logf("冲突裁决: %s", c)
	}
	doc := s.Get(dev)
	t.Logf("合并后影子: desired=%v reported=%v desiredVersion=%d", doc.Desired, doc.Reported, doc.DesiredVersion)
	if doc.Desired["mode"] != "cool" {
		t.Errorf("可控字段冲突应由平台期望获胜，mode=%v", doc.Desired["mode"])
	}
	if doc.Desired["temp"] != 26 {
		t.Errorf("只读字段冲突应由设备上报获胜，temp=%v", doc.Desired["temp"])
	}
	if len(results[0].Conflicts) != 2 {
		t.Errorf("两个字段都应留下可解释冲突记录，实际 %d", len(results[0].Conflicts))
	}
}

// 场景：reported 未偏离基线时，期望直接生效且不产生冲突记录。
func TestNoConflictWhenReportedUnchanged(t *testing.T) {
	s := New()
	dev := "d2"
	s.MarkOnline(dev)
	s.Report(dev, 1, map[string]any{"mode": "auto"})
	s.MarkOffline(dev)
	s.ApplyDesired(mkReq("req-a", dev, 1, map[string]any{"mode": "cool"},
		map[string]any{"mode": "auto"}))
	s.MarkOnline(dev)
	// 设备没有本地改动，仍上报 v1 内容（版本前进到 2）
	s.Report(dev, 2, map[string]any{"mode": "auto"})
	res := s.FlushOnReconnect(dev)
	t.Logf("reported 未偏离基线 => 冲突数=%d desired=%v", len(res[0].Conflicts), s.Get(dev).Desired)
	if len(res[0].Conflicts) != 0 || s.Get(dev).Desired["mode"] != "cool" {
		t.Fatal("无冲突时期望应静默一致地生效（字段级静默仅允许于未冲突场景）")
	}
}

// 场景：过期版本上报必须被识别并忽略，不能覆盖新版本。
func TestStaleReportRejected(t *testing.T) {
	s := New()
	dev := "d3"
	s.MarkOnline(dev)
	s.Report(dev, 3, map[string]any{"mode": "v3"})
	adv, stale := s.Report(dev, 2, map[string]any{"mode": "v2-old"})
	t.Logf("旧版本 v2 补投在 v3 之后 => advanced=%v stale=%v（判定依据: 版本单调）", adv, stale)
	if adv || !stale {
		t.Fatal("旧版本必须判 stale，不能回滚覆盖")
	}
	if s.Get(dev).Reported["mode"] != "v3" {
		t.Fatal("新版本状态不能被旧版本覆盖")
	}
}
