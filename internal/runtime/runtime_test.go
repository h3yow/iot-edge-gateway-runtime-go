package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/shadow"
)

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	rt := New(Config{
		DeviceIDs:       []string{"d1", "d2", "d3", "d4"},
		ReadOnlyFields:  []string{"temp"},
		InitialVersions: map[string]string{"d1": "v1", "d2": "v1", "d3": "v1", "d4": "v1"},
	})
	var logs []string
	var lmu sync.Mutex
	rt.SetLogger(func(format string, args ...any) {
		lmu.Lock()
		logs = append(logs, format)
		lmu.Unlock()
	})
	t.Logf("运行时已建: 4 设备在线=%d", rt.OnlineCount())
	return rt
}

// 全链路：并发上报去重 -> 离线暂存 -> 重连合并冲突。
func TestIntegrationReportStagingMerge(t *testing.T) {
	rt := newTestRuntime(t)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "d1-up-" + itoa(i%25)
			rt.Report("d1", int64(i), id, 1, map[string]any{"mode": "auto", "temp": 21})
		}(i)
	}
	wg.Wait()
	t.Logf("关键输入: d1 并发上报 100 次、唯一 ID 25 个 => 入库=%d（预期 25）", rt.StoreCount("d1"))
	if rt.StoreCount("d1") != 25 {
		t.Fatalf("并发去重失败: %d", rt.StoreCount("d1"))
	}

	rt.Disconnect("d1")
	res := rt.Desired(shadow.DesiredRequest{
		RequestID: "r1", DeviceID: "d1", BaseReportedVersion: 1,
		BaseReported: map[string]any{"mode": "auto", "temp": 21},
		Patch:        map[string]any{"mode": "cool", "temp": 18},
		IssuedAt:     time.Now(),
	})
	if !res.Staged {
		t.Fatal("设备离线，期望必须暂存")
	}

	if err := rt.Reconnect("d1"); err != nil {
		t.Fatal(err)
	}
	// 补报设备本地改动，触发暂存三方合并
	rt.Report("d1", 200, "d1-reconnect", 2, map[string]any{"mode": "heat", "temp": 30})

	doc := rt.ShadowDoc("d1")
	t.Logf("合并判定: desired=%v reported=%v", doc.Desired, doc.Reported)
	if doc.Desired["mode"] != "cool" {
		t.Error("可控字段冲突应由 desired 获胜")
	}
	if doc.Desired["temp"] != 30 {
		t.Error("只读字段冲突应由 reported 获胜")
	}
}

// 固件发布失败回滚：通过注入失败安装器验证已安装设备回到发布前版本。
func TestIntegrationFirmwareRollback(t *testing.T) {
	// 注入会失败的安装器（d2 必失败），验证失败阈值触发的整体回滚。
	failInstall := &failInstaller{failDevices: map[string]bool{"d2": true}}
	m := firmware.NewManager(failInstall.install)
	m.SetCurrentVersion("d1", "v1")
	m.SetCurrentVersion("d2", "v1")
	m.SetCurrentVersion("d3", "v1")

	rel, err := m.Publish(context.Background(), firmware.Config{
		Version: "v2", MaxConcurrency: 2, FailureThreshold: 1,
		Batches: []firmware.Batch{
			{Name: "canary", Canary: true, DeviceIDs: []string{"d1"}},
			{Name: "rest", DeviceIDs: []string{"d2", "d3"}},
		},
	})
	t.Logf("关键输入: 注入 d2 安装失败、阈值=1 => status=%s reason=%s err=%v", rel.Status, rel.Reason, err)
	if rel.Status != firmware.StatusRolledBack {
		t.Fatalf("预期回滚，实际 %s", rel.Status)
	}
	st, cur, _ := m.DeviceState("v2", "d1")
	t.Logf("回滚判定: d1 state=%s current=%s（预期 rolled_back/v1）", st, cur)
	if st != firmware.DevRolledBack || cur != "v1" {
		t.Fatal("已安装的 d1 必须回滚到 v1")
	}
	if !strings.Contains(rel.Reason, "failure") {
		t.Fatalf("终态原因应可解释为失败阈值，实际 %s", rel.Reason)
	}
}

type failInstaller struct{ failDevices map[string]bool }

func (f *failInstaller) install(ctx context.Context, dev, from, to string) error {
	time.Sleep(time.Millisecond)
	if f.failDevices[dev] && strings.HasPrefix(to, "v2") {
		return context.DeadlineExceeded
	}
	return nil
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	b := []byte{}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
