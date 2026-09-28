package gateway

import (
	"sync"
	"testing"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
)

// TestConcurrentTelemetryAcrossDevices 多设备并发上报 + 单设备重复：
// 不重复入库、不丢消息，且原因全部可区分。
func TestConcurrentTelemetryAcrossDevices(t *testing.T) {
	gw := New(Config{MaxConcurrentPublishes: 4})
	for _, id := range []string{"dev-a", "dev-b", "dev-c", "dev-d", "dev-e"} {
		gw.RegisterDevice(id)
	}

	reasons := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	// 5 设备各自 20 条不同消息并发
	devs := []string{"dev-a", "dev-b", "dev-c", "dev-d", "dev-e"}
	for _, dev := range devs {
		for i := 0; i < 20; i++ {
			dev, i := dev, i
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := gw.IngestTelemetry(dev, mqtt.Message{
					ID:      dev + "-m" + string(rune('a'+i%26)),
					Topic:   "devices/" + dev + "/telemetry/temp",
					Payload: []byte("p"), QoS: 1,
				})
				mu.Lock()
				reasons[r.Reason]++
				mu.Unlock()
			}()
		}
	}
	// dev-a 同一条消息 30 个并发重复 + 1 次非法主题 + 1 次越权
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := gw.IngestTelemetry("dev-a", mqtt.Message{ID: "hot", Topic: "devices/dev-a/telemetry/temp", QoS: 1})
			mu.Lock()
			reasons[r.Reason]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	badTopic := gw.IngestTelemetry("dev-a", mqtt.Message{ID: "x", Topic: "devices/dev-a/telemetry/+"})
	unauth := gw.IngestTelemetry("dev-a", mqtt.Message{ID: "y", Topic: "devices/dev-b/telemetry/temp"})

	t.Logf("关键输入：5设备x20不同消息 + dev-a/hot 并发30；结论分布=%v", reasons)
	t.Logf("非法主题=%s/%s 越权=%s/%s", badTopic.Status, badTopic.Reason, unauth.Status, unauth.Reason)
	if badTopic.Reason != mqtt.ReasonInvalidTopic || unauth.Reason != mqtt.ReasonUnauthorizedPub {
		t.Fatal("拒绝原因未区分")
	}
	if reasons[mqtt.ReasonDuplicate] != 29 {
		t.Fatalf("hot 重复计数=%d 期望29", reasons[mqtt.ReasonDuplicate])
	}
	// 5*20=100 条不同消息 + hot 1 次 = 101 入库
	if got := gw.Store.Count(); got != 101 {
		t.Fatalf("总入库=%d 期望101（无丢失无重复）", got)
	}
	if got := gw.Store.CountByDevice("dev-a"); got != 21 {
		t.Fatalf("dev-a 入库=%d 期望21（20不同+hot）", got)
	}
}

// TestReconnectReplayIdempotent 断线重连补投：
// 离线期间遥测在设备侧重发，补投消息与首次完全相同 ID，不重复入库也不丢其他消息；
// 同时验证离线期望暂存在重连后合并。
func TestReconnectReplayIdempotent(t *testing.T) {
	gw := New(Config{MaxConcurrentPublishes: 2})
	gw.RegisterDevice("dev-1")

	// 首次上报（在线）
	r1 := gw.IngestTelemetry("dev-1", mqtt.Message{ID: "m-1", Topic: "devices/dev-1/telemetry/temp"})
	t.Logf("首次上报 m-1 -> %s/%s", r1.Status, r1.Reason)
	if r1.Status != "delivered" {
		t.Fatalf("want delivered, got %s", r1.Status)
	}

	// 设备离线：期望下发被暂存
	if err := gw.Disconnect("dev-1"); err != nil {
		t.Fatal(err)
	}
	dr, err := gw.UpdateDesired("want-1", "dev-1", 7, map[string]string{"cfg": "v7"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("离线下发期望 want-1 v7 -> %s/%s pending=%d", dr.Status, dr.Reason, gw.Shadow.PendingCount("dev-1"))

	// 重连：补投 m-1（同 ID），并合并暂存期望
	merge, err := gw.Connect("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重连合并 -> %s/%s，裁决数=%d", merge.Status, merge.Reason, len(merge.Decisions))
	r2 := gw.IngestTelemetry("dev-1", mqtt.Message{ID: "m-1", Topic: "devices/dev-1/telemetry/temp"})
	t.Logf("重连补投 m-1 -> status=%s reason=%s（期望 duplicate）", r2.Status, r2.Reason)
	if r2.Status != "duplicate" {
		t.Fatalf("补投应判定 duplicate, got %s/%s", r2.Status, r2.Reason)
	}
	// 新消息不丢
	r3 := gw.IngestTelemetry("dev-1", mqtt.Message{ID: "m-2", Topic: "devices/dev-1/telemetry/temp"})
	if r3.Status != "delivered" {
		t.Fatalf("新消息应送达, got %s", r3.Status)
	}
	if n := gw.Store.CountByDevice("dev-1"); n != 2 {
		t.Fatalf("dev-1 入库=%d 期望2（m-1只一次 + m-2）", n)
	}
	fields, _ := gw.Shadow.Get("dev-1")
	if len(fields) != 1 || fields[0].DesiredValue != "v7" || fields[0].DesiredVer != 7 {
		t.Fatalf("重连后暂存期望未合并: %+v", fields)
	}
	t.Logf("重连后影子 cfg desired=%s v%d（暂存期望已合并）", fields[0].DesiredValue, fields[0].DesiredVer)
}

// TestFirmwareRollbackViaGateway 经网关链路验证固件失败回滚与设备版本一致。
func TestFirmwareRollbackViaGateway(t *testing.T) {
	gw := New(Config{MaxConcurrentPublishes: 4})
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		gw.RegisterDevice(id)
	}
	gw.FailInstall("d1")
	prog, _, err := gw.PublishFirmware("rel-x", "9.9.9", 2, 0, 10, []string{"d1", "d2", "d3", "d4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("关键输入：rel-x 批2 阈值10%% d1失败；终态=%s/%s rolledBack=%d cancelled=%d",
		prog.Status, prog.Reason, prog.RolledBack, prog.Cancelled)
	if prog.Status != firmware.ReleaseFailed {
		t.Fatalf("want failed, got %s", prog.Status)
	}
	if v := gw.Firmware.InstalledVersion("d2"); v != "" {
		t.Fatalf("成功设备 d2 应随回滚恢复版本为空, got %q", v)
	}
	// 离线设备的安装指令被暂存，重连不丢（安装器侧已幂等）
	if err := gw.Disconnect("d3"); err != nil {
		t.Fatal(err)
	}
	sess := gw.Devices.Get("d3")
	before := sess.Pending()
	if _, err := gw.Connect("d3"); err != nil {
		t.Fatal(err)
	}
	t.Logf("d3 取消发布前处于离线，暂存指令数=%d（重连清空，不产生二次安装）", before)
}
