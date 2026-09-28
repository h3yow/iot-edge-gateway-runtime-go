package store

import (
	"sync"
	"testing"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
)

// TestConcurrentSeenIdempotent 模拟断线重连补投 + 并发重复上报：
// 同一 (device,messageID) 无论并发多少次，只入库一条，其它消息不丢。
func TestConcurrentSeenIdempotent(t *testing.T) {
	s := New()
	msg := func(id string) mqtt.Message {
		return mqtt.Message{ID: id, Topic: "devices/d1/telemetry/t", Payload: []byte(id), QoS: 1}
	}

	var wg sync.WaitGroup
	// 50 个 goroutine 同时补投同一条消息（重连补投场景）
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Seen("d1", msg("replay-1")) }()
	}
	// 5 条各自不同的消息并发上报，全部不能丢
	for i := 0; i < 5; i++ {
		id := "msg-" + string(rune('a'+i))
		wg.Add(1)
		go func() { defer wg.Done(); s.Seen("d1", msg(id)) }()
	}
	wg.Wait()

	t.Logf("关键输入：replay-1 并发50次 + 5条不同消息；判定依据：Seen 持锁串行检查并登记")
	t.Logf("结果：total=%d（期望6），d1=%d（期望6），Has(replay-1)=%v",
		s.Count(), s.CountByDevice("d1"), s.Has("d1", "replay-1"))
	if s.Count() != 6 || s.CountByDevice("d1") != 6 {
		t.Fatalf("counts total=%d device=%d, want 6/6", s.Count(), s.CountByDevice("d1"))
	}
	// 不同设备的相同消息 ID 互不影响
	if s.Seen("d2", msg("replay-1")) {
		t.Fatal("不同设备相同消息ID不应被判重")
	}
	t.Log("跨设备相同消息ID：d2/replay-1 首次入库，互不影响")
}
