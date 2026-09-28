package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/store"
)

func testHarness(t *testing.T) (*Broker, *auth.ACL, *store.Store) {
	t.Helper()
	a := auth.NewACL()
	if err := a.Grant("s1",
		[]string{"devices/s1/up", "devices/s1/state"},
		[]string{"devices/+/up"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Grant("s2",
		[]string{"devices/s2/up"},
		[]string{"devices/+/up"}); err != nil {
		t.Fatal(err)
	}
	st := store.New()
	return New(a, st), a, st
}

func TestConnectAndRejectReasons(t *testing.T) {
	b, _, _ := testHarness(t)
	if _, err := b.Connect(auth.Identity{DeviceID: "ghost"}); err == nil {
		t.Error("未知设备连接必须被拒")
	}
	s1, err := b.Connect(auth.Identity{DeviceID: "s1"})
	if err != nil {
		t.Fatalf("已知设备连接失败: %v", err)
	}
	defer s1.Close()

	cases := []struct {
		name  string
		id    auth.Identity
		topic string
		want  auth.RejectReason
	}{
		{"越权主题", auth.Identity{DeviceID: "s1"}, "devices/s2/up", auth.ReasonUnauthorizedPublish},
		{"非法主题", auth.Identity{DeviceID: "s1"}, "devices//up", auth.ReasonInvalidTopic},
		{"通配符发布", auth.Identity{DeviceID: "s1"}, "devices/+/up", auth.ReasonWildcardOnPublish},
		{"未知设备", auth.Identity{DeviceID: "ghost"}, "devices/ghost/up", auth.ReasonUnknownDevice},
	}
	for _, c := range cases {
		r := b.Publish(c.id, c.topic, "msg-x", nil, 1)
		t.Logf("发布判定输入 dev=%s topic=%q => accepted=%v reason=%s（用例:%s）",
			c.id.DeviceID, c.topic, r.Accepted, r.Reason, c.name)
		if r.Reason != c.want {
			t.Errorf("%s: got %s want %s", c.name, r.Reason, c.want)
		}
	}
}

// 多设备并发上报 + 同设备断线重连补投：无重复入库、无丢消息。
func TestConcurrentPublishAndReconnectRedelivery(t *testing.T) {
	b, _, st := testHarness(t)

	// 两个订阅者用通配符监听上行
	var gotMu sync.Mutex
	got := map[string]int{}
	sub := func(dev string) {
		s, err := b.Connect(auth.Identity{DeviceID: dev})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Subscribe(auth.Identity{DeviceID: dev}, "devices/+/up", func(d Delivery) {
			gotMu.Lock()
			got[d.SenderID+"/"+d.MessageID]++
			gotMu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
	}
	sub("s1")
	sub("s2")

	// 模拟 s1 断连后重连（旧 Session 被顶掉）
	old, _ := b.Connect(auth.Identity{DeviceID: "s1"})
	time.Sleep(2 * time.Millisecond)
	new, _ := b.Connect(auth.Identity{DeviceID: "s1"})
	if !old.closed {
		t.Error("重连后旧连接必须标记关闭")
	}
	new.Close()
	// 重连后重新上线并订阅
	sub("s1")

	var wg sync.WaitGroup
	for _, dev := range []string{"s1", "s2"} {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(dev string, i int) {
				defer wg.Done()
				id := "msg-" + dev + "-" + itoa(i%40) // 40 个唯一 ID，重复制造补投
				r := b.Publish(auth.Identity{DeviceID: dev}, "devices/"+dev+"/up", id, []byte("p"), int64(i))
				if !r.Accepted {
					t.Errorf("合法发布被拒: %s", r.Reason)
				}
			}(dev, i)
		}
	}
	wg.Wait()

	for _, dev := range []string{"s1", "s2"} {
		n := st.Count(dev)
		t.Logf("入库判定 dev=%s => 唯一入库=%d（预期 40）在线数=%d", dev, n, b.OnlineCount())
		if n != 40 {
			t.Errorf("%s 应有 40 条唯一消息，实际 %d", dev, n)
		}
	}
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
