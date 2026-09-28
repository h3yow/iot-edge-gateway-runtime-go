package mqtt

import (
	"errors"
	"sync"
	"testing"
)

// fakeAuth 用最小授权表驱动 broker 鉴权。
type fakeAuth struct {
	pub map[string][]string // device -> 允许发布的过滤器
	sub map[string][]string // device -> 允许订阅的过滤器
}

func (a *fakeAuth) CanPublish(deviceID, topic string) bool {
	for _, f := range a.pub[deviceID] {
		if Match(f, topic) {
			return true
		}
	}
	return false
}

func (a *fakeAuth) CanSubscribe(deviceID, filter string) bool {
	for _, f := range a.sub[deviceID] {
		if Match(f, filter) {
			return true
		}
	}
	return false
}

// memDedup 是线程安全的内存去重器（不入库，仅用于 broker 单测）。
type memDedup struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func (d *memDedup) Seen(deviceID string, msg Message) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := deviceID + "|" + msg.ID
	if _, ok := d.seen[key]; ok {
		return true
	}
	d.seen[key] = struct{}{}
	return false
}

func newBrokerHarness() *Broker {
	return NewBroker(&fakeAuth{
		pub: map[string][]string{"dev-1": {"devices/dev-1/telemetry/#"}},
		sub: map[string][]string{"dev-1": {"devices/dev-1/cmd/#"}},
	}, &memDedup{seen: map[string]struct{}{}})
}

// TestRejectReasonsDistinguishable 验证三类拒绝原因可以严格区分。
func TestRejectReasonsDistinguishable(t *testing.T) {
	b := newBrokerHarness()
	got := make(chan Message, 4)
	if err := b.AddInternalHandler("devices/+/telemetry/#", func(m Message) { got <- m }); err != nil {
		t.Fatal(err)
	}

	// 1) 非法主题（含通配符）
	r := b.Publish("dev-1", Message{ID: "1", Topic: "devices/dev-1/telemetry/#"})
	t.Logf("输入 topic='devices/dev-1/telemetry/#' -> status=%s reason=%s，判定依据：主题不得含通配符", r.Status, r.Reason)
	if r.Status != "rejected" || r.Reason != ReasonInvalidTopic {
		t.Fatalf("want rejected/%s, got %s/%s", ReasonInvalidTopic, r.Status, r.Reason)
	}

	// 2) 未授权发布（主题合法但越权）
	r = b.Publish("dev-1", Message{ID: "2", Topic: "devices/dev-2/telemetry/temp"})
	t.Logf("输入 topic='devices/dev-2/telemetry/temp' -> status=%s reason=%s，判定依据：授权表仅放行 dev-1 自有主题", r.Status, r.Reason)
	if r.Status != "rejected" || r.Reason != ReasonUnauthorizedPub {
		t.Fatalf("want rejected/%s, got %s/%s", ReasonUnauthorizedPub, r.Status, r.Reason)
	}

	// 3) 未授权订阅，必须以哨兵错误返回
	err := b.Subscribe("dev-1", "devices/dev-2/cmd/#", func(Message) {})
	t.Logf("订阅 filter='devices/dev-2/cmd/#' -> err=%v，判定依据：sub 授权不覆盖他机指令", err)
	if !errors.Is(err, ErrUnauthorizedSubscribe) {
		t.Fatalf("want ErrUnauthorizedSubscribe, got %v", err)
	}
	// 非法过滤器与未授权是两类不同错误
	err = b.Subscribe("dev-1", "bad/#/filter", func(Message) {})
	t.Logf("订阅 filter='bad/#/filter' -> err=%v，判定依据：# 非最后一层", err)
	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("want ErrInvalidFilter, got %v", err)
	}

	select {
	case m := <-got:
		t.Fatalf("被拒绝消息不应进入投递，实际收到 %+v", m)
	default:
	}
}

// TestConcurrentDuplicateNoDoubleDelivery 多设备并发 + 同设备重复上报：
// 同一消息 ID 只能投递一次，其余不同消息不丢失。
func TestConcurrentDuplicateNoDoubleDelivery(t *testing.T) {
	b := newBrokerHarness()
	var mu sync.Mutex
	delivered := map[string]int{}
	if err := b.AddInternalHandler("devices/+/telemetry/#", func(m Message) {
		mu.Lock()
		delivered[m.ID]++
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	const devices = 10
	var wg sync.WaitGroup
	for i := 0; i < devices; i++ {
		id := "msg-" + string(rune('a'+i))
		// dev-1 授权范围内的主题，这里统一用 dev-1 发布以复用授权，
		// 去重键为 (device,messageID)，不同消息 ID 都应送达。
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish("dev-1", Message{ID: id, Topic: "devices/dev-1/telemetry/t"})
		}()
	}
	// 同一条消息 50 个 goroutine 并发重复上报
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := b.Publish("dev-1", Message{ID: "dup", Topic: "devices/dev-1/telemetry/t"})
			if r.Status != "delivered" && r.Status != "duplicate" {
				t.Errorf("unexpected status %s", r.Status)
			}
		}()
	}
	wg.Wait()

	t.Logf("关键输入：10 条不同消息 + 同一消息 50 并发重复；判定依据：Seen 原子检查并登记")
	t.Logf("投递计数: %v，dup 投递次数=%d（期望1），总送达消息键数=%d（期望11=10条新消息+dup）",
		delivered, delivered["dup"], len(delivered))
	if delivered["dup"] != 1 {
		t.Fatalf("dup delivered %d times, want 1", delivered["dup"])
	}
	if len(delivered) != 11 {
		t.Fatalf("distinct delivered %d, want 11", len(delivered))
	}
}
