package device

import "testing"

// TestOfflineQueueReplay FIFO：离线期间多条下行不丢、重连后按顺序补投。
func TestOfflineQueueReplay(t *testing.T) {
	s := NewSession("d1")
	s.Send("t", []byte("before")) // 离线：暂存
	s.Connect()                   // 首次上线：补投 before
	s.Send("t", []byte("online")) // 在线：立即投递
	s.Disconnect()
	s.Send("t", []byte("off1"))
	s.Send("t", []byte("off2")) // 断线期间两条指令
	if pending := s.Pending(); pending != 2 {
		t.Fatalf("pending=%d want 2", pending)
	}
	evs := s.Connect() // 重连：FIFO 补投
	var replays []string
	for _, e := range evs {
		t.Logf("重连事件 type=%s topic=%s payload=%s", e.Type, e.Topic, e.Payload)
		if e.Type == "replay" {
			replays = append(replays, string(e.Payload))
		}
	}
	if len(replays) != 2 || replays[0] != "off1" || replays[1] != "off2" {
		t.Fatalf("FIFO 补投错误: %v", replays)
	}
	if s.Pending() != 0 {
		t.Fatal("重连后暂存队列应清空")
	}
	// before 与 online 在历史中均有记录，无丢失
	h := s.History()
	delivered := 0
	for _, e := range h {
		if e.Type == "delivered" || e.Type == "replay" {
			delivered++
		}
	}
	t.Logf("累计下行触达=%d（before+online+off1+off2=4，期望4）", delivered)
	if delivered != 4 {
		t.Fatalf("下行消息丢失: delivered=%d", delivered)
	}
}
