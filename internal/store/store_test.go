package store

import (
	"sync"
	"testing"
)

func TestAppendIdempotent(t *testing.T) {
	s := New()
	m := Message{ID: "m1", DeviceID: "d1", Seq: 1, Payload: []byte("a")}
	stored, dup := s.Append("d1", m)
	t.Logf("首次入库输入 id=m1 seq=1 => stored=%v duplicated=%v", stored, dup)
	if !stored || dup {
		t.Fatal("首次消息应入库")
	}
	stored, dup = s.Append("d1", m)
	t.Logf("重复补投同一 id=m1 => stored=%v duplicated=%v（判定依据: (device,messageID) 去重表）", stored, dup)
	if stored || !dup {
		t.Fatal("重复消息必须判重且不入库")
	}
	// 不同设备同一 ID 不算重复
	stored, _ = s.Append("d2", Message{ID: "m1", Seq: 1})
	if !stored {
		t.Fatal("不同设备的相同 messageID 应独立入库")
	}
	if s.Count("d1") != 1 {
		t.Fatalf("d1 应只有 1 条，实际 %d", s.Count("d1"))
	}
}

func TestConcurrentAppendNoDupNoLoss(t *testing.T) {
	s := New()
	const workers = 16
	const per = 200
	var wg sync.WaitGroup
	var storedN, dupN int64
	var smu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				// 每个 goroutine 复用前 50 个 ID，制造大量重复，模拟重连补投
				id := i % 50
				stored, dup := s.Append("d1", Message{ID: "m" + itoa(id), Seq: int64(id)})
				smu.Lock()
				if stored {
					storedN++
				}
				if dup {
					dupN++
				}
				smu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("并发上报判定结果: 唯一入库=%d 重复拒绝=%d 库存=%d（预期唯一 50）", storedN, dupN, s.Count("d1"))
	if s.Count("d1") != 50 {
		t.Fatalf("并发去重后应有 50 条，实际 %d", s.Count("d1"))
	}
	if storedN+dupN != workers*per {
		t.Fatal("每次上报都必须有明确结论（入库或判重），不能丢消息")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
