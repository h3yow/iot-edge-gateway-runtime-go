// Package store 提供按设备维度的消息入库与幂等去重。
package store

import (
	"sort"
	"sync"
	"time"
)

// Message 是设备上报的一条消息。
type Message struct {
	ID       string
	DeviceID string
	Topic    string
	Payload  []byte
	Seq      int64
	Time     time.Time
}

// Store 是线程安全的进程内消息存储。
// 幂等键为 (deviceID, messageID)：设备重连补投同一消息不会重复入库。
type Store struct {
	mu   sync.Mutex
	msgs map[string][]Message
	seen map[string]map[string]struct{} // deviceID -> 已接收 messageID 集合
}

// New 创建消息存储。
func New() *Store {
	return &Store{
		msgs: make(map[string][]Message),
		seen: make(map[string]map[string]struct{}),
	}
}

// Append 尝试入库。
// 返回 stored=true 表示首次接收并已入库；duplicated=true 表示同一设备同一 ID 重复上报。
func (s *Store) Append(deviceID string, m Message) (stored bool, duplicated bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen, ok := s.seen[deviceID]
	if !ok {
		seen = make(map[string]struct{})
		s.seen[deviceID] = seen
	}
	if _, dup := seen[m.ID]; dup {
		return false, true
	}
	seen[m.ID] = struct{}{}
	if m.DeviceID == "" {
		m.DeviceID = deviceID
	}
	s.msgs[deviceID] = append(s.msgs[deviceID], m)
	return true, false
}

// Messages 返回某设备已入库消息（按 Seq 升序，同序按入库顺序）。
func (s *Store) Messages(deviceID string) []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Message, len(s.msgs[deviceID]))
	copy(out, s.msgs[deviceID])
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Count 返回某设备已入库消息条数。
func (s *Store) Count(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs[deviceID])
}

// TotalCount 返回全部设备入库消息总数。
func (s *Store) TotalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ms := range s.msgs {
		n += len(ms)
	}
	return n
}
