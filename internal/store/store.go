package store

import (
	"sync"
	"time"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
)

// Record 是一条落库消息。
type Record struct {
	MessageID  string
	DeviceID   string
	Topic      string
	Payload    []byte
	QoS        int
	ReceivedAt time.Time
}

type deviceLog struct {
	records []Record
}

// Store 是带消息幂等去重的内存存储，同时实现 mqtt.Deduplicator。
// 去重键为 (设备ID, 消息ID)：设备断线重连后补投同一消息、或同一设备
// 并发重复上报，都只有第一次会真正入库。
type Store struct {
	mu    sync.Mutex
	logs  map[string]*deviceLog
	total int
}

// New 创建存储。
func New() *Store {
	return &Store{logs: make(map[string]*deviceLog)}
}

// Seen 是原子的“检查并登记”：
//   - 首次见到 (deviceID, msg.ID) -> 立即落库并返回 false（未重复）；
//   - 再次见到（含并发场景，同一临界区串行化）-> 返回 true，不重复落库。
func (s *Store) Seen(deviceID string, msg mqtt.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[deviceID]
	if !ok {
		log = &deviceLog{}
		s.logs[deviceID] = log
	}
	for _, r := range log.records {
		if r.MessageID == msg.ID {
			return true
		}
	}
	log.records = append(log.records, Record{
		MessageID:  msg.ID,
		DeviceID:   deviceID,
		Topic:      msg.Topic,
		Payload:    append([]byte(nil), msg.Payload...),
		QoS:        msg.QoS,
		ReceivedAt: time.Now(),
	})
	s.total++
	return false
}

// Ingest 显式写入，语义与 Seen 相同；inserted=false 表示重复被忽略。
func (s *Store) Ingest(deviceID string, msg mqtt.Message) (inserted bool) {
	return !s.Seen(deviceID, msg)
}

// Has 判断某条消息是否已入库。
func (s *Store) Has(deviceID, messageID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[deviceID]
	if !ok {
		return false
	}
	for _, r := range log.records {
		if r.MessageID == messageID {
			return true
		}
	}
	return false
}

// Count 返回累计入库消息数。
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// CountByDevice 返回某设备入库消息数。
func (s *Store) CountByDevice(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if log, ok := s.logs[deviceID]; ok {
		return len(log.records)
	}
	return 0
}

// Records 返回某设备的全部入库记录（按接收顺序，拷贝）。
func (s *Store) Records(deviceID string) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[deviceID]
	if !ok {
		return nil
	}
	out := make([]Record, len(log.records))
	copy(out, log.records)
	return out
}
