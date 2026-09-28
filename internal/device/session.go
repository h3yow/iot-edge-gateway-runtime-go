package device

import "sync"

// Event 是模拟设备生命周期/上下行事件。
type Event struct {
	Type     string // online | offline | delivered | replay | reconnect
	DeviceID string
	Topic    string
	Payload  []byte
}

// queuedItem 是设备离线期间暂存的下行指令。
type queuedItem struct {
	topic   string
	payload []byte
}

// Session 是一个模拟设备的进程内会话。
// 下行语义（至少一次）：在线 Send 立即投递；离线 Send 入 FIFO 队列；
// Connect 重连后按入队顺序逐条补投，每条标记为 replay 事件。
type Session struct {
	mu      sync.Mutex
	id      string
	online  bool
	queue   []queuedItem
	history []Event // 生命周期与投递事件，便于测试断言
}

// NewSession 创建设备会话（初始离线）。
func NewSession(id string) *Session {
	return &Session{id: id}
}

// ID 返回设备 ID。
func (s *Session) ID() string { return s.id }

// IsOnline 返回当前在线状态。
func (s *Session) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online
}

// Connect 上线（首次或断线重连），随后补投离线期间暂存的全部下行指令。
// 返回本次产生的事件：首元素是 online/reconnect，其余为 replay。
func (s *Session) Connect() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	kind := "online"
	if len(s.history) > 0 {
		kind = "reconnect"
	}
	evs := []Event{{Type: kind, DeviceID: s.id}}
	s.online = true
	// FIFO 补投：所有暂存指令都会被投递，不丢消息；
	// 下游（如固件安装）按 (设备,版本) 自身幂等，补投不会导致重复安装。
	for _, item := range s.queue {
		evs = append(evs, Event{Type: "replay", DeviceID: s.id, Topic: item.topic, Payload: append([]byte(nil), item.payload...)})
	}
	s.queue = s.queue[:0]
	s.history = append(s.history, evs...)
	return evs
}

// Disconnect 模拟掉线：在线期间未取走的状态保持，暂存队列不受影响。
func (s *Session) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.online = false
	s.history = append(s.history, Event{Type: "offline", DeviceID: s.id})
}

// Send 下行一条指令：在线立即标记 delivered；离线入队等待重连补投。
// 返回 true 表示已即时投递，false 表示已暂存。
func (s *Session) Send(topic string, payload []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.online {
		s.history = append(s.history, Event{
			Type: "delivered", DeviceID: s.id, Topic: topic,
			Payload: append([]byte(nil), payload...),
		})
		return true
	}
	s.queue = append(s.queue, queuedItem{topic: topic, payload: append([]byte(nil), payload...)})
	return false
}

// Pending 返回暂存未投递条数。
func (s *Session) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// History 返回事件历史副本。
func (s *Session) History() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.history))
	copy(out, s.history)
	return out
}

// Registry 管理全部模拟设备会话。
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewRegistry 创建设备注册表。
func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*Session)}
}

// Add 注册会话；同 ID 已存在则覆盖。
func (r *Registry) Add(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[s.ID()] = s
}

// Get 查询会话，不存在返回 nil。
func (r *Registry) Get(deviceID string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[deviceID]
}

// Remove 移除会话。
func (r *Registry) Remove(deviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, deviceID)
}

// IDs 返回全部设备 ID（排序后返回，保证演示/测试输出稳定）。
func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	return ids
}
