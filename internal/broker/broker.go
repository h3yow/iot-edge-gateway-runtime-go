// Package broker 是进程内模拟的消息接入层：连接、订阅、层级匹配与发布鉴权。
package broker

import (
	"fmt"
	"sync"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/store"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/topic"
)

// Delivery 是投递给订阅者的消息。
type Delivery struct {
	Topic     string
	MessageID string
	Payload   []byte
	SenderID  string
}

// Handler 处理一条投递消息。
type Handler func(Delivery)

// PublishReceipt 是发布结论，原因可区分。
type PublishReceipt struct {
	Accepted   bool
	Reason     auth.RejectReason
	Stored     bool
	Duplicated bool
}

// Error 便于把拒绝原因作为错误展示。
func (r PublishReceipt) Error() string {
	if r.Accepted {
		return ""
	}
	return fmt.Sprintf("publish rejected: %s", r.Reason)
}

type subscription struct {
	filter  topic.Topic
	handler Handler
}

// Session 是一条设备连接。
type Session struct {
	broker   *Broker
	identity auth.Identity
	closed   bool
}

// Broker 是线程安全的进程内 Broker。
type Broker struct {
	mu     sync.RWMutex
	acl    *auth.ACL
	store  *store.Store
	online map[string]*Session
	subs   map[string][]subscription // deviceID -> 订阅列表
}

// New 创建 Broker。
func New(a *auth.ACL, st *store.Store) *Broker {
	return &Broker{
		acl:    a,
		store:  st,
		online: make(map[string]*Session),
		subs:   make(map[string][]subscription),
	}
}

// Connect 建立设备连接；未知设备被拒绝。
// 已在线的旧连接会被顶掉（模拟断线重连），其订阅清空，设备需重新订阅。
func (b *Broker) Connect(id auth.Identity) (*Session, error) {
	if !b.acl.IsKnown(id.DeviceID) {
		return nil, fmt.Errorf("connect rejected: %s", auth.ReasonUnknownDevice)
	}
	b.mu.Lock()
	if old := b.online[id.DeviceID]; old != nil {
		old.closed = true
	}
	s := &Session{broker: b, identity: id}
	b.online[id.DeviceID] = s
	b.subs[id.DeviceID] = nil
	b.mu.Unlock()
	return s, nil
}

// Close 断开连接。
func (s *Session) Close() {
	b := s.broker
	b.mu.Lock()
	defer b.mu.Unlock()
	s.closed = true
	if cur := b.online[s.identity.DeviceID]; cur == s {
		delete(b.online, s.identity.DeviceID)
	}
}

// Subscribe 按过滤器订阅，非法/越权订阅返回对应原因。
func (b *Broker) Subscribe(id auth.Identity, filter string, h Handler) error {
	reason := b.acl.Check(id, auth.ActionSubscribe, filter)
	if reason != auth.ReasonOK {
		return fmt.Errorf("subscribe rejected: %s", reason)
	}
	f, err := topic.ParseFilter(filter)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[id.DeviceID] = append(b.subs[id.DeviceID], subscription{filter: f, handler: h})
	return nil
}

// Unsubscribe 取消该设备对指定过滤器的全部订阅。
func (b *Broker) Unsubscribe(id auth.Identity, filter string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	list := b.subs[id.DeviceID]
	kept := list[:0]
	for _, sub := range list {
		if sub.filter.Raw != filter {
			kept = append(kept, sub)
		}
	}
	b.subs[id.DeviceID] = kept
}

// Publish 发布消息：鉴权 -> 去重入库 -> 按订阅过滤器层级匹配后投递。
// 重复 MessageID 不入库，但仍然投递给订阅者（幂等语义在“入库”侧）。
func (b *Broker) Publish(id auth.Identity, topicRaw, messageID string, payload []byte, seq int64) PublishReceipt {
	reason := b.acl.Check(id, auth.ActionPublish, topicRaw)
	if reason != auth.ReasonOK {
		return PublishReceipt{Accepted: false, Reason: reason}
	}
	tgt, err := topic.Parse(topicRaw)
	if err != nil {
		return PublishReceipt{Accepted: false, Reason: auth.ReasonInvalidTopic}
	}

	stored, duplicated := b.store.Append(id.DeviceID, storeMessage(id.DeviceID, topicRaw, messageID, payload, seq))

	// 收集匹配的订阅处理器（持锁期间只做收集，不做回调，避免回调重入死锁）。
	b.mu.RLock()
	var deliveries []struct {
		h Handler
		d Delivery
	}
	d := Delivery{Topic: topicRaw, MessageID: messageID, Payload: payload, SenderID: id.DeviceID}
	for devID, list := range b.subs {
		for _, sub := range list {
			if sub.filter.Match(tgt) {
				deliveries = append(deliveries, struct {
					h Handler
					d Delivery
				}{sub.handler, d})
				_ = devID
			}
		}
	}
	b.mu.RUnlock()

	for _, item := range deliveries {
		item.h(item.d)
	}
	return PublishReceipt{Accepted: true, Stored: stored, Duplicated: duplicated}
}

func storeMessage(deviceID, topicRaw, messageID string, payload []byte, seq int64) store.Message {
	return store.Message{
		ID:       messageID,
		DeviceID: deviceID,
		Topic:    topicRaw,
		Payload:  append([]byte(nil), payload...),
		Seq:      seq,
	}
}

// OnlineCount 返回当前在线连接数。
func (b *Broker) OnlineCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.online)
}

// IsOnline 判断设备是否在线。
func (b *Broker) IsOnline(deviceID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.online[deviceID] != nil
}
