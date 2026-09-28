package mqtt

import (
	"errors"
	"sync"
)

// 结论原因常量：调用方据此按原因区分结论。
const (
	ReasonDelivered       = "delivered"
	ReasonDuplicate       = "duplicate"
	ReasonInvalidTopic    = "invalid_topic"
	ReasonInvalidFilter   = "invalid_filter"
	ReasonUnauthorizedPub = "unauthorized_publish"
	ReasonUnauthorizedSub = "unauthorized_subscribe"
)

// 鉴权/校验类哨兵错误，便于调用方 errors.Is 判定。
var (
	ErrUnauthorizedSubscribe = errors.New("mqtt: device not authorized to subscribe filter")
	ErrUnauthorizedPublish   = errors.New("mqtt: device not authorized to publish topic")
)

// Message 是进程内传递的一条消息。
type Message struct {
	ID      string
	Topic   string
	Payload []byte
	QoS     int
}

// Handler 是订阅回调。
type Handler func(Message)

// Authorizer 判定设备是否有权发布/订阅目标主题。
type Authorizer interface {
	CanPublish(deviceID, topic string) bool
	CanSubscribe(deviceID, filter string) bool
}

// Deduplicator 在消息扇出前做幂等判定。
// Seen 必须是“检查并登记”的原子操作：返回 true 表示该 (设备,消息ID)
// 此前已经处理过（重复上报/重连补投），broker 将不再扇出。
type Deduplicator interface {
	Seen(deviceID string, msg Message) bool
}

// PublishResult 描述一次发布的结论。
type PublishResult struct {
	Status    string // delivered | duplicate | rejected
	Reason    string
	Delivered int // 实际扇出到的订阅者数量
}

type subscription struct {
	deviceID string
	filter   string
	internal bool
	h        Handler
}

// Broker 是进程内的极简发布/订阅 broker。
type Broker struct {
	mu    sync.RWMutex
	auth  Authorizer
	dedup Deduplicator
	subs  []*subscription
}

// NewBroker 创建 broker；auth 与 dedup 均可为 nil（不做限制）。
func NewBroker(auth Authorizer, dedup Deduplicator) *Broker {
	return &Broker{auth: auth, dedup: dedup}
}

// Subscribe 按过滤器订阅。返回 error 区分两类拒绝原因：
//   - ErrInvalidFilter（过滤器非法）
//   - ErrUnauthorizedSubscribe（鉴权不通过）
func (b *Broker) Subscribe(deviceID, filter string, h Handler) error {
	if err := ValidateFilter(filter); err != nil {
		return err
	}
	if b.auth != nil && !b.auth.CanSubscribe(deviceID, filter) {
		return ErrUnauthorizedSubscribe
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		if s.deviceID == deviceID && s.filter == filter && !s.internal {
			s.h = h // 同设备同过滤器重复订阅：覆盖回调，不产生重复投递
			return nil
		}
	}
	b.subs = append(b.subs, &subscription{deviceID: deviceID, filter: filter, h: h})
	return nil
}

// AddInternalHandler 注册网关内部钩子（如落库），不经过设备鉴权。
func (b *Broker) AddInternalHandler(filter string, h Handler) error {
	if err := ValidateFilter(filter); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, &subscription{deviceID: "__internal__", filter: filter, h: h, internal: true})
	return nil
}

// Unsubscribe 取消某设备对过滤器的订阅。
func (b *Broker) Unsubscribe(deviceID, filter string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.subs[:0]
	for _, s := range b.subs {
		if s.deviceID == deviceID && s.filter == filter && !s.internal {
			continue
		}
		out = append(out, s)
	}
	b.subs = out
}

// Publish 发布一条消息。
// 判定顺序固定，保证拒绝原因可区分：
//  1. 主题非法      -> rejected/invalid_topic
//  2. 未授权发布    -> rejected/unauthorized_publish
//  3. 重复消息      -> duplicate/duplicate（不扇出、不入库）
//  4. 正常扇出      -> delivered/delivered
func (b *Broker) Publish(deviceID string, msg Message) PublishResult {
	if err := ValidateTopic(msg.Topic); err != nil {
		return PublishResult{Status: "rejected", Reason: ReasonInvalidTopic}
	}
	if b.auth != nil && !b.auth.CanPublish(deviceID, msg.Topic) {
		return PublishResult{Status: "rejected", Reason: ReasonUnauthorizedPub}
	}
	// 原子“检查并登记”，与扇出在同一临界区之外仍安全：Seen 自身保证
	// 同一消息 ID 的并发上报只有一个赢家能走到扇出。
	if b.dedup != nil && b.dedup.Seen(deviceID, msg) {
		return PublishResult{Status: "duplicate", Reason: ReasonDuplicate}
	}
	delivered := 0
	b.mu.RLock()
	snapshot := make([]*subscription, len(b.subs))
	copy(snapshot, b.subs)
	b.mu.RUnlock()
	for _, s := range snapshot {
		if Match(s.filter, msg.Topic) {
			s.h(msg)
			delivered++
		}
	}
	return PublishResult{Status: "delivered", Reason: ReasonDelivered, Delivered: delivered}
}
