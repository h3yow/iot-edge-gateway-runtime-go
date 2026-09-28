// Package auth 维护设备身份与基于主题过滤器的发布/订阅权限模型。
package auth

import "github.com/h3yow/iot-edge-gateway-runtime-go/internal/topic"

// RejectReason 用于区分拒绝接入的具体原因。
type RejectReason string

const (
	ReasonOK                    RejectReason = ""
	ReasonInvalidTopic          RejectReason = "invalid_topic"
	ReasonWildcardOnPublish     RejectReason = "wildcard_not_allowed_on_publish"
	ReasonUnknownDevice         RejectReason = "unknown_device"
	ReasonUnauthorizedPublish   RejectReason = "unauthorized_publish"
	ReasonUnauthorizedSubscribe RejectReason = "unauthorized_subscribe"
)

// Error 使 RejectReason 可作为错误返回。
func (r RejectReason) Error() string { return string(r) }

// Action 表示接入动作。
type Action string

const (
	ActionPublish   Action = "publish"
	ActionSubscribe Action = "subscribe"
)

// Identity 是设备连接时出示的身份。
type Identity struct {
	DeviceID string
	Token    string
}

// ACL 是设备到发布/订阅过滤器集合的授权表。
type ACL struct {
	devices map[string]bool
	pub     map[string][]topic.Topic
	sub     map[string][]topic.Topic
}

// NewACL 创建空授权表。
func NewACL() *ACL {
	return &ACL{
		devices: make(map[string]bool),
		pub:     make(map[string][]topic.Topic),
		sub:     make(map[string][]topic.Topic),
	}
}

// Grant 注册设备并授予其发布/订阅过滤器权限。
func (a *ACL) Grant(deviceID string, publishFilters, subscribeFilters []string) error {
	a.devices[deviceID] = true
	for _, raw := range publishFilters {
		f, err := topic.ParseFilter(raw)
		if err != nil {
			return err
		}
		a.pub[deviceID] = append(a.pub[deviceID], f)
	}
	for _, raw := range subscribeFilters {
		f, err := topic.ParseFilter(raw)
		if err != nil {
			return err
		}
		a.sub[deviceID] = append(a.sub[deviceID], f)
	}
	return nil
}

// IsKnown 判断设备是否已注册。
func (a *ACL) IsKnown(deviceID string) bool { return a.devices[deviceID] }

// Check 校验某身份对主题执行动作是否合法，返回可区分的拒绝原因。
// 判定顺序：未知设备 -> 主题非法 -> 越权。
func (a *ACL) Check(id Identity, action Action, rawTopic string) RejectReason {
	if !a.devices[id.DeviceID] {
		return ReasonUnknownDevice
	}
	switch action {
	case ActionPublish:
		tgt, err := topic.Parse(rawTopic)
		if err != nil {
			if containsWildcard(rawTopic) {
				return ReasonWildcardOnPublish
			}
			return ReasonInvalidTopic
		}
		for _, f := range a.pub[id.DeviceID] {
			if f.Match(tgt) {
				return ReasonOK
			}
		}
		return ReasonUnauthorizedPublish
	case ActionSubscribe:
		f, err := topic.ParseFilter(rawTopic)
		if err != nil {
			return ReasonInvalidTopic
		}
		// 只要存在一条被授予的过滤器“覆盖”订阅过滤器即可。
		for _, granted := range a.sub[id.DeviceID] {
			if covers(granted, f) {
				return ReasonOK
			}
		}
		return ReasonUnauthorizedSubscribe
	default:
		return ReasonInvalidTopic
	}
}

// covers 判断 granted 是否覆盖 want：即 want 能匹配的任意具体主题，granted 都能匹配。
func covers(granted, want topic.Topic) bool {
	gi, wi := 0, 0
	for wi < len(want.Segments) {
		if gi >= len(granted.Segments) {
			return false // granted 更短且无 '#' 兜底
		}
		g, w := granted.Segments[gi], want.Segments[wi]
		switch {
		case g == "#":
			return true // '#' 匹配任意剩余层级
		case w == "#":
			// want 要覆盖更深层，granted 同位置必须也是 '#'
			return g == "#"
		case w == "+":
			// granted 必须同样宽（'+'）或更宽（'#'），具体段覆盖不了 '+'
			if g != "+" && g != "#" {
				return false
			}
		default:
			// want 为具体段：granted 为同值具体段、'+'、'#' 均可
			if g != "+" && g != w {
				return false
			}
		}
		gi++
		wi++
	}
	// want 已耗尽；granted 剩余的只能恰好是结尾 '#'，或同时结束。
	if gi == len(granted.Segments) {
		return true
	}
	return gi == len(granted.Segments)-1 && granted.Segments[gi] == "#"
}

func containsWildcard(raw string) bool {
	for _, r := range raw {
		if r == '+' || r == '#' {
			return true
		}
	}
	return false
}
