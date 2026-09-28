package auth

import (
	"strings"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
)

// Action 表示授权动作类型。
type Action string

const (
	ActionPublish   Action = "pub"
	ActionSubscribe Action = "sub"
)

// Permission 是一条授权：允许某设备对某主题过滤器执行某动作。
type Permission struct {
	DeviceID string
	Action   Action
	Filter   string
}

// Policy 是基于主题过滤器的授权表。
// 模型为“每设备显式授权、默认拒绝”：只有调用 Grant 明确放行的
// (设备, 动作, 过滤器) 才被允许，未授权一律拒绝。过滤器可含通配符，
// 因此一条授权可以覆盖一个主题层级（如 dev/42/telemetry/#）。
type Policy struct {
	perms map[string][]Permission // deviceID -> 授权列表
}

// NewPolicy 创建空授权表（默认全部拒绝）。
func NewPolicy() *Policy {
	return &Policy{perms: make(map[string][]Permission)}
}

// Grant 添加一条授权；非法过滤器会被忽略（调用方可先 mqtt.ValidateFilter）。
// 重复 Grant 同一条目不会产生重复授权。
func (p *Policy) Grant(deviceID string, action Action, filter string) error {
	if err := mqtt.ValidateFilter(filter); err != nil {
		return err
	}
	for _, existing := range p.perms[deviceID] {
		if existing.Action == action && existing.Filter == filter {
			return nil
		}
	}
	p.perms[deviceID] = append(p.perms[deviceID], Permission{
		DeviceID: deviceID,
		Action:   action,
		Filter:   filter,
	})
	return nil
}

// DenyAll 撤销某设备的全部授权。
func (p *Policy) DenyAll(deviceID string) {
	delete(p.perms, deviceID)
}

// Revoke 撤销单条授权。
func (p *Policy) Revoke(deviceID string, action Action, filter string) {
	list := p.perms[deviceID]
	out := list[:0]
	for _, perm := range list {
		if perm.Action == action && perm.Filter == filter {
			continue
		}
		out = append(out, perm)
	}
	p.perms[deviceID] = out
}

// CanPublish 判定设备能否发布到具体主题：任意一条 pub 授权过滤器匹配即放行。
// 发布主题本身若含通配符（非法）直接拒绝。
func (p *Policy) CanPublish(deviceID, topic string) bool {
	if err := mqtt.ValidateTopic(topic); err != nil {
		return false
	}
	for _, perm := range p.perms[deviceID] {
		if perm.Action == ActionPublish && mqtt.Match(perm.Filter, topic) {
			return true
		}
	}
	return false
}

// CanSubscribe 判定设备能否订阅该过滤器：任意一条 sub 授权过滤器覆盖即放行。
// 过滤器本身非法直接拒绝（拒绝原因由 broker 的校验阶段区分）。
func (p *Policy) CanSubscribe(deviceID string, filter string) bool {
	if err := mqtt.ValidateFilter(filter); err != nil {
		return false
	}
	for _, perm := range p.perms[deviceID] {
		if perm.Action != ActionSubscribe {
			continue
		}
		// 授权过滤器 "a/#" 覆盖更窄的订阅 "a/b"；
		// 反之订阅 "a/#" 不被授权 "a/b" 覆盖，避免越权扩大可见范围。
		if filterCoveredBy(perm.Filter, filter) {
			return true
		}
	}
	return false
}

// filterCoveredBy 判断 granted 过滤器是否覆盖 requested 过滤器：
// 请求订阅可见的每一个具体主题，都必须落在授权过滤器可见集合内。
// 规则：逐层比较；granted 出现 '#' 即覆盖其后所有层级；
// granted 出现 '+' 时 requested 同层必须恰好一层；普通串必须相等。
func filterCoveredBy(granted, requested string) bool {
	g := strings.Split(granted, "/")
	r := strings.Split(requested, "/")
	for i := 0; i < len(g); i++ {
		if g[i] == "#" {
			return true
		}
		if i >= len(r) {
			return false
		}
		if r[i] == "#" {
			// 请求比授权更宽（如 granted=a/b, request=a/#），不覆盖。
			return false
		}
		if g[i] == "+" {
			continue // granted 的 + 覆盖 requested 同层（普通串或同为 +）
		}
		if g[i] != r[i] {
			return false
		}
	}
	return len(g) == len(r)
}
