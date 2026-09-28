package mqtt

import (
	"errors"
	"strings"
)

// 主题/过滤器校验错误。
var (
	ErrInvalidTopic = errors.New(
		"mqtt: invalid topic: topic must be non-empty and must not contain wildcard characters '+' or '#'")
	ErrInvalidFilter = errors.New(
		"mqtt: invalid filter: '#' must be the last level (whole level), '+' must occupy a whole level")
)

// splitLevels 按 '/' 切出主题层级。空层级在 MQTT 中是合法的不同层级，
// 例如 "a/" 与 "a" 是两个不同主题，因此这里不做剔除。
func splitLevels(s string) []string {
	return strings.Split(s, "/")
}

// ValidateTopic 校验具体发布主题：非空且不得包含通配符。
func ValidateTopic(topic string) error {
	if topic == "" {
		return ErrInvalidTopic
	}
	for _, lvl := range splitLevels(topic) {
		if strings.ContainsAny(lvl, "+#") {
			return ErrInvalidTopic
		}
	}
	return nil
}

// ValidateFilter 校验订阅过滤器：
//   - 非空；
//   - '#' 只能独占最后一层（"#" 或 "a/#"）；
//   - '+' 必须独占一整层（"+" 合法，"a+" 非法）。
func ValidateFilter(filter string) error {
	if filter == "" {
		return ErrInvalidFilter
	}
	levels := splitLevels(filter)
	for i, lvl := range levels {
		switch {
		case strings.Contains(lvl, "#"):
			if lvl != "#" || i != len(levels)-1 {
				return ErrInvalidFilter
			}
		case strings.Contains(lvl, "+"):
			if lvl != "+" {
				return ErrInvalidFilter
			}
		}
	}
	return nil
}

// Match 按 MQFT 规则判断具体主题是否匹配过滤器：
//   - '+' 恰好匹配一层；
//   - '#' 匹配零层或多层，因此 "a/#" 也匹配父主题 "a"。
func Match(filter, topic string) bool {
	f := splitLevels(filter)
	t := splitLevels(topic)
	i := 0
	for ; i < len(f); i++ {
		if f[i] == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if f[i] == "+" {
			continue
		}
		if f[i] != t[i] {
			return false
		}
	}
	return i == len(t)
}
