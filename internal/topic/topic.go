// Package topic 提供 MQTT 风格分层主题的解析与通配符匹配。
package topic

import (
	"errors"
	"strings"
)

// ErrInvalid 表示主题非法（空层级、通配符位置错误等）。
var ErrInvalid = errors.New("invalid topic")

const (
	wildSingle = "+" // 匹配恰好一个层级
	wildMulti  = "#" // 只能位于末尾，匹配零个或多个层级
)

// Topic 是按 '/' 切分后的主题。IsFilter 为 true 时允许包含通配符。
type Topic struct {
	Segments []string
	Raw      string
	IsFilter bool
}

// split 按 '/' 切分并拒绝空层级。
func split(raw string) ([]string, error) {
	if raw == "" {
		return nil, ErrInvalid
	}
	segs := strings.Split(raw, "/")
	for _, s := range segs {
		if s == "" {
			return nil, ErrInvalid // 拒绝 "/a"、"a//b"、"a/" 等
		}
	}
	return segs, nil
}

// Parse 解析普通发布主题（不允许通配符）。
func Parse(raw string) (Topic, error) {
	segs, err := split(raw)
	if err != nil {
		return Topic{}, err
	}
	for _, seg := range segs {
		if seg == wildSingle || seg == wildMulti || strings.ContainsAny(seg, "+#") {
			return Topic{}, ErrInvalid
		}
	}
	return Topic{Segments: segs, Raw: raw, IsFilter: false}, nil
}

// ParseFilter 解析订阅过滤器（允许 '+' 与结尾 '#'）。
func ParseFilter(raw string) (Topic, error) {
	segs, err := split(raw)
	if err != nil {
		return Topic{}, err
	}
	for i, s := range segs {
		if s == wildMulti && i != len(segs)-1 {
			return Topic{}, ErrInvalid // '#' 只能是最后一个层级
		}
		if strings.ContainsAny(s, "+#") && s != wildSingle && s != wildMulti {
			return Topic{}, ErrInvalid // 拒绝 "foo+"、"a#b" 之类混合写法
		}
	}
	return Topic{Segments: segs, Raw: raw, IsFilter: true}, nil
}

// Match 判断过滤器 t 是否能匹配目标主题 target。
// '+' 匹配恰好一个层级；结尾 '#' 匹配零个或多个层级。
func (t Topic) Match(target Topic) bool {
	for i, seg := range t.Segments {
		if seg == wildMulti {
			return true // 末尾 '#'，匹配目标剩余的任意层级（含零层）
		}
		if i >= len(target.Segments) {
			return false // 过滤器比目标长，且当前不是 '#'
		}
		if seg != wildSingle && seg != target.Segments[i] {
			return false
		}
	}
	// 过滤器无 '#' 收尾时，层级数必须完全一致
	return len(t.Segments) == len(target.Segments)
}
