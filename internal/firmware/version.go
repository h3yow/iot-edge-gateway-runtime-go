// Package firmware 的版本裁决：发布单之间按版本号高低决定接管/拒绝关系。
package firmware

import (
	"strconv"
	"strings"
)

// CompareVersions 裁决两个固件版本的先后：
//
//	返回 -1 表示 a < b（a 更旧），0 表示相等，1 表示 a > b（a 更新）。
//
// 规则：
//  1. 去除常见前缀 v/V/fw-/FW-/firmware- 后，取首个 '-' 之前为主版本序列；
//  2. 主版本序列按 '.' 切分：能按整数解析就按整数比较（10 > 9，
//     避免字符串序把 10 排在 9 前），否则按字符串比较；数字段新于非数字段；
//  3. 主版本完全相等时，带预发布后缀（'-' 之后）的更旧（2.0-rc1 < 2.0）；
//     双方都带后缀则按后缀字符串比较；
//  4. 主版本段数不同时逐段比较，公共段相等则段多者新（1.0.1 > 1.0）；
//  5. 空串视为最旧（设备无已记录版本时的起点）。
func CompareVersions(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return -1
	}
	if b == "" {
		return 1
	}
	ca, sa := parseVer(a)
	cb, sb := parseVer(b)
	n := len(ca)
	if len(cb) < n {
		n = len(cb)
	}
	for i := 0; i < n; i++ {
		if c := compareSegment(ca[i], cb[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(ca) > len(cb):
		return 1
	case len(ca) < len(cb):
		return -1
	}
	// 主版本序列相等：无预发布后缀的新于带后缀的。
	switch {
	case sa == "" && sb != "":
		return 1
	case sa != "" && sb == "":
		return -1
	case sa != sb:
		return strings.Compare(sa, sb)
	default:
		return 0
	}
}

// parseVer 返回 (主版本各段, 预发布后缀)，已做前缀归一化。
func parseVer(v string) ([]string, string) {
	lower := strings.ToLower(v)
	for _, p := range []string{"firmware-", "fw-", "v"} {
		if strings.HasPrefix(lower, p) {
			v = v[len(p):]
			break
		}
	}
	core, suffix := v, ""
	if i := strings.IndexByte(v, '-'); i >= 0 {
		core, suffix = v[:i], v[i+1:]
	}
	return strings.Split(core, "."), suffix
}

func compareSegment(x, y string) int {
	nx, errX := strconv.Atoi(x)
	ny, errY := strconv.Atoi(y)
	switch {
	case errX == nil && errY == nil:
		switch {
		case nx < ny:
			return -1
		case nx > ny:
			return 1
		default:
			return 0
		}
	case errX == nil: // 数字段新于非数字段
		return 1
	case errY == nil:
		return -1
	default:
		return strings.Compare(x, y)
	}
}
