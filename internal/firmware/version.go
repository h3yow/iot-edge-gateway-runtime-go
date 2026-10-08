// 版本先后裁决：用于并行发布时判断“能否下发 / 是否降级 / 是否被接管”。
package firmware

import (
	"strconv"
	"strings"
)

// CompareVersions 比较两个固件版本号的先后。
//
// 返回：-1 表示 a 早于 b；0 表示相同；1 表示 a 新于 b。
//
// 解析规则（尽量宽容，兼容各团队不同命名习惯）：
//   - 忽略首个数字段之前的前缀，如 "v1.2"、"fw-1.2" 都按 1.2 处理；
//   - 主体按 "." 拆分为数字段逐段比较数值（"1.0.10" > "1.0.9"），缺位按 0 补齐；
//   - "-" 之后视为预发布后缀：有后缀的早于无后缀的正式版（"2.0-rc1" < "2.0"），
//     两个预发布版本按后缀分段比较（数字段比数值，其余按字符串）；
//   - 空串视为“设备上无版本”，早于任何非空版本；
//   - 完全无法解析出数字的版本回退为字符串字典序，保证仍有确定结论。
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
	ca, preA, okA := parseVersion(a)
	cb, preB, okB := parseVersion(b)
	if !okA || !okB {
		return strings.Compare(a, b)
	}
	if c := compareNumericSegments(ca, cb); c != 0 {
		return c
	}
	return comparePrerelease(preA, preB)
}

// parseVersion 去掉非数字前缀，拆出数字主体段与预发布后缀。
// ok=false 表示找不到任何数字，调用方应回退到字符串比较。
func parseVersion(s string) (coreSegs []int, prerelease string, ok bool) {
	start := strings.IndexFunc(s, func(r rune) bool { return r >= '0' && r <= '9' })
	if start < 0 {
		return nil, "", false
	}
	body := s[start:]
	if i := strings.IndexByte(body, '-'); i >= 0 {
		prerelease = body[i+1:]
		body = body[:i]
	}
	for _, seg := range strings.Split(body, ".") {
		coreSegs = append(coreSegs, leadingInt(seg))
	}
	return coreSegs, prerelease, true
}

// leadingInt 取分段的前导数字；没有数字则返回 0。
func leadingInt(seg string) int {
	end := 0
	for end < len(seg) && seg[end] >= '0' && seg[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0
	}
	n, err := strconv.Atoi(seg[:end])
	if err != nil {
		return 0
	}
	return n
}

func compareNumericSegments(a, b []int) int {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		x, y := 0, 0
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
	}
	return 0
}

// comparePrerelease：空（正式版）> 非空（预发布）；都非空则按 "." 分段比较。
func comparePrerelease(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return 1 // a 是正式版
	}
	if b == "" {
		return -1
	}
	sa := strings.Split(a, ".")
	sb := strings.Split(b, ".")
	n := len(sa)
	if len(sb) > n {
		n = len(sb)
	}
	for i := 0; i < n; i++ {
		var x, y string
		if i < len(sa) {
			x = sa[i]
		}
		if i < len(sb) {
			y = sb[i]
		}
		if x == y {
			continue
		}
		xn, xErr := strconv.Atoi(x)
		yn, yErr := strconv.Atoi(y)
		switch {
		case xErr == nil && yErr == nil:
			if xn < yn {
				return -1
			}
			return 1
		case xErr == nil: // 纯数字段早于字符串段（SemVer 约定）
			return -1
		case yErr == nil:
			return 1
		default:
			return strings.Compare(x, y)
		}
	}
	return 0
}
