package firmware

import "strconv"

// compareVersions 比较点分数字版本号（如 1.2.10），长度不足按 0 补齐。
// 非数字段退化到按字符串比较；返回 -1/0/1 表示 a 低于/等于/高于 b。
func compareVersions(a, b string) int {
	pa := splitVer(a)
	pb := splitVer(b)
	n := len(pa)
	if len(pb) > n {
		n = len(pb)
	}
	for len(pa) < n {
		pa = append(pa, "0")
	}
	for len(pb) < n {
		pb = append(pb, "0")
	}
	for i := 0; i < n; i++ {
		na, errA := strconv.Atoi(pa[i])
		nb, errB := strconv.Atoi(pb[i])
		if errA != nil || errB != nil {
			if pa[i] != pb[i] {
				if pa[i] < pb[i] {
					return -1
				}
				return 1
			}
			continue
		}
		if na != nb {
			if na < nb {
				return -1
			}
			return 1
		}
	}
	return 0
}

func splitVer(v string) []string {
	out := []string{}
	cur := ""
	for _, c := range v {
		if c == '.' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	return append(out, cur)
}
