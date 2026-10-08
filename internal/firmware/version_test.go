package firmware

import "testing"

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		name   string
		a, b   string
		expect int
	}{
		{"完全相同", "1.0.0", "1.0.0", 0},
		{"补丁位递增", "1.0.9", "1.0.10", -1},
		{"主次版本比较", "2.0", "1.9.9", 1},
		{"缺位补零-相等", "1.0", "1.0.0", 0},
		{"缺位补零-更大", "1.0.1", "1.0", 1},
		{"v 前缀等价", "v1.2", "1.2", 0},
		{"fw- 前缀等价", "fw-2.0", "2.0", 0},
		{"预发布早于正式版", "2.0-rc1", "2.0", -1},
		{"两个预发布数字段", "1.0-rc1", "1.0-rc2", -1},
		{"预发布数字段早于字符串段", "1.0-1", "1.0-alpha", -1},
		{"空版本早于任何版本", "", "1.0", -1},
		{"任何版本新于空版本", "1.0", "", 1},
		{"两个空版本相等", "", "", 0},
		{"典型降级判定 v3 对 v2", "2.0", "3.0", -1},
		{"典型接管判定 v3 对 v2", "3.0", "2.0", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := CompareVersions(c.a, c.b)
			t.Logf("关键输入 a=%q b=%q => 判定=%d（预期 %d）", c.a, c.b, got, c.expect)
			if got != c.expect {
				t.Fatalf("CompareVersions(%q,%q)=%d，预期 %d", c.a, c.b, got, c.expect)
			}
			// 对称性：反向比较结果必须取反
			if got != 0 {
				if rev := CompareVersions(c.b, c.a); rev != -got {
					t.Fatalf("对称性不成立 CompareVersions(%q,%q)=%d", c.b, c.a, rev)
				}
			}
		})
	}
}
