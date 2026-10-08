package firmware

import "testing"

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		name   string
		a, b   string
		expect int
	}{
		{"相等", "2.0.0", "2.0.0", 0},
		{"v 前缀相等", "v2.0", "2.0", 0},
		{"fw 前缀相等", "fw-2.0", "2.0", 0},
		{"数字大小非字典序", "1.10.0", "1.9.0", 1},
		{"主版本新旧", "3.0", "2.9.9", 1},
		{"补丁位新旧", "2.0.1", "2.0.0", 1},
		{"段多者新", "1.0.1", "1.0", 1},
		{"旧版判定", "1.0", "2.0", -1},
		{"空串最旧", "", "1.0", -1},
		{"非空对空", "1.0", "", 1},
		{"两空相等", "", "", 0},
		{"预发布段旧于数字段", "2.0-rc1", "2.0", -1},
		{"大小写前缀", "V2.1", "v2.0", 1},
		{"示例版本", "fw-2.0", "fw-1.0", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := CompareVersions(c.a, c.b)
			t.Logf("判定输入 a=%q b=%q => %d（预期 %d）", c.a, c.b, got, c.expect)
			if got != c.expect {
				t.Fatalf("CompareVersions(%q,%q)=%d, 预期 %d", c.a, c.b, got, c.expect)
			}
		})
	}
}
