package topic

import "testing"

func TestParseRejectsInvalid(t *testing.T) {
	bad := []string{"", "/a", "a/", "a//b", "a/+", "#/x", "x/#/y", "ab+", "a#"}
	for _, raw := range bad {
		if _, err := Parse(raw); err == nil {
			t.Errorf("Parse(%q) 应判非法却通过", raw)
		}
	}
	if _, err := Parse("a/b/c"); err != nil {
		t.Fatalf("合法发布主题被拒: %v", err)
	}
	if _, err := Parse("a/+/c"); err == nil {
		t.Error("发布主题包含 + 必须被拒")
	}
}

func TestParseFilter(t *testing.T) {
	if _, err := ParseFilter("a/+/c/#"); err != nil {
		t.Fatalf("合法过滤器被拒: %v", err)
	}
	if _, err := ParseFilter("a/#/b"); err == nil {
		t.Error("非结尾 # 必须被拒")
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter string
		target string
		want   bool
	}{
		{"devices/d1/up", "devices/d1/up", true},
		{"devices/+/up", "devices/d1/up", true},
		{"devices/+/up", "devices/d1/down", false},
		{"devices/#", "devices/d1/up", true},
		{"devices/#", "devices", true}, // '#' 匹配零层
		{"#", "a/b/c", true},
		{"+/+", "a/b", true},
		{"+/+", "a/b/c", false},
		{"a/b", "a/b/c", false},
	}
	for _, c := range cases {
		f, _ := ParseFilter(c.filter)
		tgt, _ := Parse(c.target)
		if got := f.Match(tgt); got != c.want {
			t.Errorf("filter %q match %q = %v, want %v", c.filter, c.target, got, c.want)
		}
	}
}
