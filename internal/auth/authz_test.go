package auth

import "testing"

func TestPolicyPublish(t *testing.T) {
	p := NewPolicy()
	if err := p.Grant("dev-1", ActionPublish, "devices/dev-1/telemetry/#"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		device, topic string
		want          bool
		reason        string
	}{
		{"dev-1", "devices/dev-1/telemetry/temp", true, "授权过滤器前缀匹配"},
		{"dev-1", "devices/dev-1/telemetry/a/b", true, "# 匹配多层"},
		{"dev-1", "devices/dev-2/telemetry/temp", false, "越权：他机主题"},
		{"dev-1", "devices/dev-1/cmd/x", false, "仅有 pub 授权的 telemetry 前缀"},
		{"dev-2", "devices/dev-1/telemetry/temp", false, "dev-2 无任何授权（默认拒绝）"},
		{"dev-1", "devices/dev-1/telemetry/+", false, "发布主题不允许通配符"},
	}
	for _, c := range cases {
		got := p.CanPublish(c.device, c.topic)
		t.Logf("CanPublish(device=%q topic=%q)=%v want=%v 判定依据:%s", c.device, c.topic, got, c.want, c.reason)
		if got != c.want {
			t.Errorf("CanPublish(%q,%q)=%v want %v", c.device, c.topic, got, c.want)
		}
	}
}

func TestPolicySubscribeCoverage(t *testing.T) {
	p := NewPolicy()
	if err := p.Grant("dev-1", ActionSubscribe, "devices/dev-1/cmd/#"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		device, filter string
		want           bool
		reason         string
	}{
		{"dev-1", "devices/dev-1/cmd/install", true, "授权过滤器覆盖更窄订阅"},
		{"dev-1", "devices/dev-1/cmd/#", true, "完全相同过滤器"},
		{"dev-1", "devices/dev-1/#", false, "订阅比授权宽，会越权看到他类主题"},
		{"dev-1", "devices/+/cmd/#", false, "通配扩大到他机，拒绝"},
		{"dev-1", "devices/dev-2/cmd/#", false, "他机命名空间"},
		{"dev-1", "bad/#/x", false, "非法过滤器本身拒绝"},
	}
	for _, c := range cases {
		got := p.CanSubscribe(c.device, c.filter)
		t.Logf("CanSubscribe(device=%q filter=%q)=%v want=%v 判定依据:%s", c.device, c.filter, got, c.want, c.reason)
		if got != c.want {
			t.Errorf("CanSubscribe(%q,%q)=%v want %v", c.device, c.filter, got, c.want)
		}
	}

	// 撤销后立即失效
	p.Revoke("dev-1", ActionSubscribe, "devices/dev-1/cmd/#")
	if p.CanSubscribe("dev-1", "devices/dev-1/cmd/install") {
		t.Fatal("revoke 后授权应失效")
	}
	t.Log("Revoke 后 devices/dev-1/cmd/install -> false，符合预期")

	// 非法过滤器 Grant 必须报错
	if err := p.Grant("dev-1", ActionSubscribe, "a/#/b"); err == nil {
		t.Fatal("非法过滤器 Grant 应返回错误")
	}
}
