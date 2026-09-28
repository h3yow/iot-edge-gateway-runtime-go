package auth

import "testing"

func testACL(t *testing.T) *ACL {
	t.Helper()
	a := NewACL()
	if err := a.Grant("dev1",
		[]string{"devices/dev1/up", "devices/dev1/state"},
		[]string{"devices/dev1/cmd", "devices/dev1/cmd/#", "ota/+/dev1"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	return a
}

func TestCheckPublish(t *testing.T) {
	a := testACL(t)
	id := Identity{DeviceID: "dev1"}
	cases := []struct {
		name  string
		id    Identity
		topic string
		want  RejectReason
	}{
		{"允许", id, "devices/dev1/up", ReasonOK},
		{"未知设备", Identity{DeviceID: "ghost"}, "devices/dev1/up", ReasonUnknownDevice},
		{"越权发布他人主题", id, "devices/dev2/up", ReasonUnauthorizedPublish},
		{"非法主题", id, "devices//up", ReasonInvalidTopic},
		{"发布带通配符", id, "devices/dev1/+", ReasonWildcardOnPublish},
	}
	for _, c := range cases {
		got := a.Check(c.id, ActionPublish, c.topic)
		t.Logf("publish 判定输入 device=%q topic=%q => reason=%q（用例:%s）", c.id.DeviceID, c.topic, got, c.name)
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestCheckSubscribe(t *testing.T) {
	a := testACL(t)
	id := Identity{DeviceID: "dev1"}
	cases := []struct {
		topic string
		want  RejectReason
	}{
		{"devices/dev1/cmd", ReasonOK},
		{"devices/dev1/cmd/reboot", ReasonOK},             // 被 cmd/# 覆盖
		{"ota/1.0.0/dev1", ReasonOK},                      // 被 ota/+/dev1 覆盖
		{"devices/dev1/#", ReasonUnauthorizedSubscribe},   // 授予的过滤器没这么宽
		{"devices/dev2/cmd", ReasonUnauthorizedSubscribe}, // 他人主题
		{"devices//cmd", ReasonInvalidTopic},
	}
	for _, c := range cases {
		got := a.Check(id, ActionSubscribe, c.topic)
		t.Logf("subscribe 判定输入 topic=%q => reason=%q", c.topic, got)
		if got != c.want {
			t.Errorf("subscribe %q: got %q want %q", c.topic, got, c.want)
		}
	}
}
