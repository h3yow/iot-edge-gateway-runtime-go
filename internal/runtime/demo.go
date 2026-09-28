package runtime

import (
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
)

// StoreCount 返回某设备已入库的唯一消息条数。
func (r *Runtime) StoreCount(dev string) int { return r.Store.Count(dev) }

// DemoRejections 演示不同拒绝原因（非法主题 / 越权 / 通配符发布）。
func (r *Runtime) DemoRejections() {
	cases := []struct {
		dev   string
		topic string
		desc  string
	}{
		{"dev-A", "devices/dev-A/up", "合法主题（对照）"},
		{"dev-A", "devices/dev-B/up", "越权：替他人上报"},
		{"dev-A", "devices//up", "非法：空层级"},
		{"dev-A", "devices/+/up", "非法：发布带通配符"},
		{"ghost", "devices/ghost/up", "未知设备"},
	}
	for _, c := range cases {
		rc := r.Broker.Publish(auth.Identity{DeviceID: c.dev}, c.topic, "demo-"+c.dev, nil, 0)
		if rc.Accepted {
			r.log("拒绝演示 [%s] topic=%s => 接受 stored=%v", c.desc, c.topic, rc.Stored)
		} else {
			r.log("拒绝演示 [%s] topic=%s => 拒绝原因=%s", c.desc, c.topic, rc.Reason)
		}
	}
}
