package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/gateway"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime)
	section := func(name string) {
		logger.Printf("\n==== %s ====", name)
	}

	gw := gateway.New(gateway.Config{MaxConcurrentPublishes: 3, ConflictPolicy: "reject"})
	devices := []string{"dev-1", "dev-2", "dev-3", "dev-4", "dev-5", "dev-6"}
	for _, id := range devices {
		gw.RegisterDevice(id)
	}
	// 一个只订阅不发布的观察服务账号，演示层级订阅。
	_ = gw.Policy.Grant("monitor", auth.ActionSubscribe, "devices/+/telemetry/temp")
	received := make(chan mqtt.Message, 32)
	if err := gw.Broker.Subscribe("monitor", "devices/+/telemetry/temp", func(m mqtt.Message) {
		received <- m
	}); err != nil {
		logger.Fatalf("monitor subscribe: %v", err)
	}

	// ---------- 场景 1：多设备并发上报 + 同设备重复/补投幂等 ----------
	section("场景1 并发上报 / 重复上报 / 断线补投（不重复入库、不丢消息）")
	var wg sync.WaitGroup
	publish := func(dev, msgID, topic string, qos int) {
		defer wg.Done()
		res := gw.IngestTelemetry(dev, mqtt.Message{ID: msgID, Topic: topic, Payload: []byte(msgID), QoS: qos})
		logger.Printf("[上报] device=%s id=%s topic=%s -> status=%s reason=%s delivered=%d",
			dev, msgID, topic, res.Status, res.Reason, res.Delivered)
	}
	for i, dev := range devices {
		wg.Add(1)
		go publish(dev, fmt.Sprintf("msg-%d", i), fmt.Sprintf("devices/%s/telemetry/temp", dev), 1)
	}
	// dev-1 重复上报同一条消息（并发竞态）
	wg.Add(2)
	go publish("dev-1", "msg-0", "devices/dev-1/telemetry/temp", 1)
	go publish("dev-1", "msg-0", "devices/dev-1/telemetry/temp", 1)
	// dev-1 断线后重连补投同一条消息（QoS1 语义，ID 相同）
	if err := gw.Disconnect("dev-1"); err != nil {
		logger.Fatalf("disconnect: %v", err)
	}
	if _, err := gw.Connect("dev-1"); err != nil {
		logger.Fatalf("reconnect: %v", err)
	}
	wg.Add(1)
	go publish("dev-1", "msg-0", "devices/dev-1/telemetry/temp", 1)
	wg.Wait()
	logger.Printf("[入库] total=%d (期望 6) dev-1=%d (期望 1) 投递日志=%d",
		gw.Store.Count(), gw.Store.CountByDevice("dev-1"), gw.DeliveredCount())

	// ---------- 场景 2：非法主题 vs 未授权，原因可区分 ----------
	section("场景2 拒绝原因区分：非法主题 / 未授权发布 / 未授权订阅")
	r1 := gw.IngestTelemetry("dev-1", mqtt.Message{ID: "bad-1", Topic: "devices/dev-1/telemetry/#"})
	logger.Printf("[拒绝] 主题含通配符 -> status=%s reason=%s", r1.Status, r1.Reason)
	r2 := gw.IngestTelemetry("dev-1", mqtt.Message{ID: "bad-2", Topic: "devices/dev-2/telemetry/temp"})
	logger.Printf("[拒绝] 越权发到他机主题 -> status=%s reason=%s", r2.Status, r2.Reason)
	err := gw.Broker.Subscribe("dev-1", "devices/dev-2/cmd/#", func(mqtt.Message) {})
	logger.Printf("[拒绝] 越权订阅他机指令 -> err=%v", err)

	// ---------- 场景 3：影子离线暂存、重连合并、冲突裁决、请求幂等 ----------
	section("场景3 设备影子：离线暂存 / 按版本合并 / 冲突不静默 / 下发幂等")
	if err := gw.Disconnect("dev-2"); err != nil {
		logger.Fatalf("disconnect dev-2: %v", err)
	}
	offlineReq, err := gw.UpdateDesired("req-1", "dev-2", 2, map[string]string{"color": "blue"})
	if err != nil {
		log.Fatal(err)
	}
	logger.Printf("[影子] 离线期望 req-1 v2 -> status=%s reason=%s pending=%d",
		offlineReq.Status, offlineReq.Reason, gw.Shadow.PendingCount("dev-2"))
	// 同一请求重复下发：幂等
	dupReq, _ := gw.UpdateDesired("req-1", "dev-2", 2, map[string]string{"color": "blue"})
	logger.Printf("[影子] req-1 重复下发 -> status=%s reason=%s", dupReq.Status, dupReq.Reason)
	// 设备离线期间带着旧值上报：reported 入库，重连后做版本对账
	rep, _ := gw.Shadow.ReportState("dev-2", 1, map[string]string{"color": "red"})
	for _, d := range rep.Decisions {
		logger.Printf("[影子] 离线前上报对账 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
	}
	mergeRes, err := gw.Connect("dev-2")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range mergeRes.Decisions {
		logger.Printf("[影子] 重连合并 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
	}
	// 同版本不同值：冲突必须显式裁决，不静默覆盖
	conflict, _ := gw.UpdateDesired("req-2", "dev-2", 2, map[string]string{"color": "green"})
	for _, d := range conflict.Decisions {
		logger.Printf("[影子] 同版本冲突 field=%s verdict=%s detail=%s", d.Field, d.Verdict, d.Detail)
	}

	// ---------- 场景 4：固件分批灰度 + 中途失败回滚 ----------
	section("场景4 固件灰度发布：灰度波失败率超阈值 -> 整体回滚")
	gw.FailInstall("dev-2") // 目标排序后灰度波(dev-1,dev-2)中 dev-2 失败：50% > 阈值0%
	targets := []string{"dev-1", "dev-2", "dev-3", "dev-4", "dev-5", "dev-6"}
	prog, rel, err := gw.PublishFirmware("rel-1", "2.0.0", 2, 30, 0, targets)
	logger.Printf("[固件] rel-1 v2.0.0 灰度30%% 批2 失败阈值0%% -> err=%v", err)
	logger.Printf("[固件] 终态 status=%s reason=%s success=%d failed=%d rolledBack=%d cancelled=%d",
		prog.Status, prog.Reason, prog.Succeeded, prog.Failed, prog.RolledBack, prog.Cancelled)
	if rel != nil {
		for _, r := range gw.Firmware.Installs(rel.ID) {
			logger.Printf("[固件] 设备=%s 状态=%s 说明=%s", r.DeviceID, r.Status, r.Detail)
		}
	}
	// 关键一致性断言：dev-1 曾安装成功，但随回滚，当前生效版本必须恢复为空。
	logger.Printf("[固件] 回滚后 dev-1 生效版本=%q (期望空)", gw.Firmware.InstalledVersion("dev-1"))
	gw.PassInstall("dev-2")

	// ---------- 场景 5：正常灰度成功后，同版本重发必须幂等跳过 ----------
	section("场景5 回滚后重新发布成功，随后同版本再发必须幂等跳过")
	prog2, rel2, err := gw.PublishFirmware("rel-2", "2.0.0", 2, 30, 50, targets)
	if err != nil {
		log.Fatal(err)
	}
	logger.Printf("[固件] rel-2 v2.0.0 回滚后首次发布 -> status=%s reason=%s success=%d skipped=%d err=%v",
		prog2.Status, prog2.Reason, prog2.Succeeded, prog2.Skipped, err)
	for _, r := range gw.Firmware.Installs(rel2.ID) {
		logger.Printf("[固件] 设备=%s 状态=%s 说明=%s", r.DeviceID, r.Status, r.Detail)
	}
	// 设备现已全部在 2.0.0，同版本再发一次：必须全部 skipped、安装器零调用
	prog2b, _, err := gw.PublishFirmware("rel-2b", "2.0.0", 2, 30, 50, targets)
	if err != nil {
		log.Fatal(err)
	}
	logger.Printf("[固件] rel-2b v2.0.0 同版本再发 -> status=%s reason=%s success=%d skipped=%d err=%v",
		prog2b.Status, prog2b.Reason, prog2b.Succeeded, prog2b.Skipped, err)
	for _, d := range devices {
		logger.Printf("[固件] %s 安装器真实调用次数=%d（rel-1灰度波1次+rel-2一次，rel-2b必须0次）", d, gw.InstallAttempts(d))
	}

	// ---------- 场景 6：更高版本发布取代已完成发布 ----------
	section("场景6 更高版本取代旧发布：旧发布成功设备回滚，状态一致")
	if _, err := gw.Firmware.CreateRelease("rel-3", "2.1.0", 10, 0, []string{"dev-4", "dev-5"}); err != nil {
		log.Fatal(err)
	}
	if _, err := gw.Firmware.Publish("rel-3", 50); err != nil {
		log.Fatal(err)
	}
	logger.Printf("[固件] rel-3 v2.1.0 完成后 dev-4 生效版本=%q", gw.Firmware.InstalledVersion("dev-4"))
	if _, err := gw.Firmware.CreateRelease("rel-4", "3.0.0", 10, 0, []string{"dev-4", "dev-5"}); err != nil {
		log.Fatal(err)
	}
	if _, err := gw.Firmware.Publish("rel-4", 50); err != nil {
		// rel-3 已完成、rel-4 也已完成；取代针对的是 rel-3。
		logger.Printf("[固件] rel-4 publish err=%v", err)
	}
	if err := gw.Firmware.Supersede("rel-3", "rel-4"); err != nil {
		log.Fatalf("supersede: %v", err)
	}
	r3, _ := gw.Firmware.Get("rel-3")
	r4, _ := gw.Firmware.Get("rel-4")
	logger.Printf("[固件] 取代后 rel-3=%s(%s) rel-4=%s dev-4生效版本=%q (期望3.0.0保留)",
		r3.Status, r3.FailReason, r4.Status, gw.Firmware.InstalledVersion("dev-4"))
	for _, r := range gw.Firmware.Installs("rel-3") {
		logger.Printf("[固件] rel-3 设备=%s 状态=%s 说明=%s", r.DeviceID, r.Status, r.Detail)
	}
	_ = firmware.ReleaseComplete
	logger.Printf("[完成] 全部发布数=%d", gw.Firmware.ReleaseCount())
}
