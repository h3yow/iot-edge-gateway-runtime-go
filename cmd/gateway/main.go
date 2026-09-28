// Command gateway 是可在本机直接运行的 IoT 边缘网关模拟入口。
//
// 运行：go run ./cmd/gateway
// 它会在一个进程内模拟：多设备并发上报（含重复/补投去重）、
// 一台设备断线期间下发期望并暂存、重连后按版本合并冲突、
// 固件分批灰度成功与一次失败触发的整体回滚。
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/runtime"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/shadow"
)

func main() {
	logger := log.New(log.Writer(), "[gw] ", log.Ltime|log.Lmsgprefix)
	rt := runtime.New(runtime.Config{
		DeviceIDs:      []string{"dev-A", "dev-B", "dev-C", "dev-D"},
		ReadOnlyFields: []string{"temp"},
		InitialVersions: map[string]string{
			"dev-A": "fw-1.0", "dev-B": "fw-1.0",
			"dev-C": "fw-1.0", "dev-D": "fw-1.0",
		},
	})
	rt.SetLogger(func(format string, args ...any) {
		logger.Printf(format, args...)
	})

	// ---------- 1. 多设备并发上报（含重复 ID，模拟重连补投） ----------
	logger.Println("=== 阶段1：多设备并发上报（重复/补投不重复入库） ===")
	var wg sync.WaitGroup
	for _, dev := range []string{"dev-A", "dev-B", "dev-C"} {
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(dev string, i int) {
				defer wg.Done()
				id := fmt.Sprintf("%s-up-%02d", dev, i%8) // 仅 8 个唯一 ID
				rt.Report(dev, int64(i), id, 1, map[string]any{"mode": "auto", "temp": 20 + i%5})
			}(dev, i)
		}
	}
	wg.Wait()
	for _, dev := range []string{"dev-A", "dev-B", "dev-C"} {
		logger.Printf("入库统计 device=%s count=%d", dev, rt.StoreCount(dev))
	}

	// ---------- 2. dev-A 断线，离线期间下发期望并暂存 ----------
	logger.Println("=== 阶段2：dev-A 断线，平台离线下发期望（暂存） ===")
	rt.Disconnect("dev-A")
	doc := rt.ShadowDoc("dev-A")
	res := rt.Desired(shadow.DesiredRequest{
		RequestID:           "cmd-001",
		DeviceID:            "dev-A",
		BaseReportedVersion: 1,
		BaseReported:        map[string]any{"mode": "auto", "temp": 20},
		Patch:               map[string]any{"mode": "cool", "temp": 18},
		IssuedAt:            time.Now(),
	})
	logger.Printf("离线下发 cmd-001 => staged=%v applied=%v duplicate=%v", res.Staged, res.Applied, res.Duplicate)
	dup := rt.Desired(shadow.DesiredRequest{RequestID: "cmd-001", DeviceID: "dev-A",
		Patch: map[string]any{"mode": "cool"}, IssuedAt: time.Now()})
	logger.Printf("重复下发 cmd-001 => duplicate=%v（幂等）", dup.Duplicate)
	_ = doc

	// ---------- 3. dev-A 本地被手动改设置，重连补报后触发暂存合并 ----------
	logger.Println("=== 阶段3：dev-A 重连并补报本地改动，随后三方合并暂存期望 ===")
	if err := rt.Reconnect("dev-A"); err != nil {
		log.Fatal(err)
	}
	// 设备断线期间被手动调成 heat、温度升到 26，以新版本 v2 补报；
	// 运行时在该次上报后自动合并暂存的 cmd-001 并裁决冲突。
	rt.Report("dev-A", 100, "dev-A-up-reconnect", 2, map[string]any{"mode": "heat", "temp": 26})
	d := rt.ShadowDoc("dev-A")
	logger.Printf("dev-A 影子 desired=%v reported=%v desiredVersion=%d", d.Desired, d.Reported, d.DesiredVersion)

	// ---------- 4. 固件分批灰度：成功 ----------
	logger.Println("=== 阶段4：固件 fw-2.0 分批灰度（canary -> rest），应全部成功 ===")
	rel, err := rt.PublishFirmware(context.Background(), firmware.Config{
		Version:          "fw-2.0",
		MaxConcurrency:   2,
		FailureThreshold: 1,
		Batches: []firmware.Batch{
			{Name: "canary", Canary: true, DeviceIDs: []string{"dev-A"}},
			{Name: "rest", DeviceIDs: []string{"dev-B", "dev-C", "dev-D"}},
		},
	})
	logger.Printf("发布 fw-2.0 => status=%s reason=%s err=%v", rel.Status, rel.Reason, err)

	logger.Println("=== 阶段5：演示非法/越权主题被拒（原因可区分） ===")
	// 通过 broker 直接展示拒绝原因
	rt.DemoRejections()

	logger.Println("网关模拟结束")
}
