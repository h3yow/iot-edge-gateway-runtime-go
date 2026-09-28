package gateway

import (
	"fmt"
	"sync"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
)

// SimInstaller 是进程内模拟的设备侧安装执行器。
//   - 可按设备注入失败（Fail/Pass），复现发布中途失败回滚；
//   - 记录每台设备真实安装尝试次数，供“重复版本不重复安装”断言；
//   - 安装指令同时经设备会话下行：在线立即送达，离线暂存重连补投。
type SimInstaller struct {
	gw *Gateway

	mu      sync.Mutex
	failing map[string]bool
	attempt map[string]int
}

// NewSimInstaller 创建模拟安装器。
func NewSimInstaller(gw *Gateway) *SimInstaller {
	return &SimInstaller{gw: gw, failing: make(map[string]bool), attempt: make(map[string]int)}
}

// Fail / Pass 控制设备安装结果。
func (i *SimInstaller) Fail(deviceID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.failing[deviceID] = true
}

func (i *SimInstaller) Pass(deviceID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.failing, deviceID)
}

// Attempts 返回真实安装调用次数。
func (i *SimInstaller) Attempts(deviceID string) int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.attempt[deviceID]
}

// Install 实现 firmware.Installer。
func (i *SimInstaller) Install(deviceID, releaseID, version string) (string, string) {
	i.mu.Lock()
	i.attempt[deviceID]++
	fail := i.failing[deviceID]
	i.mu.Unlock()

	// 下行安装指令（离线设备会暂存，重连补投；固件管理器侧已保证幂等）。
	if sess := i.gw.Devices.Get(deviceID); sess != nil {
		sess.Send(fmt.Sprintf("devices/%s/cmd/install", deviceID),
			[]byte(fmt.Sprintf(`{"release":%q,"version":%q}`, releaseID, version)))
	}
	if fail {
		return firmware.InstallFailed, fmt.Sprintf("simulated install failure on %s for %s", deviceID, version)
	}
	return firmware.InstallSuccess, fmt.Sprintf("installed %s on %s", version, deviceID)
}
