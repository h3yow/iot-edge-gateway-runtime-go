package gateway

import (
	"fmt"
	"sort"
	"sync"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/device"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/mqtt"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/shadow"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/store"
)

// Gateway 把 broker、授权、存储、影子、固件、设备会话串成一条链路。
type Gateway struct {
	Broker   *mqtt.Broker
	Policy   *auth.Policy
	Store    *store.Store
	Shadow   *shadow.Service
	Firmware *firmware.Manager
	Devices  *device.Registry

	installer *SimInstaller

	mu           sync.Mutex
	deliveredLog []mqtt.Message // 内部钩子观察到的已投递消息（去重之后）
}

// Config 是网关构造参数。
type Config struct {
	MaxConcurrentPublishes int
	ConflictPolicy         string // shadow.Conflict* 三选一，空值=reject
}

// New 组装网关并注册内部订阅钩子。
func New(cfg Config) *Gateway {
	policy := auth.NewPolicy()
	st := store.New()
	broker := mqtt.NewBroker(policy, st)
	sh := shadow.NewWithConflictPolicy(cfg.ConflictPolicy)
	g := &Gateway{
		Broker:  broker,
		Policy:  policy,
		Store:   st,
		Shadow:  sh,
		Devices: device.NewRegistry(),
	}
	g.installer = NewSimInstaller(g)
	g.Firmware = firmware.New(maxOr(cfg.MaxConcurrentPublishes, 4), g.installer)

	// 内部钩子：所有设备遥测在“原子去重 + 入库”之后扇出到此处，
	// 只负责记录可观测日志，不做第二次去重判定。
	_ = broker.AddInternalHandler("devices/+/telemetry/#", func(msg mqtt.Message) {
		g.mu.Lock()
		g.deliveredLog = append(g.deliveredLog, msg)
		g.mu.Unlock()
	})
	return g
}

// RegisterDevice 注册设备：影子 + 会话 + 默认授权。
// 默认授权范围（最小权限）：
//   - 发布：devices/<id>/telemetry/#
//   - 订阅：devices/<id>/cmd/#
//
// 其他设备的主题、影子/固件等内部主题均不可见。
func (g *Gateway) RegisterDevice(deviceID string) {
	g.Shadow.Register(deviceID)
	g.Devices.Add(device.NewSession(deviceID))
	_ = g.Policy.Grant(deviceID, auth.ActionPublish, "devices/"+deviceID+"/telemetry/#")
	_ = g.Policy.Grant(deviceID, auth.ActionSubscribe, "devices/"+deviceID+"/cmd/#")
}

// Grant 追加一条授权。
func (g *Gateway) Grant(deviceID string, action auth.Action, filter string) error {
	return g.Policy.Grant(deviceID, action, filter)
}

// Connect 设备上线：会话重放离线暂存指令，随后合并影子暂存期望。
func (g *Gateway) Connect(deviceID string) (shadow.Result, error) {
	sess := g.Devices.Get(deviceID)
	if sess == nil {
		return shadow.Result{}, fmt.Errorf("gateway: unknown device %q", deviceID)
	}
	// 会话先重放下行（固件指令等，由各子系统自行幂等）。
	sess.Connect()
	if err := g.Shadow.SetOnline(deviceID); err != nil {
		return shadow.Result{}, err
	}
	return g.Shadow.MergePending(deviceID)
}

// Disconnect 设备下线。
func (g *Gateway) Disconnect(deviceID string) error {
	sess := g.Devices.Get(deviceID)
	if sess == nil {
		return fmt.Errorf("gateway: unknown device %q", deviceID)
	}
	sess.Disconnect()
	return g.Shadow.SetOffline(deviceID)
}

// IngestTelemetry 处理一条设备上行遥测。
// 链路：主题校验 -> 授权 -> 原子去重入库 -> 通配符扇出。
// 拒绝原因在 PublishResult.Reason 中区分（invalid_topic /
// unauthorized_publish / duplicate / delivered）。
func (g *Gateway) IngestTelemetry(deviceID string, msg mqtt.Message) mqtt.PublishResult {
	return g.Broker.Publish(deviceID, msg)
}

// UpdateDesired 下发期望状态（离线暂存、上线按版本合并、reqID 幂等）。
func (g *Gateway) UpdateDesired(reqID, deviceID string, version int64, fields map[string]string) (shadow.Result, error) {
	return g.Shadow.DesiredUpdate(reqID, deviceID, version, fields)
}

// PublishFirmware 登记并推进一次固件灰度发布。
func (g *Gateway) PublishFirmware(id, version string, batchSize, canaryPct, failRateThreshold int, targets []string) (firmware.Progress, *firmware.Release, error) {
	rel, err := g.Firmware.CreateRelease(id, version, batchSize, canaryPct, targets)
	if err != nil {
		return firmware.Progress{}, nil, err
	}
	prog, err := g.Firmware.Publish(id, failRateThreshold)
	return prog, rel, err
}

// DeliveredCount 返回去重后实际进入投递日志的消息数。
func (g *Gateway) DeliveredCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.deliveredLog)
}

// DeliveredMessages 返回投递日志副本。
func (g *Gateway) DeliveredMessages() []mqtt.Message {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]mqtt.Message, len(g.deliveredLog))
	copy(out, g.deliveredLog)
	return out
}

// FailInstall 让某设备的下次（及以后）安装返回失败，用于测试失败回滚链路。
func (g *Gateway) FailInstall(deviceID string) { g.installer.Fail(deviceID) }

// PassInstall 撤销设备的安装失败设定。
func (g *Gateway) PassInstall(deviceID string) { g.installer.Pass(deviceID) }

// InstallAttempts 返回某设备被实际调用安装器的次数（跳过不计）。
func (g *Gateway) InstallAttempts(deviceID string) int { return g.installer.Attempts(deviceID) }

func maxOr(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

// sortedTargets 供演示输出稳定排序。
func sortedTargets(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
