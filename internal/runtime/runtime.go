// Package runtime 把消息接入、设备影子与固件分发串成一条自洽链路。
package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/auth"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/broker"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/firmware"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/shadow"
	"github.com/h3yow/iot-edge-gateway-runtime-go/internal/store"
)

// Config 描述本地模拟运行时的设备与初始固件。
type Config struct {
	DeviceIDs       []string
	ReadOnlyFields  []string
	InitialVersions map[string]string
}

// Runtime 持有网关运行时的全部组件。
type Runtime struct {
	ACL      *auth.ACL
	Store    *store.Store
	Broker   *broker.Broker
	Shadow   *shadow.Shadow
	Firmware *firmware.Manager

	mu         sync.Mutex
	sessions   map[string]*broker.Session
	needsFlush map[string]bool
	logf       func(format string, args ...any)
}

// New 创建运行时并完成设备注册、授权与固件安装器接线。
func New(cfg Config) *Runtime {
	a := auth.NewACL()
	st := store.New()
	sh := shadow.New()
	br := broker.New(a, st)

	for _, f := range cfg.ReadOnlyFields {
		sh.MarkReadOnly(f)
	}

	// 固件安装器：设备“收到” ota/<version>/<deviceID> 主题后实际执行的安装动作。
	fm := firmware.NewManager(func(ctx context.Context, deviceID, fromVer, toVer string) error {
		time.Sleep(5 * time.Millisecond) // 模拟安装耗时
		return nil
	})
	for dev, ver := range cfg.InitialVersions {
		fm.SetCurrentVersion(dev, ver)
	}

	r := &Runtime{
		ACL:        a,
		Store:      st,
		Broker:     br,
		Shadow:     sh,
		Firmware:   fm,
		sessions:   make(map[string]*broker.Session),
		needsFlush: make(map[string]bool),
		logf:       func(string, ...any) {},
	}

	// 每个设备：注册授权 -> 连接 -> 订阅命令与 OTA 主题。
	for _, dev := range cfg.DeviceIDs {
		if err := a.Grant(dev,
			[]string{"devices/" + dev + "/up", "devices/" + dev + "/state"},
			[]string{"devices/" + dev + "/cmd/#", "ota/+/" + dev}); err != nil {
			panic(err)
		}
		if err := r.connectLocked(dev); err != nil {
			panic(err)
		}
	}
	return r
}

// handleDesired 处理平台下发的期望状态（仅在线设备实际收到）。
// 运行时内部链路直接调用 Shadow；这里作为主题通道的示例入口，
// 当前由上层显式调用 Desired，主题处理保留为扩展点。
func (r *Runtime) handleDesired(dev string) broker.Handler {
	return func(d broker.Delivery) {
		r.log("device=%s 经主题收到期望命令 msg=%s", dev, d.MessageID)
	}
}

// handleOTA 处理 ota/<version>/<deviceID> 安装邀请。
func (r *Runtime) handleOTA(dev string) broker.Handler {
	return func(d broker.Delivery) {
		// 从主题 ota/<version>/<dev> 解析版本
		parts := splitTopic(d.Topic)
		if len(parts) != 3 {
			return
		}
		st, err := r.Firmware.Offer(parts[1], dev)
		r.log("device=%s 经主题 %s 收到 OTA => state=%s err=%v", dev, d.Topic, st, err)
	}
}

func splitTopic(raw string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == '/' {
			out = append(out, raw[start:i])
			start = i + 1
		}
	}
	return out
}

// SetLogger 注入日志函数。
func (r *Runtime) SetLogger(f func(format string, args ...any)) { r.logf = f }

func (r *Runtime) log(format string, args ...any) {
	r.logf(format, args...)
}

// connectLocked 建立连接、更新在线状态并注册命令/OTA 订阅。
func (r *Runtime) connectLocked(dev string) error {
	id := auth.Identity{DeviceID: dev}
	sess, err := r.Broker.Connect(id)
	if err != nil {
		return err
	}
	wasOffline := r.Shadow.MarkOnline(dev)

	// 期望状态命令：{"req":"...","base":n,"patch":{...}}
	if err := r.Broker.Subscribe(id, "devices/"+dev+"/cmd/desired", r.handleDesired(dev)); err != nil {
		return err
	}
	// OTA 下发：设备收到安装邀请，调用 Firmware.Offer，幂等由其保证。
	if err := r.Broker.Subscribe(id, "ota/+/"+dev, r.handleOTA(dev)); err != nil {
		return err
	}

	r.mu.Lock()
	r.sessions[dev] = sess
	r.mu.Unlock()

	if wasOffline {
		// 设备重连后应先补报其最新状态，再合并离线暂存的期望，
		// 否则三方合并缺少“设备本地是否改过”的信息。标记后由首次上报触发。
		r.mu.Lock()
		r.needsFlush[dev] = true
		r.mu.Unlock()
	}
	return nil
}

// Disconnect 模拟设备断线。
func (r *Runtime) Disconnect(dev string) {
	r.mu.Lock()
	sess := r.sessions[dev]
	delete(r.sessions, dev)
	r.mu.Unlock()
	if sess != nil {
		sess.Close()
	}
	r.Shadow.MarkOffline(dev)
	r.log("device=%s 已断线", dev)
}

// Reconnect 模拟设备重连（含订阅恢复与补投合并）。
func (r *Runtime) Reconnect(dev string) error {
	return r.connectLocked(dev)
}

// Report 是设备上报一条消息：先过 broker 鉴权/入库/投递，再更新影子版本。
func (r *Runtime) Report(dev string, seq int64, msgID string, version int64, state map[string]any) broker.PublishReceipt {
	payload := []byte(fmt.Sprintf(`{"ver":%d}`, version))
	rc := r.Broker.Publish(auth.Identity{DeviceID: dev}, "devices/"+dev+"/up", msgID, payload, seq)
	if rc.Accepted {
		adv, stale := r.Shadow.Report(dev, version, state)
		r.log("device=%s 上报 msg=%s ver=%d => stored=%v dup=%v shadowAdvanced=%v stale=%v",
			dev, msgID, version, rc.Stored, rc.Duplicated, adv, stale)
		r.maybeFlushStaged(dev)
	}
	return rc
}

// maybeFlushStaged 在设备重连后的首次上报之后，合并离线期间暂存的期望。
func (r *Runtime) maybeFlushStaged(dev string) {
	r.mu.Lock()
	need := r.needsFlush[dev]
	delete(r.needsFlush, dev)
	r.mu.Unlock()
	if !need {
		return
	}
	results := r.Shadow.FlushOnReconnect(dev)
	for _, res := range results {
		r.log("device=%s 重连补报后合并暂存期望 desiredVersion=%d 冲突数=%d", dev, res.DesiredVersion, len(res.Conflicts))
		for _, c := range res.Conflicts {
			r.log("  冲突裁决 field=%s base=%v desired=%v reported=%v winner=%s reason=%q",
				c.Field, c.BaseValue, c.DesiredVal, c.ReportedVal, c.Winner, c.Reason)
		}
	}
}

// Desired 是平台向设备下发期望状态（设备离线时由影子暂存）。
func (r *Runtime) Desired(req shadow.DesiredRequest) shadow.ApplyResult {
	return r.Shadow.ApplyDesired(req)
}

// PublishFirmware 发起一次分批灰度发布。
func (r *Runtime) PublishFirmware(ctx context.Context, cfg firmware.Config) (*firmware.Release, error) {
	return r.Firmware.Publish(ctx, cfg)
}

// CancelFirmware 取消发布。
func (r *Runtime) CancelFirmware(version string) error { return r.Firmware.Cancel(version) }

// OfferOTA 通过主题向某设备投递安装邀请（模拟命令通道），返回设备侧结论。
func (r *Runtime) OfferOTA(version, dev string) (firmware.DeviceStatus, error) {
	st, err := r.Firmware.Offer(version, dev)
	r.log("OTA 投递 version=%s device=%s => state=%s err=%v", version, dev, st, err)
	return st, err
}

// ShadowDoc 读取设备影子快照。
func (r *Runtime) ShadowDoc(dev string) shadow.Doc { return r.Shadow.Get(dev) }

// OnlineCount 返回在线设备数。
func (r *Runtime) OnlineCount() int { return r.Broker.OnlineCount() }
