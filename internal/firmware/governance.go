package firmware

import (
	"context"
	"errors"
	"sync/atomic"
)

// 发布治理引入的设备级状态：
const (
	// DevReady 设备当前版本已是发布目标版本，无需安装（已就绪）。
	DevReady DeviceStatus = "ready"
	// DevRejected 低版本下发被裁决拒绝（设备已在更高的已发布版本上）。
	DevRejected DeviceStatus = "rejected"
	// DevKept 回滚/接管时设备保持在更新的已发布版本，未被拽回旧版本。
	DevKept DeviceStatus = "kept"
)

// 默认容量参数：Config 或 ManagerOption 未填 / 填 0 / 填负数时采用安全兜底值。
const (
	// DefaultPerReleaseConcurrency 单个发布单未指定并发时的兜底值（保守串行起步）。
	DefaultPerReleaseConcurrency = 1
	// DefaultGlobalMaxConcurrency 网关同时安装设备总数的全局兜底上限。
	DefaultGlobalMaxConcurrency = 8
)

// 治理相关错误。
var (
	// ErrLowerVersionRejected 目标版本不高于设备上已成功发布的版本，整单被拒。
	ErrLowerVersionRejected = errors.New("firmware: lower or equal version rejected by governance")
	// ErrInvalidVersionOrder Supersede 时新版本不高于旧版本。
	ErrInvalidVersionOrder = errors.New("firmware: superseding version must be higher")
)

// ManagerOption 配置 Manager 的发布治理参数。
type ManagerOption func(*managerOptions)

type managerOptions struct {
	globalMaxConcurrency int
	logf                 func(format string, args ...any)
}

// WithGlobalMaxConcurrency 设置全局同时安装设备数上限；<=0 时取默认安全值。
func WithGlobalMaxConcurrency(n int) ManagerOption {
	return func(o *managerOptions) { o.globalMaxConcurrency = n }
}

// WithLogger 注入治理判定日志（关键输入与裁决依据均通过它输出）。
func WithLogger(f func(format string, args ...any)) ManagerOption {
	return func(o *managerOptions) { o.logf = f }
}

// normalizeConcurrency 把不规范（未填/0/负数）的并发参数兜底为安全值。
func normalizeConcurrency(n, fallback int) int {
	if n <= 0 {
		return fallback
	}
	return n
}

// globalSlot 是全局安装容量的计数凭证（骨架：仅类型与字段，下一步接线）。
type globalSlot struct {
	inflight atomic.Int32
}

// acquire 占一个全局安装名额；ctx 取消时放弃排队。
func (m *Manager) acquireGlobalSlot(ctx context.Context) error {
	select {
	case m.globalSem <- struct{}{}:
		m.globalInflight.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) releaseGlobalSlot() {
	<-m.globalSem
	m.globalInflight.Add(-1)
}

// GlobalInflight 返回当前真正处于安装中的设备总数（可观测性/测试用）。
func (m *Manager) GlobalInflight() int {
	return int(m.globalInflight.Load())
}

// reconcileTerminal 是终态一致性兜底：发布单收敛后不允许任何设备仍停留在
// pending/installing。按发布单终态与设备实际版本把记录归一到确定设备状态，
// 保证“发布单已完成、设备却挂等待中”的不一致不会对外可见。
func (m *Manager) reconcileTerminal(info *releaseInfo) {
	info.mu.Lock()
	final := info.status
	info.mu.Unlock()
	for id, rec := range info.devices {
		rec.mu.Lock()
		st := rec.state
		rec.mu.Unlock()
		if st != DevPending && st != DevInstalling {
			continue
		}
		cur := m.CurrentVersion(id)
		switch {
		case final == StatusPublished && cur == rec.targetVer:
			st = DevInstalled
		case cur == rec.targetVer:
			st = DevReady
		case CompareVersions(cur, rec.targetVer) > 0:
			st = DevKept
		default:
			// 中止路径上未及安装的设备统一记 skipped。
			st = DevSkipped
		}
		rec.mu.Lock()
		rec.state = st
		rec.mu.Unlock()
		m.log("终态一致性兜底 device=%s release=%s 悬挂状态=%s => %s（current=%s target=%s）",
			id, info.cfg.Version, DevPending, st, cur, rec.targetVer)
	}
}

func (rec *installRecord) snapshotState() DeviceStatus {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.state
}
