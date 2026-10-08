// 并行发布治理：全局在装容量门与单设备安装串行化。
package firmware

import "context"

// slotToken 表示占用了一个全局在装名额，release 归还。
type slotToken struct {
	ch   chan struct{}
	done bool
}

// acquireSlot 申请一个全局安装名额（安装与回滚共用同一个闸）。
// ctx 取消时放弃排队，返回 (nil,false)；拿到名额返回 (token,true)。
func (m *Manager) acquireSlot(ctx context.Context) (*slotToken, bool) {
	select {
	case m.globalSem <- struct{}{}:
		return &slotToken{ch: m.globalSem}, true
	case <-ctx.Done():
		return nil, false
	}
}

// release 归还全局名额，重复调用安全。
func (t *slotToken) release() {
	if t == nil || t.done {
		return
	}
	t.done = true
	<-t.ch
}
