package firmware

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeInstaller 可按设备注入失败、可阻塞，用于精确控制发布时序。
type fakeInstaller struct {
	mu       sync.Mutex
	fail     map[string]bool
	attempts map[string]int
	inFlight int
	maxSeen  int
	block    map[string]chan struct{}
}

func newFakeInstaller() *fakeInstaller {
	return &fakeInstaller{fail: map[string]bool{}, attempts: map[string]int{}, block: map[string]chan struct{}{}}
}

func (f *fakeInstaller) Install(deviceID, releaseID, version string) (string, string) {
	f.mu.Lock()
	f.attempts[deviceID]++
	f.inFlight++
	if f.inFlight > f.maxSeen {
		f.maxSeen = f.inFlight
	}
	block := f.block[deviceID]
	fail := f.fail[deviceID]
	f.mu.Unlock()

	if block != nil {
		<-block // 测试控制释放
	}
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
	if fail {
		return InstallFailed, fmt.Sprintf("forced failure on %s", deviceID)
	}
	return InstallSuccess, "ok"
}

// TestFailureRollback 发布中途失败率超阈值：成功设备回滚、未下发取消、版本恢复。
func TestFailureRollback(t *testing.T) {
	fi := newFakeInstaller()
	m := New(4, fi)
	targets := []string{"d1", "d2", "d3", "d4"}
	if _, err := m.CreateRelease("r1", "1.0.0", 2, 0, targets); err != nil {
		t.Fatal(err)
	}
	fi.fail["d1"] = true // 第一批 2 台中 1 台失败：50% > 阈值 40%

	t.Log("关键输入：r1 v1.0.0 批大小2 失败阈值40%，d1 强制失败；判定依据：波次边界失败率 1/2=50%>40%")
	prog, err := m.Publish("r1", 40)
	if err != nil {
		t.Fatalf("Publish 返回错误: %v", err)
	}
	t.Logf("终态 status=%s reason=%s success=%d failed=%d rolledBack=%d cancelled=%d",
		prog.Status, prog.Reason, prog.Succeeded, prog.Failed, prog.RolledBack, prog.Cancelled)
	if prog.Status != ReleaseFailed || prog.Reason != ReasonFailureRateAbort {
		t.Fatalf("want failed/failure_rate_exceeded, got %s/%s", prog.Status, prog.Reason)
	}
	if prog.RolledBack != 1 || prog.Cancelled != 2 || prog.Failed != 1 {
		t.Fatalf("回滚分布不符: %+v", prog)
	}
	status := map[string]string{}
	for _, r := range m.Installs("r1") {
		status[r.DeviceID] = r.Status
		t.Logf("设备 %s -> %s (%s)", r.DeviceID, r.Status, r.Detail)
	}
	if status["d1"] != InstallFailed || status["d2"] != InstallRolledBack ||
		status["d3"] != InstallCancelled || status["d4"] != InstallCancelled {
		t.Fatalf("设备状态分布错误: %v", status)
	}
	if v := m.InstalledVersion("d2"); v != "" {
		t.Fatalf("回滚后 d2 版本应为空，实际 %q", v)
	}
	// 终态发布不可再次推进
	if _, err := m.Publish("r1", 100); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("终态再发布应报 ErrAlreadyTerminal, got %v", err)
	}
	t.Log("终态发布再次 Publish -> ErrAlreadyTerminal，状态不可被破坏")
}

// TestSameVersionIdempotent 同一设备重复收到同一版本只真实安装一次。
func TestSameVersionIdempotent(t *testing.T) {
	fi := newFakeInstaller()
	m := New(4, fi)
	if _, err := m.CreateRelease("r1", "2.0.0", 10, 0, []string{"d1", "d2"}); err != nil {
		t.Fatal(err)
	}
	p1, _ := m.Publish("r1", 50)
	t.Logf("首次发布 r1 v2.0.0 -> %s/%s success=%d", p1.Status, p1.Reason, p1.Succeeded)

	// 同版本再发一次：全部 skipped，安装器零调用
	if _, err := m.CreateRelease("r2", "2.0.0", 10, 0, []string{"d1", "d2"}); err != nil {
		t.Fatal(err)
	}
	p2, _ := m.Publish("r2", 50)
	t.Logf("重发 r2 v2.0.0 -> status=%s reason=%s skipped=%d；安装器调用 d1=%d d2=%d（首次后不再增长）",
		p2.Status, p2.Reason, p2.Skipped, fi.attempts["d1"], fi.attempts["d2"])
	if p2.Status != ReleaseComplete || p2.Skipped != 2 {
		t.Fatalf("want complete with 2 skipped, got %s %+v", p2.Status, p2)
	}
	if fi.attempts["d1"] != 1 || fi.attempts["d2"] != 1 {
		t.Fatalf("每台设备真实安装应仅1次, got d1=%d d2=%d", fi.attempts["d1"], fi.attempts["d2"])
	}

	// 高版本才允许再次安装
	if _, err := m.CreateRelease("r3", "2.1.0", 10, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish("r3", 50); err != nil {
		t.Fatal(err)
	}
	if fi.attempts["d1"] != 2 {
		t.Fatalf("升级版本应再次安装, attempts=%d want 2", fi.attempts["d1"])
	}
	t.Logf("升级到 2.1.0 后 d1 真实安装次数=%d（期望2），版本=%s", fi.attempts["d1"], m.InstalledVersion("d1"))
}

// TestCancelDuringPublish 发布进行中取消：在途设备完成后回滚，未开始的取消，状态一致。
func TestCancelDuringPublish(t *testing.T) {
	fi := newFakeInstaller()
	m := New(2, fi)
	// d1 阻塞，发布启动后 d1/d2 在途；d3/d4 未开始
	fi.block["d1"] = make(chan struct{})
	fi.block["d2"] = make(chan struct{})
	if _, err := m.CreateRelease("r1", "1.0.0", 2, 0, []string{"d1", "d2", "d3", "d4"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan Progress, 1)
	go func() {
		p, err := m.Publish("r1", 100)
		if err != nil {
			t.Errorf("publish: %v", err)
		}
		done <- p
	}()
	time.Sleep(50 * time.Millisecond) // 等首批安装进入阻塞

	cp, inProgress, err := m.Cancel("r1", "operator abort")
	if err != nil || !inProgress {
		t.Fatalf("cancel: err=%v inProgress=%v", err, inProgress)
	}
	t.Logf("取消信号已设置并立即返回 reason=%s（不等待在途安装，避免等待环）", cp.Reason)

	// 释放在途安装：Publish 波次边界观察信号后回滚收敛
	close(fi.block["d1"])
	close(fi.block["d2"])
	p := <-done
	final, err := m.WaitTerminal("r1", time.Second)
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	_ = cp
	t.Logf("在途完成后发布终态 status=%s reason=%s success=%d rolledBack=%d cancelled=%d waitStatus=%s",
		p.Status, p.Reason, p.Succeeded, p.RolledBack, p.Cancelled, final.Status)
	if p.Status != ReleaseCancelled {
		t.Fatalf("want cancelled, got %s", p.Status)
	}
	if p.RolledBack != 2 || p.Cancelled != 2 {
		t.Fatalf("取消后分布应为2回滚2取消, got %+v", p)
	}
	if v := m.InstalledVersion("d1"); v != "" {
		t.Fatalf("取消后 d1 版本应回滚为空, got %q", v)
	}
}

// TestConcurrentPublishControlled 并发发布受控：maxConcurrent=1 时第二个发布被拒。
func TestConcurrentPublishControlled(t *testing.T) {
	fi := newFakeInstaller()
	m := New(1, fi) // 发布与安装并发均为1
	block := make(chan struct{})
	fi.block["d1"] = block
	if _, err := m.CreateRelease("r1", "1.0.0", 1, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = m.Publish("r1", 100)
	}()
	<-started
	time.Sleep(50 * time.Millisecond)

	if _, err := m.CreateRelease("r2", "2.0.0", 1, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	_, err := m.Publish("r2", 100)
	t.Logf("r1 发布进行中发起 r2 -> err=%v，判定依据：发布信号量容量1", err)
	if err == nil {
		t.Fatal("并发发布应被受控拒绝")
	}
	close(block)
	time.Sleep(50 * time.Millisecond)
}

// TestSupersedeVersionGuard 取代必须是严格更高版本，否则拒绝。
func TestSupersedeVersionGuard(t *testing.T) {
	fi := newFakeInstaller()
	m := New(2, fi)
	if _, err := m.CreateRelease("r1", "2.0.0", 10, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish("r1", 50); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateRelease("r2", "1.9.0", 10, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	err := m.Supersede("r1", "r2")
	t.Logf("低版本 1.9.0 取代 2.0.0 -> err=%v，判定依据：点分版本比较", err)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}

	// 高版本取代：r1 已完成且设备已装 r2 新版本时保留 success
	if _, err := m.CreateRelease("r3", "3.0.0", 10, 0, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish("r3", 50); err != nil {
		t.Fatal(err)
	}
	if err := m.Supersede("r1", "r3"); err != nil {
		t.Fatalf("高版本取代失败: %v", err)
	}
	r1, _ := m.Get("r1")
	rec := m.Installs("r1")[0]
	t.Logf("r1 被 r3 取代后 status=%s 设备d1=%s（设备已在3.0.0，保留成功事实）当前版本=%s",
		r1.Status, rec.Status, m.InstalledVersion("d1"))
	if r1.Status != ReleaseSuperseded || rec.Status != InstallSuccess {
		t.Fatalf("supersede 后状态不一致: %s / %s", r1.Status, rec.Status)
	}
}
