package firmware

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingInstaller 观测全局同时在装设备数，并为设备维护当前版本。
type countingInstaller struct {
	active   atomic.Int32
	maxSeen  atomic.Int32
	mu       chan struct{} // 仅用于序列化内部 map（信号量已限并发，这里用简单互斥）
	versions map[string]string
	delay    time.Duration
}

func newCounting(delay time.Duration) *countingInstaller {
	return &countingInstaller{
		mu:       make(chan struct{}, 1),
		versions: map[string]string{},
		delay:    delay,
	}
}

func (c *countingInstaller) lock()   { c.mu <- struct{}{} }
func (c *countingInstaller) unlock() { <-c.mu }

func (c *countingInstaller) install(ctx context.Context, dev, from, to string) error {
	cur := c.active.Add(1)
	for {
		old := c.maxSeen.Load()
		if cur <= old || c.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		c.active.Add(-1)
		return ctx.Err()
	}
	c.active.Add(-1)
	c.lock()
	c.versions[dev] = to
	c.unlock()
	return nil
}

// 场景 A：多个发布单同时进行，真正在安装的设备总数不超过全局上限，
// 即使每个发布单各自填了很大的单发布并发也不行。
func TestParallelReleasesRespectGlobalCap(t *testing.T) {
	const globalCap = 3
	inst := newCounting(8 * time.Millisecond)
	var (
		logMu sync.Mutex
		logs  []string
	)
	m := NewManager(inst.install,
		WithGlobalConcurrent(globalCap),
		WithLogger(func(format string, args ...any) {
			logMu.Lock()
			logs = append(logs, fmt.Sprintf(format, args...))
			logMu.Unlock()
		}),
	)

	// 三个发布单并行，各 6 台互不重叠的设备，单发布并发都给很大值。
	mkCfg := func(ver string, off int) Config {
		devs := make([]string, 6)
		for i := range devs {
			devs[i] = fmt.Sprintf("%s-d%d", ver, off+i)
		}
		return Config{
			Version:        ver,
			MaxConcurrency: 100, // 故意超过全局上限
			Batches:        []Batch{{Name: "all", DeviceIDs: devs}},
		}
	}

	errCh := make(chan error, 3)
	go func() {
		_, e := m.Publish(context.Background(), mkCfg("1.0.0", 0))
		errCh <- e
	}()
	go func() {
		_, e := m.Publish(context.Background(), mkCfg("2.0.0", 100))
		errCh <- e
	}()
	go func() {
		_, e := m.Publish(context.Background(), mkCfg("3.0.0", 200))
		errCh <- e
	}()
	for i := 0; i < 3; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("并行发布不应失败: %v", e)
		}
	}

	peak := inst.maxSeen.Load()
	t.Logf("关键输入: 并行发布=3 每单设备=6 单发布并发=100 全局上限=%d => 观测在装峰值=%d", globalCap, peak)
	if peak > globalCap {
		t.Fatalf("全局在装峰值 %d 超过全局上限 %d（多发布叠加不得顶穿）", peak, globalCap)
	}
	if peak < 2 {
		t.Fatalf("峰值 %d 过小，并行发布没有真正重叠执行", peak)
	}
	for _, ver := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		st, ok := m.StatusOf(ver)
		if !ok || st != StatusPublished {
			t.Fatalf("发布 %s 终态异常: %s ok=%v", ver, st, ok)
		}
	}
	if len(logs) == 0 {
		t.Fatal("应有关键输入/判定日志")
	}
	t.Logf("治理日志样例: %q", logs[0])
}

// 场景 B：并发参数缺失/0/负数/超大值时系统给出安全取值，发布不卡死、可正常完成。
func TestConcurrencyFallbackSafeDefaults(t *testing.T) {
	cases := []struct {
		name  string
		input int
		want  int
	}{
		{"零值回落默认", 0, DefaultMaxConcurrency},
		{"负数回落默认", -3, DefaultMaxConcurrency},
		{"超大值收敛硬上限", HardConcurrencyCap + 1000, HardConcurrencyCap},
		{"正常值原样使用", 2, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeCapacity(c.input, DefaultMaxConcurrency)
			t.Logf("关键输入 MaxConcurrency=%d => 判定生效=%d（预期 %d）", c.input, got, c.want)
			if got != c.want {
				t.Fatalf("normalizeCapacity(%d)=%d，预期 %d", c.input, got, c.want)
			}
		})
	}

	// 端到端：0 与负数绝不能造成零容量信号量死锁，发布必须正常收敛到 published。
	for _, bad := range []int{0, -5} {
		inst := newCounting(2 * time.Millisecond)
		m := NewManager(inst.install, WithGlobalConcurrent(DefaultGlobalConcurrent))
		cfg := Config{
			Version:        fmt.Sprintf("bad-%d", bad),
			MaxConcurrency: bad,
			Batches:        []Batch{{Name: "all", DeviceIDs: []string{"a", "b", "c", "d", "e"}}},
		}
		done := make(chan struct{})
		var rel *Release
		go func() {
			rel, _ = m.Publish(context.Background(), cfg)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("MaxConcurrency=%d 导致发布卡死（零容量信号量未兜底）", bad)
		}
		t.Logf("端到端兜底输入 MaxConcurrency=%d 设备=5 => status=%s 设备统计=%v", bad, rel.Status, rel.DeviceCounts)
		if rel.Status != StatusPublished {
			t.Fatalf("异常参数发布应成功，实际 %s", rel.Status)
		}
	}

	// 全局参数为 0/负数时同样回落默认。
	for _, bad := range []int{0, -1} {
		inst := newCounting(1 * time.Millisecond)
		m := NewManager(inst.install, WithGlobalConcurrent(bad))
		if m.globalCap != DefaultGlobalConcurrent {
			t.Fatalf("全局并发=%d 应回落 %d，实际 %d", bad, DefaultGlobalConcurrent, m.globalCap)
		}
		t.Logf("全局参数兜底输入 GlobalConcurrent=%d => 生效=%d", bad, m.globalCap)
	}
}

// 场景 C：发布时设备当前已经是目标版本 -> 按“已就绪、无需安装”处理；
// 发布单报完成时所有设备都处于终态，不能有设备还挂在 pending。
func TestDeviceAlreadyReadyNoInstall(t *testing.T) {
	f := newFake()
	m := NewManager(f.install)

	// d2 已经是 v2.0（Manager 与安装器两侧一致），d1/d3 还是 v1。
	m.SetCurrentVersion("d1", "1.0")
	m.SetCurrentVersion("d2", "2.0")
	m.SetCurrentVersion("d3", "1.0")
	f.versions["d1"] = "1.0"
	f.versions["d2"] = "2.0"
	f.versions["d3"] = "1.0"

	cfg := Config{
		Version:        "2.0",
		MaxConcurrency: 2,
		Batches: []Batch{
			{Name: "canary", Canary: true, DeviceIDs: []string{"d1", "d2"}},
			{Name: "rest", DeviceIDs: []string{"d3"}},
		},
	}
	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("关键输入 version=2.0 d2 已在 2.0，d1/d3 在 1.0 => status=%s err=%v 设备统计=%v",
		rel.Status, err, rel.DeviceCounts)
	if rel.Status != StatusPublished || err != nil {
		t.Fatalf("应发布成功，实际 status=%s err=%v", rel.Status, err)
	}

	st2, cur2, _ := m.DeviceState("2.0", "d2")
	t.Logf("d2 判定: state=%s current=%s（预期 ready/2.0，且不触发实际安装）", st2, cur2)
	if st2 != DevReady {
		t.Fatalf("d2 应为 ready，实际 %s", st2)
	}
	if cur2 != "2.0" {
		t.Fatalf("d2 版本应保持 2.0，实际 %s", cur2)
	}
	if n := f.callCount("d2:2.0"); n != 0 {
		t.Fatalf("已就绪设备不应触发安装，实际安装次数=%d", n)
	}

	// 终态发布单不再接受 Offer（既有契约：返回 ErrAlreadyTerminal），
	// 就绪结论通过 DeviceState 稳定可见。
	if _, err := m.Offer("2.0", "d2"); err == nil {
		t.Fatal("对已终态发布单 Offer 应返回 ErrAlreadyTerminal")
	}

	// 发布单与设备状态对账：不允许任何设备残留 pending/installing。
	for _, d := range []string{"d1", "d2", "d3"} {
		st, _, ok := m.DeviceState("2.0", d)
		if !ok {
			t.Fatalf("设备 %s 不在发布单内", d)
		}
		if st == DevPending || st == DevInstalling {
			t.Fatalf("发布单已 published 但设备 %s 仍停在 %s（状态对不上）", d, st)
		}
	}
	if got := rel.DeviceCounts[DevReady]; got != 1 {
		t.Fatalf("ready 计数应为 1，实际 %d", got)
	}
	if got := rel.DeviceCounts[DevInstalled]; got != 2 {
		t.Fatalf("installed 计数应为 2，实际 %d", got)
	}
	if m.CurrentVersion("d1") != "2.0" || m.CurrentVersion("d3") != "2.0" {
		t.Fatalf("d1/d3 应升级到 2.0，实际 d1=%s d3=%s", m.CurrentVersion("d1"), m.CurrentVersion("d3"))
	}
}

// 场景 D：低版本发布不能顶掉设备上已发布成功的高版本。
// 被拒设备不安装、不降级、不按失败触发整单回滚；设备状态与发布单统计可对账。
func TestDowngradeReleaseRejected(t *testing.T) {
	f := newFake()
	m := NewManager(f.install)

	// 设备已先成功发布到 3.0。
	m.SetCurrentVersion("d1", "1.0")
	m.SetCurrentVersion("d2", "1.0")
	f.versions["d1"] = "1.0"
	f.versions["d2"] = "1.0"
	high := Config{Version: "3.0", MaxConcurrency: 2,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1", "d2"}}}}
	relHigh, err := m.Publish(context.Background(), high)
	if err != nil || relHigh.Status != StatusPublished {
		t.Fatalf("高版本 3.0 应先发布成功: %s %v", relHigh.Status, err)
	}
	t.Logf("前置发布 3.0 => status=%s 设备当前 d1=%s d2=%s",
		relHigh.Status, m.CurrentVersion("d1"), m.CurrentVersion("d2"))

	// 再发一个 2.0（更低）。阈值=1：若被拒被误计为失败就会触发回滚。
	low := Config{
		Version:          "2.0",
		MaxConcurrency:   2,
		FailureThreshold: 1,
		Batches:          []Batch{{Name: "all", DeviceIDs: []string{"d1", "d2"}}},
	}
	relLow, errLow := m.Publish(context.Background(), low)
	t.Logf("关键输入: 在 3.0 设备上下发 2.0 阈值=1 => status=%s err=%v 统计=%v",
		relLow.Status, errLow, relLow.DeviceCounts)

	for _, d := range []string{"d1", "d2"} {
		st, cur, _ := m.DeviceState("2.0", d)
		t.Logf("低版本裁决 device=%s state=%s current=%s（预期 rejected/3.0）", d, st, cur)
		if st != DevRejected {
			t.Fatalf("设备 %s 应 rejected，实际 %s", d, st)
		}
		if cur != "3.0" {
			t.Fatalf("设备 %s 必须保留 3.0，实际被改成 %s", d, cur)
		}
		if n := f.callCount(d + ":2.0"); n != 0 {
			t.Fatalf("设备 %s 不应安装被拒的 2.0，实际次数=%d", d, n)
		}
	}
	// 被拒不算失败：发布单不应因此回滚；设备都在 3.0，发布本身正常收敛。
	if relLow.Status != StatusPublished {
		t.Fatalf("被拒不应触发回滚，发布单终态应仍为 published，实际 %s reason=%s",
			relLow.Status, relLow.Reason)
	}
	if relLow.FailureCount != 0 {
		t.Fatalf("降级拒绝不计入失败数，实际 FailureCount=%d", relLow.FailureCount)
	}
	if got := relLow.DeviceCounts[DevRejected]; got != 2 {
		t.Fatalf("rejected 计数应为 2，实际 %d", got)
	}
	if m.CurrentVersion("d1") != "3.0" || m.CurrentVersion("d2") != "3.0" {
		t.Fatalf("高版本必须保住: d1=%s d2=%s", m.CurrentVersion("d1"), m.CurrentVersion("d2"))
	}
}

// Supersede 的版本先后裁决：只能用更高版本接管，平级/更低直接拒绝。
func TestSupersedeRequiresNewer(t *testing.T) {
	f := newFake()
	f.delay = 10 * time.Millisecond
	m := NewManager(f.install)
	cfg := Config{Version: "3.0", MaxConcurrency: 1,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"x", "y"}}}}
	go func() { _, _ = m.Publish(context.Background(), cfg) }()
	time.Sleep(20 * time.Millisecond)

	if err := m.Supersede("3.0", "2.0"); err == nil {
		t.Fatal("用更低版本 2.0 接管 3.0 必须被拒绝")
	} else {
		t.Logf("低版本接管被正确拒绝: %v", err)
	}
	if err := m.Supersede("3.0", "3.0"); err == nil {
		t.Fatal("用相同版本接管必须被拒绝")
	}
}

// 场景 E：低版本发布在进行中被更高版本接管后，无论随后取消还是回滚，
// 已被接管的设备都必须保持在最新已发布版本（kept），不能被拽回旧版本。
func TestSupersededDeviceKeptOnRollbackAndRetry(t *testing.T) {
	var (
		mu      sync.Mutex
		vers    = map[string]string{}
		calls   []string
		d1OnV1  = make(chan struct{})
		blockD2 = make(chan struct{})
	)
	install := func(ctx context.Context, dev, from, to string) error {
		// v1 第二批的 d2 阻塞住，让 v1 发布稳定停在 rolling 阶段。
		if dev == "d2" && to == "1.0" {
			<-blockD2
		}
		mu.Lock()
		calls = append(calls, dev+":"+from+"->"+to)
		vers[dev] = to
		mu.Unlock()
		if dev == "d1" && to == "1.0" {
			close(d1OnV1)
		}
		return nil
	}
	m := NewManager(install, WithGlobalConcurrent(8))

	v1cfg := Config{
		Version:        "1.0",
		MaxConcurrency: 1,
		Batches: []Batch{
			{Name: "canary", Canary: true, DeviceIDs: []string{"d1"}},
			{Name: "rest", DeviceIDs: []string{"d2", "d3", "d4"}},
		},
	}
	v1Done := make(chan *Release)
	go func() {
		rel, _ := m.Publish(context.Background(), v1cfg)
		v1Done <- rel
	}()

	// 等 d1 已被 v1 装上，此时 v1 正卡在第二批 d2。
	<-d1OnV1
	t.Logf("接管前: d1 已装到 1.0，v1 发布停在 rolling（第二批 d2 被阻塞）")

	// 高版本 2.0 并行接管 d1（与 v1 发布同时进行）。
	v2cfg := Config{Version: "2.0", MaxConcurrency: 1,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1"}}}}
	relV2, err := m.Publish(context.Background(), v2cfg)
	if err != nil || relV2.Status != StatusPublished {
		t.Fatalf("高版本 2.0 接管发布应成功: status=%s err=%v", relV2.Status, err)
	}
	t.Logf("高版本接管: d1 => current=%s（预期 2.0）", m.CurrentVersion("d1"))
	if m.CurrentVersion("d1") != "2.0" {
		t.Fatalf("接管后 d1 应为 2.0，实际 %s", m.CurrentVersion("d1"))
	}

	// 宣告 v1 被 2.0 取代，再放行 v1 第二批，让 v1 走完“停止 -> 回滚”。
	if err := m.Supersede("1.0", "2.0"); err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	close(blockD2)
	relV1 := <-v1Done
	t.Logf("被接管发布终态: status=%s reason=%s 设备统计=%v",
		relV1.Status, relV1.Reason, relV1.DeviceCounts)
	if relV1.Status != StatusSuperseded {
		t.Fatalf("v1 终态应为 superseded，实际 %s", relV1.Status)
	}

	// 核心断言：d1 已被 2.0 接管，回滚必须 kept 且保留 2.0。
	st1, cur1, _ := m.DeviceState("1.0", "d1")
	t.Logf("接管保护裁决 device=d1 state=%s current=%s（预期 kept/2.0，禁止回 1.0）", st1, cur1)
	if st1 != DevKept {
		t.Fatalf("d1 必须 kept，实际 %s", st1)
	}
	if cur1 != "2.0" {
		t.Fatalf("d1 必须保持 2.0，实际被拽成 %s", cur1)
	}

	// 未被接管、确实装了 v1 的 d2 正常回滚；没装上的 d3/d4 为 skipped。
	st2, cur2, _ := m.DeviceState("1.0", "d2")
	t.Logf("普通回滚对照 device=d2 state=%s current=%q（预期 rolled_back/空）", st2, cur2)
	if st2 != DevRolledBack {
		t.Fatalf("d2 应正常回滚 rolled_back，实际 %s", st2)
	}
	for _, d := range []string{"d3", "d4"} {
		st, cur, _ := m.DeviceState("1.0", d)
		// 中止时点不同：未启动的为 skipped，放行后抢装成功的为 rolled_back，
		// 两种都合法；关键是不能残留 installing/pending，也不能留在 1.0。
		if st != DevSkipped && st != DevRolledBack {
			t.Fatalf("未接管设备 %s 应为 skipped/rolled_back，实际 %s", d, st)
		}
		if cur == "1.0" {
			t.Fatalf("设备 %s 不应停留在被回滚版本 1.0，实际 %s", d, cur)
		}
	}

	// 接管后再“重试/取消”都不能降级：
	// 1) 对已终态 v1 再 Cancel -> ErrAlreadyTerminal，d1 不变；
	if err := m.Cancel("1.0"); err == nil {
		t.Fatal("对已终态的被接管发布再次取消应被拒绝")
	}
	// 2) 再发一个仍低于 2.0 的版本到 d1 -> rejected，d1 仍是 2.0。
	relLow, _ := m.Publish(context.Background(), Config{
		Version: "1.5", MaxConcurrency: 1,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1"}}},
	})
	stLow, curLow, _ := m.DeviceState("1.5", "d1")
	t.Logf("接管后重试低版本: 发布1.5 status=%s d1 state=%s current=%s", relLow.Status, stLow, curLow)
	if stLow != DevRejected || curLow != "2.0" {
		t.Fatalf("接管后重试低版本必须 rejected 且保持 2.0，实际 state=%s cur=%s", stLow, curLow)
	}

	// 安装轨迹里不得出现把 d1 从 2.0 装回更旧版本（1.0/空）的降级动作。
	mu.Lock()
	for _, c := range calls {
		if strings.HasPrefix(c, "d1:2.0->") {
			t.Fatalf("发现把 d1 从 2.0 拽回旧版本的安装调用: %s", c)
		}
	}
	mu.Unlock()
}

// 场景 F：同一台设备同时被两个不同版本的发布单下发（设备重叠的并行发布）。
// 设备安装门必须把两次安装串行化并做版本裁决，保证高版本赢、低版本被拒，
// 最终 current 单调向高，且实际安装不会出现“低版本覆盖高版本”。
func TestSameDeviceParallelVersionArbitration(t *testing.T) {
	var (
		mu        sync.Mutex
		vers      = map[string]string{"d1": "1.0"}
		count     = map[string]int{}
		transitUp bool
	)
	install := func(ctx context.Context, dev, from, to string) error {
		// 模拟安装耗时，让两个发布 goroutine 在同设备上充分交错。
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		// 安装器侧也做一次防线：不接受把已更高的版本降下去。
		if CompareVersions(vers[dev], to) > 0 {
			return fmt.Errorf("installer refuses downgrade %s -> %s", vers[dev], to)
		}
		if CompareVersions(vers[dev], to) < 0 {
			transitUp = true
		}
		vers[dev] = to
		count[to]++
		return nil
	}
	m := NewManager(install, WithGlobalConcurrent(8))
	m.SetCurrentVersion("d1", "1.0")

	// 两个不同版本、针对同一设备，从两个 goroutine 同时发起。
	errCh := make(chan error, 2)
	go func() {
		_, e := m.Publish(context.Background(), Config{Version: "2.0", MaxConcurrency: 1,
			Batches: []Batch{{Name: "x", DeviceIDs: []string{"d1"}}}})
		errCh <- e
	}()
	go func() {
		_, e := m.Publish(context.Background(), Config{Version: "1.5", MaxConcurrency: 1,
			Batches: []Batch{{Name: "x", DeviceIDs: []string{"d1"}}}})
		errCh <- e
	}()
	for i := 0; i < 2; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("并行发布不应返回错误: %v", e)
		}
	}

	cur := m.CurrentVersion("d1")
	st2, _, _ := m.DeviceState("2.0", "d1")
	st15, _, _ := m.DeviceState("1.5", "d1")
	t.Logf("同设备并行下发裁决: 2.0 与 1.5 抢 d1（起点 1.0） => current=%s 安装计数=%v 2.0单=%s 1.5单=%s",
		cur, count, st2, st15)

	// 不变量 1：最终版本必须收敛到更高的 2.0（无论中间是 1.0->2.0 还是 1.0->1.5->2.0）。
	if cur != "2.0" {
		t.Fatalf("d1 最终必须是更高的 2.0，实际 %s", cur)
	}
	// 不变量 2：2.0 发布单上 d1 一定 installed。
	if st2 != DevInstalled {
		t.Fatalf("2.0 发布单上 d1 应为 installed，实际 %s", st2)
	}
	// 不变量 3：1.5 若在 2.0 之前执行则合法 installed（1.0->1.5 是升级）；
	// 若在 2.0 之后执行则必须被 rejected。两种顺序都允许，但绝不允许降级安装。
	switch st15 {
	case DevInstalled, DevRejected:
	default:
		t.Fatalf("1.5 发布单上 d1 只可能 installed（先执行）或 rejected（后执行），实际 %s", st15)
	}
	if st15 == DevInstalled && count["1.5"] != 1 {
		t.Fatalf("1.5 显示 installed 但实际安装次数异常: %v", count)
	}
	if st15 == DevRejected && count["1.5"] != 0 {
		t.Fatalf("1.5 已 rejected 却发生了实际安装，计数=%v", count)
	}
	// 不变量 4：至少发生过一次向上迁移，且安装器从未观察到降级（降级会返回 error 导致发布失败）。
	mu.Lock()
	up := transitUp
	mu.Unlock()
	if !up {
		t.Fatal("应至少观察到一次向上版本迁移")
	}
}
