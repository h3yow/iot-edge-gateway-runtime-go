package firmware

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 场景 G1：多个发布单同时进行，真正在安装的设备总数受全局上限约束，
// 即使每个发布单各自声明了很大的单发布并发。
func TestGovernanceGlobalConcurrencyAcrossParallelReleases(t *testing.T) {
	var active, maxSeen atomic.Int32
	inst := func(ctx context.Context, dev, from, to string) error {
		cur := active.Add(1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		return nil
	}
	const globalCap = 3
	m := NewManager(inst, WithGlobalMaxConcurrency(globalCap),
		WithLogger(func(f string, a ...any) { t.Logf("[gov] "+f, a...) }))

	// 两个相互独立的发布单（设备集合不相交），各自声明单发布并发=10，
	// 叠加后的安装峰值仍必须被全局上限压住。
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, v := range []string{"v10", "v11"} {
		v := v
		devs := []string{v + "-a", v + "-b", v + "-c", v + "-d", v + "-e"}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rel, err := m.Publish(context.Background(), Config{
				Version:        v,
				MaxConcurrency: 10, // 故意填大，验证全局闸门才是硬上限
				Batches:        []Batch{{Name: "all", DeviceIDs: devs}},
			})
			if err != nil || rel.Status != StatusPublished {
				t.Errorf("发布 %s 预期成功，实际 status=%s err=%v", v, rel.Status, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	peak := maxSeen.Load()
	t.Logf("判定输入 并行发布=2 各自并发=10 全局上限=%d => 观测峰值并发=%d", globalCap, peak)
	if peak > globalCap {
		t.Fatalf("全局安装峰值 %d 超过全局上限 %d", peak, globalCap)
	}
	if got := m.GlobalInflight(); got != 0 {
		t.Fatalf("全部结束后全局在装数应为 0，实际 %d", got)
	}
}

// 场景 G2：并发参数未填/0/负数 -> 兜底为安全值，发布仍可完成且不 panic。
func TestGovernanceConcurrencyFallback(t *testing.T) {
	inst := func(ctx context.Context, dev, from, to string) error { return nil }

	t.Run("Manager全局容量异常兜底为默认值", func(t *testing.T) {
		for _, n := range []int{0, -5} {
			m := NewManager(inst, WithGlobalMaxConcurrency(n))
			if cap(m.globalSem) != DefaultGlobalMaxConcurrency {
				t.Fatalf("全局容量输入=%d 应兜底=%d，实际=%d", n, DefaultGlobalMaxConcurrency, cap(m.globalSem))
			}
			t.Logf("判定输入 WithGlobalMaxConcurrency(%d) => 实际全局容量=%d（默认安全值）", n, cap(m.globalSem))
		}
	})

	t.Run("发布单并发异常兜底且正常完成", func(t *testing.T) {
		for _, n := range []int{0, -3} {
			m := NewManager(inst)
			ver := fmt.Sprintf("v-cfg-%d", n)
			rel, err := m.Publish(context.Background(), Config{
				Version: ver, MaxConcurrency: n, FailureThreshold: n,
				Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1", "d2"}}},
			})
			t.Logf("判定输入 version=%s MaxConcurrency=%d FailureThreshold=%d => status=%s err=%v",
				ver, n, n, rel.Status, err)
			if err != nil || rel.Status != StatusPublished {
				t.Fatalf("异常参数不应卡住发布：status=%s err=%v", rel.Status, err)
			}
		}
	})
}

// 场景 G3：设备当前已是目标版本 -> 按“已就绪、无需安装”处理，
// 发布单 published 且设备终态为 ready/installed 对得上，不挂 pending。
func TestGovernanceDeviceAlreadyReady(t *testing.T) {
	var installCalls atomic.Int32
	inst := func(ctx context.Context, dev, from, to string) error {
		installCalls.Add(1)
		return nil
	}
	m := NewManager(inst,
		WithLogger(func(f string, a ...any) { t.Logf("[gov] "+f, a...) }))

	// 三台设备：d1 已是 v6（就绪），d2/d3 在 v5（需要安装）。
	m.SetCurrentVersion("d1", "v6")
	m.SetCurrentVersion("d2", "v5")
	m.SetCurrentVersion("d3", "v5")

	cfg := Config{
		Version: "v6", MaxConcurrency: 2, FailureThreshold: 1,
		Batches: []Batch{
			{Name: "canary", Canary: true, DeviceIDs: []string{"d1", "d2"}},
			{Name: "rest", DeviceIDs: []string{"d3"}},
		},
	}
	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("判定输入 version=v6 设备当前版本 d1=v6(就绪) d2=v5 d3=v5 => status=%s err=%v reason=%s 实际安装调用=%d",
		rel.Status, err, rel.Reason, installCalls.Load())
	if err != nil || rel.Status != StatusPublished {
		t.Fatalf("预期 published，实际 status=%s err=%v", rel.Status, err)
	}
	if installCalls.Load() != 2 {
		t.Fatalf("只有 d2/d3 需要安装，预期调用 2 次，实际 %d", installCalls.Load())
	}

	// 发布单已完成：每台设备状态必须与实际版本对得上，不得有设备挂 pending。
	for _, dev := range []string{"d1", "d2", "d3"} {
		st, cur, ok := m.DeviceState("v6", dev)
		if !ok {
			t.Fatalf("设备 %s 无发布记录", dev)
		}
		if cur != "v6" {
			t.Fatalf("设备 %s 当前版本应为 v6，实际 %s", dev, cur)
		}
		if st == DevPending || st == DevInstalling {
			t.Fatalf("发布单已完成但设备 %s 仍挂在 %s，状态不一致", dev, st)
		}
		t.Logf("终态对齐 device=%s state=%s current=%s", dev, st, cur)
	}
	if st, _, _ := m.DeviceState("v6", "d1"); st != DevReady {
		t.Fatalf("d1 应为 ready（无需安装），实际 %s", st)
	}
}

// 场景 G3b：滚动期间对已就绪设备 Offer，返回 ready 且不产生安装调用。
func TestGovernanceOfferReadyDuringRolling(t *testing.T) {
	release := make(chan struct{})
	var installCalls atomic.Int32
	inst := func(ctx context.Context, dev, from, to string) error {
		installCalls.Add(1)
		if dev == "block" {
			<-release // 卡住发布，保证 Offer 发生在 rolling 期间
		}
		return nil
	}
	m := NewManager(inst)
	m.SetCurrentVersion("r1", "v7") // r1 已是目标版本

	var rel *Release
	var pubErr error
	done := make(chan struct{})
	go func() {
		rel, pubErr = m.Publish(context.Background(), Config{
			Version: "v7", MaxConcurrency: 1,
			Batches: []Batch{{Name: "all", DeviceIDs: []string{"block", "r1"}}},
		})
		close(done)
	}()

	// 等待发布进入 rolling（block 设备占住唯一名额）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, ok := m.StatusOf("v7")
		if ok && st == StatusRolling {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	got, err := m.Offer("v7", "r1")
	t.Logf("滚动期间对已就绪设备 Offer 输入 device=r1 current=v7 target=v7 => state=%s err=%v 安装调用=%d",
		got, err, installCalls.Load())
	if err != nil || got != DevReady {
		t.Fatalf("预期 ready,nil，实际 %s,%v", got, err)
	}
	close(release)
	<-done
	if pubErr != nil || rel.Status != StatusPublished {
		t.Fatalf("发布应成功：status=%s err=%v", rel.Status, pubErr)
	}
}

// 场景 G4：设备已成功发布高版本后，低版本发布整单被拒，设备保持高版本。
func TestGovernanceLowerVersionRejected(t *testing.T) {
	f := newFake()
	m := NewManager(f.install,
		WithLogger(func(fm string, a ...any) { t.Logf("[gov] "+fm, a...) }))

	// 先发 v8 成功，设备 d1/d2 全部到 v8。
	ok, err := m.Publish(context.Background(), Config{
		Version: "v8", MaxConcurrency: 2,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1", "d2"}}},
	})
	if err != nil || ok.Status != StatusPublished {
		t.Fatalf("前置 v8 发布应成功: %+v %v", ok, err)
	}

	// 再尝试下发 v7（低版本）覆盖同一批设备 -> 必须整单拒绝。
	low, err := m.Publish(context.Background(), Config{
		Version: "v7", MaxConcurrency: 2,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"d1", "d2"}}},
	})
	t.Logf("判定输入 target=v7 设备当前=v8 => err=%v（预期包裹 ErrLowerVersionRejected）", err)
	if !errors.Is(err, ErrLowerVersionRejected) {
		t.Fatalf("低版本发布必须返回 ErrLowerVersionRejected，实际 %v", err)
	}
	if low != nil {
		t.Fatalf("被拒发布不应产生 Release 记录，实际 %+v", low)
	}
	for _, dev := range []string{"d1", "d2"} {
		if cur := m.CurrentVersion(dev); cur != "v8" {
			t.Fatalf("设备 %s 必须保持高版本 v8，实际 %s", dev, cur)
		}
		if n := f.callCount(dev + ":v7"); n != 0 {
			t.Fatalf("设备 %s 不应发生任何 v7 安装，实际 %d 次", dev, n)
		}
	}
	t.Logf("裁决后设备版本 d1=%s d2=%s，均无降级安装", m.CurrentVersion("d1"), m.CurrentVersion("d2"))

	// Supersede 也不允许低版本“接管”高版本。
	if err := m.Supersede("v8", "v7"); !errors.Is(err, ErrInvalidVersionOrder) {
		t.Fatalf("低版本接管高版本必须返回 ErrInvalidVersionOrder，实际 %v", err)
	}
	t.Log("Supersede(v8 <- v7) 被版本顺序裁决拒绝")
}

// 场景 G5：高版本接管进行中的低版本发布；被接管发布随后回滚，
// 已升到高版本的设备必须保持在最新已发布版本，不被拽回。
func TestGovernanceSupersededRollbackKeepsHigherVersion(t *testing.T) {
	f := newFake()
	f.delay = 15 * time.Millisecond
	m := NewManager(f.install,
		WithLogger(func(fm string, a ...any) { t.Logf("[gov] "+fm, a...) }))
	m.SetCurrentVersion("p1", "v1")
	m.SetCurrentVersion("p2", "v1")
	f.versions["p1"], f.versions["p2"] = "v1", "v1"

	// v2 进行中（慢安装），中途 v3 发布接管同一批设备。
	doneLow := make(chan struct{})
	var lowRel *Release
	go func() {
		lowRel, _ = m.Publish(context.Background(), Config{
			Version: "v2", MaxConcurrency: 1, FailureThreshold: 10,
			Batches: []Batch{{Name: "all", DeviceIDs: []string{"p1", "p2"}}},
		})
		close(doneLow)
	}()

	// 等 v2 进入 rolling 后发起 v3（更高版本），触发自动接管。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, ok := m.StatusOf("v2")
		if ok && st == StatusRolling {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("v2 未进入 rolling")
		}
		time.Sleep(time.Millisecond)
	}

	hiRel, hiErr := m.Publish(context.Background(), Config{
		Version: "v3", MaxConcurrency: 2,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"p1", "p2"}}},
	})
	<-doneLow

	t.Logf("接管判定 low=v2 hi=v3 => 低发布终态=%s reason=%s；高发布 status=%s err=%v",
		lowRel.Status, lowRel.Reason, hiRel.Status, hiErr)
	if lowRel.Status != StatusSuperseded {
		t.Fatalf("v2 应被接管为 superseded，实际 %s", lowRel.Status)
	}
	if hiErr != nil || hiRel.Status != StatusPublished {
		t.Fatalf("v3 应发布成功，实际 status=%s err=%v", hiRel.Status, hiErr)
	}

	// 被接管之后再尝试 Cancel / 回滚旧发布：终态拒绝；设备版本纹丝不动。
	if err := m.Cancel("v2"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("接管后 Cancel 旧单应 ErrAlreadyTerminal，实际 %v", err)
	}
	for _, dev := range []string{"p1", "p2"} {
		if cur := m.CurrentVersion(dev); cur != "v3" {
			t.Fatalf("设备 %s 必须保持在最新已发布版本 v3，实际 %s", dev, cur)
		}
		st, _, _ := m.DeviceState("v2", dev)
		t.Logf("旧发布设备视图 device=%s v2-state=%s current=%s（kept 表示回滚被裁决拦住）", dev, st, m.CurrentVersion(dev))
	}
	// 不允许出现任何 v3 -> v1/v2 的降级安装轨迹。
	for _, c := range f.snapshotCalls() {
		if c == "p1:v3->v1" || c == "p2:v3->v1" || c == "p1:v3->v2" || c == "p2:v3->v2" {
			t.Fatalf("出现降级安装调用: %s", c)
		}
	}
}
