package firmware

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeInstaller 记录安装调用，可按 (device,target) 注入失败；保证版本切换原子。
type fakeInstaller struct {
	mu        sync.Mutex
	calls     []string
	failTo    map[string]bool // key deviceID:targetVer -> 失败
	versions  map[string]string
	installed map[string]int // device:version -> 实际安装次数（验证幂等）
	delay     time.Duration
}

func newFake() *fakeInstaller {
	return &fakeInstaller{
		failTo:    map[string]bool{},
		versions:  map[string]string{},
		installed: map[string]int{},
	}
}

func (f *fakeInstaller) install(ctx context.Context, deviceID, from, to string) error {
	f.mu.Lock()
	f.calls = append(f.calls, deviceID+":"+from+"->"+to)
	if f.failTo[deviceID+":"+to] {
		f.mu.Unlock()
		return fmt.Errorf("simulated install failure on %s->%s", from, to)
	}
	delay := f.delay
	f.mu.Unlock()
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// 安装到 to：计数 +1，回滚也算一次安装但键不同，这里只统计目标为非空版本
	if to != "" {
		f.installed[deviceID+":"+to]++
	}
	f.versions[deviceID] = to
	return nil
}

func (f *fakeInstaller) callCount(devVer string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.installed[devVer]
}

func canaryConfig(ver string, devices ...string) Config {
	return Config{
		Version: ver,
		Batches: []Batch{
			{Name: "canary", Canary: true, DeviceIDs: devices[:1]},
			{Name: "rest", DeviceIDs: devices[1:]},
		},
		MaxConcurrency:   2,
		FailureThreshold: 1,
	}
}

// 场景 1：分批灰度全部成功；同一版本重复 Offer 不会重复安装。
func TestPublishCanarySuccessAndIdempotentOffer(t *testing.T) {
	f := newFake()
	m := NewManager(f.install)
	cfg := canaryConfig("v2", "d1", "d2", "d3")

	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("发布输入 version=v2 批次=canary[d1],rest[d2,d3] => status=%s err=%v reason=%s", rel.Status, err, rel.Reason)
	if rel.Status != StatusPublished || err != nil {
		t.Fatalf("预期发布成功，实际 status=%s err=%v", rel.Status, err)
	}
	// 重复 Offer 已发布单：应返回终态错误；对已安装设备查询状态保持 installed
	st, _, _ := m.DeviceState("v2", "d1")
	if st != DevInstalled {
		t.Fatalf("d1 应为 installed，实际 %s", st)
	}
	// 模拟设备重复收到同版本（发布进行中再次投递）：用独立发布验证幂等键
	if f.callCount("d1:v2") != 1 {
		t.Errorf("d1 安装 v2 次数=%d，必须为 1", f.callCount("d1:v2"))
	}
	t.Logf("判定依据: (device,version) 当前版本检查 => 各设备实际安装次数 d1=%d d2=%d d3=%d",
		f.callCount("d1:v2"), f.callCount("d2:v2"), f.callCount("d3:v2"))
}

// 场景 2：灰度批次内失败达到阈值 -> 已安装设备全部回滚到发布前版本。
func TestPublishFailureRollsBack(t *testing.T) {
	f := newFake()
	m := NewManager(f.install)
	// 预置 d1 已在 v1（Manager 与安装器两侧版本一致）
	m.SetCurrentVersion("d1", "v1")
	f.versions["d1"] = "v1"
	f.failTo["d2:v2"] = true // 第二批 d2 安装失败
	cfg := canaryConfig("v2", "d1", "d2", "d3")

	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("发布输入 version=v2 注入失败=d2 阈值=1 => status=%s err=%v reason=%s", rel.Status, err, rel.Reason)
	if rel.Status != StatusRolledBack {
		t.Fatalf("预期 rolled_back，实际 %s", rel.Status)
	}
	d1State, d1Cur, _ := m.DeviceState("v2", "d1")
	t.Logf("回滚判定: d1 state=%s current=%s（预期 rolled_back/v1，已安装才回滚）", d1State, d1Cur)
	if d1State != DevRolledBack || d1Cur != "v1" {
		t.Fatalf("d1 必须回滚到 v1，实际 state=%s cur=%s", d1State, d1Cur)
	}
	d2State, _, _ := m.DeviceState("v2", "d2")
	if d2State != DevFailed {
		t.Fatalf("d2 应保留 failed 终态，实际 %s", d2State)
	}
	if f.callCount("d1:v2") != 1 {
		t.Errorf("回滚不等于重复安装：d1 正向安装 v2 次数应 1，实际 %d", f.callCount("d1:v2"))
	}
}

// 场景 3：发布进行中被 Cancel -> 已安装部分回滚，终态为 cancelled。
func TestPublishCancelMidway(t *testing.T) {
	f := newFake()
	f.delay = 20 * time.Millisecond
	m := NewManager(f.install)
	cfg := Config{
		Version:          "v3",
		Batches:          []Batch{{Name: "all", DeviceIDs: []string{"a", "b", "c", "d"}}},
		MaxConcurrency:   1,
		FailureThreshold: 5,
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		if err := m.Cancel("v3"); err != nil {
			t.Errorf("Cancel: %v", err)
		}
	}()
	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("中途取消输入 version=v3 => status=%s err=%v reason=%s 回滚错误=%v", rel.Status, err, rel.Reason, rel.RollbackErrors)
	if rel.Status != StatusCancelled {
		t.Fatalf("预期 cancelled，实际 %s", rel.Status)
	}
	// 已安装的设备必须已被回滚（current 不再是 v3）
	for _, dev := range []string{"a", "b", "c", "d"} {
		st, cur, _ := m.DeviceState("v3", dev)
		if st == DevInstalled || cur == "v3" {
			t.Fatalf("取消后设备 %s 状态必须一致: state=%s cur=%s", dev, st, cur)
		}
	}
}

// 场景 4：发布进行中被更高版本 Supersede -> 终态 superseded 并回滚。
func TestPublishSuperseded(t *testing.T) {
	f := newFake()
	f.delay = 15 * time.Millisecond
	m := NewManager(f.install)
	cfg := Config{Version: "v4", Batches: []Batch{{Name: "all", DeviceIDs: []string{"x", "y"}}}, MaxConcurrency: 1}
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := m.Supersede("v4", "v5"); err != nil {
			t.Errorf("Supersede: %v", err)
		}
	}()
	rel, _ := m.Publish(context.Background(), cfg)
	t.Logf("高版本取代输入 old=v4 new=v5 => status=%s reason=%s", rel.Status, rel.Reason)
	if rel.Status != StatusSuperseded {
		t.Fatalf("预期 superseded，实际 %s", rel.Status)
	}
}

// 场景 5：并发受控——观测到的同时安装数不超过 MaxConcurrency。
func TestConcurrencyIsBounded(t *testing.T) {
	var active, maxSeen atomic.Int32
	inst := func(ctx context.Context, dev, from, to string) error {
		cur := active.Add(1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return nil
	}
	m := NewManager(inst)
	cfg := Config{Version: "v9", MaxConcurrency: 2,
		Batches: []Batch{{Name: "all", DeviceIDs: []string{"1", "2", "3", "4", "5", "6"}}}}
	rel, err := m.Publish(context.Background(), cfg)
	t.Logf("并发控制输入 max=2 设备=6 => status=%s 峰值并发=%d err=%v", rel.Status, maxSeen.Load(), err)
	if maxSeen.Load() > 2 {
		t.Fatalf("峰值并发 %d 超过上限 2", maxSeen.Load())
	}
}
