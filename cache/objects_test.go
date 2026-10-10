package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

type cacheScenarioResult struct {
	name                            string
	bytes                           int64
	gets, ranges                    int
	p50, p99                        time.Duration
	memoryHits, memoryMisses        int
	diskHits, diskMisses, evictions uint64
}

// TestCacheTierScenarios runs the disk tier through cold, warm, partial,
// thrashing and cache-loss request streams. The normal test uses the
// complete deterministic stream without sleeping; set
// OBJSTORE_CACHE_BENCH_LONG=1 to add 20ms of store latency per miss.
func TestCacheTierScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("cache-tier scenario matrix")
	}
	results := runCacheScenarios(t)
	byName := make(map[string]cacheScenarioResult, len(results))
	for _, r := range results {
		byName[r.name] = r
		t.Logf("%s bytes=%d get=%d range=%d p50=%s p99=%s memory=%d/%d disk=%d/%d evictions=%d", r.name, r.bytes, r.gets, r.ranges, r.p50, r.p99, r.memoryHits, r.memoryHits+r.memoryMisses, r.diskHits, r.diskHits+r.diskMisses, r.evictions)
	}
	if !(byName["cold"].bytes >= byName["partial"].bytes && byName["partial"].bytes >= byName["warm"].bytes) {
		t.Fatalf("cold/partial/warm bytes are not monotonic: %d/%d/%d", byName["cold"].bytes, byName["partial"].bytes, byName["warm"].bytes)
	}
	if byName["thrashing"].bytes <= byName["partial"].bytes {
		t.Fatalf("thrashing bytes %d <= partial %d", byName["thrashing"].bytes, byName["partial"].bytes)
	}
}

func BenchmarkCacheTierScenarios(b *testing.B) {
	for range b.N {
		runCacheScenarios(b)
	}
}

func runCacheScenarios(tb testing.TB) []cacheScenarioResult {
	tb.Helper()
	ctx := context.Background()
	raw := fs.Open(tb.TempDir(), "cache-bench", objstore.Config{})
	if err := raw.EnsureBucket(ctx); err != nil {
		tb.Fatal(err)
	}
	s, fault := storetest.NewFault(raw)
	const objects, objectBytes, queries = 50, 32 << 10, 200
	oracle := make(map[string][]byte, objects)
	for i := range objects {
		key := fmt.Sprintf("ns/cachebench/seg/%03d/object", i)
		data := make([]byte, objectBytes)
		rand.New(rand.NewSource(int64(i + 1))).Read(data)
		oracle[key] = data
		if err := s.Put(ctx, key, data); err != nil {
			tb.Fatal(err)
		}
	}
	keys := slices.Sorted(maps.Keys(oracle))
	workingBytes := int64(objects * objectBytes)
	type scenario struct {
		name       string
		diskBytes  int64
		warm, wipe bool
	}
	scenarios := []scenario{
		{name: "cold"},
		{name: "warm", diskBytes: workingBytes, warm: true},
		{name: "partial", diskBytes: workingBytes / 2, warm: true},
		{name: "thrashing", diskBytes: workingBytes / 10},
		{name: "cache-loss", diskBytes: workingBytes, warm: true, wipe: true},
	}
	load := func(key string) func(context.Context) ([]byte, error) {
		return func(ctx context.Context) ([]byte, error) {
			if os.Getenv("OBJSTORE_CACHE_BENCH_LONG") == "1" {
				time.Sleep(20 * time.Millisecond)
			}
			return s.Get(ctx, key)
		}
	}
	results := make([]cacheScenarioResult, 0, len(scenarios))
	for _, sc := range scenarios {
		disk, err := NewDisk(tb.TempDir(), sc.diskBytes)
		if err != nil {
			tb.Fatal(err)
		}
		c := New(s, 1, disk, Keys{}) // a memory tier that holds nothing: exercise the SSD tier
		if sc.warm {
			warmKeys := keys
			if sc.name == "partial" {
				warmKeys = keys[:len(keys)/2]
			}
			for _, key := range warmKeys {
				disk.Put(DiskKey(key, c.Memory.GenerationOf(key)), oracle[key])
			}
		}
		fault.ResetOps()
		latencies := make([]time.Duration, 0, queries)
		for i := range queries {
			if sc.wipe && i == queries/2 {
				if err := disk.Wipe(); err != nil {
					tb.Fatal(err)
				}
			}
			keyIndex := i / (queries / objects)
			if sc.name == "thrashing" {
				keyIndex = i % objects
			}
			key := keys[keyIndex]
			start := time.Now()
			got, err := c.FetchWith(ctx, key, load(key))
			if err != nil {
				tb.Fatal(err)
			}
			if !slices.Equal(got, oracle[key]) {
				tb.Fatalf("%s query %d differs from oracle", sc.name, i)
			}
			latencies = append(latencies, time.Since(start))
		}
		slices.Sort(latencies)
		ops, stats := fault.Ops(), disk.Stats()
		mh, mm := c.Memory.Stats()
		results = append(results, cacheScenarioResult{name: sc.name, bytes: fault.ReadBytes(), gets: ops[storetest.OpGet], ranges: ops[storetest.OpGetRange], p50: latencies[len(latencies)/2], p99: latencies[len(latencies)*99/100], memoryHits: mh, memoryMisses: mm, diskHits: stats.Hits, diskMisses: stats.Misses, evictions: stats.Evictions})
		if counts := c.ClassCounts(); counts.MemoryHits[ClassObject]+counts.DiskHits[ClassObject]+counts.Loads[ClassObject] != queries {
			tb.Fatalf("%s: %+v do not sum to %d lookups", sc.name, counts, queries)
		}
		c.Close()
	}
	return results
}

func TestWALPagesHaveTheirOwnBudget(t *testing.T) {
	cache := New(nil, 256<<20, nil, Keys{WAL: func(key string) bool { return strings.Contains(key, "/wal/") }})
	if cache.ByteCacheFor("ns/x/wal/page-1") != cache.WAL {
		t.Fatal("WAL page did not select its bounded cache")
	}
	probe := "ns/x/probe"
	cache.Memory.Put(probe, []byte("probe"), cache.Memory.GenerationOf(probe))
	for i := range 80 {
		key := "ns/x/wal/page-" + strconv.Itoa(i)
		cache.WAL.Put(key, bytes.Repeat([]byte{1}, 1<<20), cache.WAL.GenerationOf(key))
	}
	if got := cache.WAL.Charge(); got > cache.WAL.Cap {
		t.Fatalf("WAL charge %d exceeds cap %d", got, cache.WAL.Cap)
	}
	if _, ok := cache.Memory.Peek(probe); !ok {
		t.Fatal("WAL pages evicted a regular object")
	}
	cache.InvalidateNamespace("x")
	if got := cache.WAL.Charge(); got != 0 {
		t.Fatalf("WAL charge after invalidation = %d", got)
	}
}

// TestPutFillsTheDiskTier: an object this process put is on disk as well
// as in memory, so a range read the memory tier cannot answer (here: a
// table larger than the whole budget) is a disk hit, not a store read.
func TestPutFillsTheDiskTier(t *testing.T) {
	ctx := context.Background()
	disk := newDisk(t, 1<<20)
	c := New(bucket.New(t), 1<<10, disk, Keys{})
	key := "ns/x/table/1"
	data := bytes.Repeat([]byte("table"), 1000)
	if err := c.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Memory.Peek(key); ok {
		t.Fatal("a table past the memory budget was cached in memory")
	}
	b, fromDisk, ok := c.CachedRange(key, 100, 50)
	if !ok || !fromDisk || !bytes.Equal(b, data[100:150]) {
		t.Fatalf("CachedRange after a Put = %q disk %v ok %v, want a disk hit", b, fromDisk, ok)
	}
	// Under WithoutDiskFill (an indexer's output, deleted by a later
	// compaction) the write leaves the disk tier alone.
	if err := c.Put(WithoutDiskFill(ctx), "ns/x/table/2", data); err != nil {
		t.Fatal(err)
	}
	if st := disk.Stats(); st.Entries != 1 {
		t.Fatalf("disk entries = %d, want only the first table", st.Entries)
	}
}

// waiters is the number of callers joined to key's flight.
func (c *Cache) waiters(key string) int {
	c.flightMu.Lock()
	defer c.flightMu.Unlock()
	if ref := c.flights[key]; ref != nil {
		return ref.n
	}
	return 0
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		runtime.Gosched()
	}
}

// TestLeaderCancellationDoesNotFailFollowers: the shared load serves every
// waiter, so the caller that happened to start it leaving does not cancel
// it for the rest.
func TestLeaderCancellationDoesNotFailFollowers(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	entered, release := make(chan struct{}), make(chan struct{})
	load := func(ctx context.Context) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return []byte("obj"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.FetchWith(leaderCtx, "k", load)
		leaderErr <- err
	}()
	<-entered
	follower := make(chan error, 1)
	go func() {
		b, err := c.FetchWith(context.Background(), "k", load)
		if err == nil && string(b) != "obj" {
			err = fmt.Errorf("got %q", b)
		}
		follower <- err
	}()
	waitFor(t, func() bool { return c.waiters("k") == 2 })
	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v, want context.Canceled", err)
	}
	close(release)
	if err := <-follower; err != nil {
		t.Fatalf("follower failed with the leader: %v", err)
	}
}

// TestLastWaiterLeavingCancelsLoad: once nobody waits, the load is
// cancelled and the flight forgotten, so the next caller starts afresh
// instead of joining a load nobody wants.
func TestLastWaiterLeavingCancelsLoad(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	entered, release := make(chan struct{}), make(chan struct{})
	cancelled := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.FetchWith(ctx, "k", func(ctx context.Context) ([]byte, error) {
			close(entered)
			<-release // ignores cancellation for now
			cancelled <- ctx.Err()
			return nil, ctx.Err()
		})
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller: %v, want context.Canceled", err)
	}
	b, err := c.FetchWith(context.Background(), "k", func(context.Context) ([]byte, error) {
		return []byte("fresh"), nil
	})
	if err != nil || string(b) != "fresh" {
		t.Fatalf("next caller joined the abandoned load: %q, %v", b, err)
	}
	close(release)
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned load's context: %v, want context.Canceled", err)
	}
}

func TestLoaderPanicIsAnError(t *testing.T) {
	var logged bytes.Buffer
	c := New(nil, 1<<20, nil, Keys{})
	c.Logger = slog.New(slog.NewTextHandler(&logged, nil)) // the stack goes to the log
	_, err := c.FetchWith(context.Background(), "k", func(context.Context) ([]byte, error) { panic("boom") })
	if err == nil || !strings.Contains(err.Error(), "panic: boom") || strings.Contains(err.Error(), "goroutine") {
		t.Fatalf("err = %v, want the panic message without a stack", err)
	}
	if !strings.Contains(logged.String(), "loader panic") || !strings.Contains(logged.String(), "goroutine") {
		t.Fatalf("Cache.Logger did not get the panic and its stack: %q", logged.String())
	}
}

func TestGatedSpendsTheBudget(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	ctx, budget := WithBudget(context.Background(), 2)
	reads := 0
	read := func(context.Context) ([]byte, error) {
		reads++
		if c.GateInUse.Load() != 1 {
			t.Errorf("read outside the gate: %d slots in use", c.GateInUse.Load())
		}
		return nil, nil
	}
	for i := range 2 {
		if _, err := c.Gated(ctx, read); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if budget.Exceeded() {
		t.Fatal("budget exceeded within its limit")
	}
	for range 5 {
		if _, err := c.Gated(ctx, read); !errors.Is(err, ErrBudget) {
			t.Fatalf("over budget: %v, want ErrBudget", err)
		}
	}
	if reads != 2 || !budget.Exceeded() || budget.spent.Load() != 3 {
		t.Fatalf("reads %d, exceeded %v, spent %d", reads, budget.Exceeded(), budget.spent.Load())
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Gated(cancelled, read); !errors.Is(err, context.Canceled) || c.GateInUse.Load() != 0 {
		t.Fatalf("cancelled: %v, %d slots in use", err, c.GateInUse.Load())
	}
}

func TestOutcomeAndClassStrings(t *testing.T) {
	for v, want := range map[fmt.Stringer]string{MemoryHit: "memory-hit", DiskHit: "disk-hit", Load: "load", Outcome(9): "Outcome(9)", ClassBlock: "block", ClassObject: "object"} {
		if got := v.String(); got != want {
			t.Errorf("%d: %q, want %q", v, got, want)
		}
	}
	c := New(nil, 1<<20, nil, Keys{})
	c.NoteAs(context.Background(), "k", Outcome(-1), true) // out of range: counts nothing, does not panic
	c.NoteAs(context.Background(), "k", Outcome(3), true)
	if got := c.ClassCounts(); got.HitRatio() != 1 {
		t.Fatalf("out-of-range outcomes counted: %+v", got)
	}
}

// TestInvalidatedLoadSkipsTheDiskTier: a load whose namespace is
// invalidated while it runs publishes nowhere, the disk tier included: its
// disk key names a retired generation nothing reads again.
func TestInvalidatedLoadSkipsTheDiskTier(t *testing.T) {
	disk := newDisk(t, 1<<20)
	c := New(nil, 1<<20, disk, Keys{})
	key := "ns/x/obj"
	b, err := c.FetchWith(context.Background(), key, func(context.Context) ([]byte, error) {
		c.InvalidateNamespace("x")
		return []byte("stale"), nil
	})
	if err != nil || string(b) != "stale" {
		t.Fatalf("fetch: %q, %v", b, err)
	}
	if n := disk.Stats().Entries; n != 0 {
		t.Fatalf("%d disk entries under a retired generation", n)
	}
	if _, ok := c.Memory.Peek(key); ok {
		t.Fatal("memory kept a retired generation's value")
	}
}

// waitingContext signals once Fetch's select has registered its flight.
type waitingContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		panic("unreachable")
	}
}

func TestFetchFollowerRetriesBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c := New(nil, 1<<20, nil, Keys{})
	leaderCtx, _ := WithBudget(ctx, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	leader := make(chan error, 1)
	go func() {
		_, err := c.FetchWith(leaderCtx, "k", func(ctx context.Context) ([]byte, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return c.Gated(ctx, func(context.Context) ([]byte, error) { return []byte("leader"), nil })
		})
		leader <- err
	}()
	receive(t, ctx, entered)
	fctx := &waitingContext{Context: ctx, waiting: make(chan struct{})}
	follower := make(chan error, 1)
	go func() {
		b, err := c.FetchWith(fctx, "k", func(ctx context.Context) ([]byte, error) {
			return c.Gated(ctx, func(context.Context) ([]byte, error) { return []byte("fresh"), nil })
		})
		if err == nil && string(b) != "fresh" {
			err = fmt.Errorf("follower read %q", b)
		}
		follower <- err
	}()
	receive(t, ctx, fctx.waiting)
	close(release)
	if err := receive(t, ctx, leader); !errors.Is(err, ErrBudget) {
		t.Fatalf("leader: %v, want ErrBudget", err)
	}
	if err := receive(t, ctx, follower); err != nil {
		t.Fatalf("follower inherited the leader's budget: %v", err)
	}
}

func TestWithResultsReleasesPreviousStage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	// Return only the new scope and a weak reference to the old buffer.
	// A lookup alone cannot detect a shadowed map retained by the context.
	next, old := func() (context.Context, weak.Pointer[byte]) {
		b := make([]byte, 1<<20)
		old := weak.Make(&b[0])
		first := WithResults(ctx, map[string][]byte{"old": b})
		return WithResults(first, map[string][]byte{"new": []byte("new")}), old
	}()
	if _, ok := Scoped(next, "old"); ok {
		t.Fatal("new scope serves the old stage")
	}
	for old.Value() != nil && ctx.Err() == nil {
		runtime.GC()
	}
	runtime.KeepAlive(next)
	if old.Value() != nil {
		t.Fatal("new scope retains the old stage's buffer")
	}
}
