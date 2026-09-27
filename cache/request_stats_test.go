package cache

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentRequestStatsStayIsolated(t *testing.T) {
	c := New(nil, 4096, nil, Keys{})
	c.Memory.Put("hot", []byte("cached"), c.Memory.GenerationOf("hot"))
	hotCtx, hot := WithRequestStats(context.Background())
	coldCtx, cold := WithRequestStats(context.Background())
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, tc := range []struct {
		ctx context.Context
		key string
	}{{hotCtx, "hot"}, {coldCtx, "cold"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				if _, err := c.FetchWith(tc.ctx, tc.key, func(context.Context) ([]byte, error) {
					return make([]byte, 8192), nil // larger than cache capacity
				}); err != nil {
					t.Errorf("fetch %s: %v", tc.key, err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := hot.Classes(); got.MemoryHits[ClassObject] != 100 || got.Loads[ClassObject] != 0 {
		t.Fatalf("hot query: %+v", got)
	}
	if got := cold.Classes(); got.MemoryHits[ClassObject] != 0 || got.Loads[ClassObject] != 100 {
		t.Fatalf("cold query: %+v", got)
	}
}

func TestRequestStatsCountDiskHits(t *testing.T) {
	disk, err := NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	c := New(nil, 1024, disk, Keys{})
	t.Cleanup(c.Close)
	loads := 0
	load := func(context.Context) ([]byte, error) {
		loads++
		return make([]byte, 2048), nil // too large for the memory tier
	}
	var stats [2]*RequestStats
	for i := range stats {
		var ctx context.Context
		ctx, stats[i] = WithRequestStats(context.Background())
		for range 2 {
			if _, err := c.FetchWith(ctx, "disk-key", load); err != nil {
				t.Fatal(err)
			}
		}
	}
	if loads != 1 {
		t.Fatalf("store loads=%d, want 1", loads)
	}
	// A request's lookups of a key it already missed are loads for it (the
	// memory tier refuses the value, so every lookup misses): the first
	// request loads twice; the next one's first lookup is a disk hit.
	if got := stats[0].Classes(); got.Loads[ClassObject] != 2 || got.DiskHits[ClassObject] != 0 {
		t.Fatalf("loading request: %+v, want two loads", got)
	}
	if got := stats[1].Classes(); got.DiskHits[ClassObject] != 1 || got.Loads[ClassObject] != 1 {
		t.Fatalf("second request: %+v, want a disk hit, then a load", got)
	}
	if got := c.ClassCounts(); got.Loads[ClassObject] != 1 || got.DiskHits[ClassObject] != 3 {
		t.Fatalf("process: %+v, want one load and three disk hits", got)
	}
}

// A load that cuts its bytes from an object a cache tier already held is a
// hit of that tier; one cut from an object this request itself read from
// the store, or one that reports no cached source, is a load.
func TestRequestStatsCountSlicesOfCachedObjects(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	for _, tc := range []struct {
		name     string
		loadedBy bool // the request read "obj" from the store first
		cached   bool // the load reports a cached source
		fromDisk bool // ... on the disk tier
		want     Outcome
	}{
		{"slice of an object in memory before the request", false, true, false, MemoryHit},
		{"slice of an object on disk before the request", false, true, true, DiskHit},
		{"slice of an object this request loaded", true, true, false, Load},
		{"load with no cached source", false, false, false, Load},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stats := WithRequestStats(context.Background())
			if tc.loadedBy {
				NoteStoreLoad(ctx, "obj")
			}
			if _, err := c.FetchWith(ctx, "obj#col#"+tc.name, func(ctx context.Context) ([]byte, error) {
				if tc.cached {
					MarkServedFromCache(ctx, "obj", tc.fromDisk)
				}
				return []byte("col"), nil
			}); err != nil {
				t.Fatal(err)
			}
			var want ClassCounts
			[]*[NumClasses]int64{&want.MemoryHits, &want.DiskHits, &want.Loads}[tc.want][ClassObject] = 1
			if got := stats.Classes(); got != want {
				t.Fatalf("%+v, want %+v", got, want)
			}
		})
	}
}

func TestClassCountsSplitLookupsByKeyAndOutcome(t *testing.T) {
	keys := Keys{Ranged: func(key string) bool { return strings.Contains(key, "#range#") }}
	for key, want := range map[string]Class{
		"ns/a/table/1#range#0+4096":  ClassBlock,
		"ns/a/table/1#range#4096+50": ClassBlock,
		"ns/a/head/00000001":         ClassObject,
	} {
		if got := keys.class(key); got != want {
			t.Errorf("class(%q) = %d, want %d", key, got, want)
		}
	}
	disk, err := NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	key := "ns/a/table/1#range#0+4096"
	load := func(context.Context) ([]byte, error) { return []byte("region"), nil }
	cold := New(nil, 1<<20, disk, keys)
	for range 2 { // a load, then a memory hit
		if _, err := cold.FetchWith(context.Background(), key, load); err != nil {
			t.Fatal(err)
		}
	}
	if got := cold.ClassCounts(); got.Loads[ClassBlock] != 1 || got.MemoryHits[ClassBlock] != 1 || got.DiskHits[ClassBlock] != 0 {
		t.Fatalf("cold process: %+v, want one load and one memory hit", got)
	}
	// A new process over the same disk tier: its memory misses, the disk answers.
	warm := New(nil, 1<<20, disk, keys)
	if _, err := warm.FetchWith(context.Background(), key, func(context.Context) ([]byte, error) {
		t.Fatal("store read on a disk hit")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := warm.ClassCounts(); got.Loads[ClassBlock] != 0 || got.DiskHits[ClassBlock] != 1 || got.MemoryHits[ClassBlock] != 0 {
		t.Fatalf("disk-backed process: %+v, want one disk hit", got)
	}
}

// A caller that joins a load in flight counts a memory hit: the load is
// counted once, by the caller that ran it, so loads match store reads.
func TestFlightFollowerCountsAMemoryHit(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	entered, release := make(chan struct{}), make(chan struct{})
	reads := 0
	load := func(context.Context) ([]byte, error) {
		reads++
		close(entered)
		<-release
		return []byte("obj"), nil
	}
	leaderCtx, leader := WithRequestStats(context.Background())
	followerCtx, follower := WithRequestStats(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := c.FetchWith(leaderCtx, "k", load); err != nil {
			t.Error(err)
		}
	}()
	<-entered
	go func() {
		defer wg.Done()
		if _, err := c.FetchWith(followerCtx, "k", load); err != nil {
			t.Error(err)
		}
	}()
	waitFor(t, func() bool { return c.waiters("k") == 2 }) // the follower joined the flight
	close(release)
	wg.Wait()
	if reads != 1 {
		t.Fatalf("%d store reads, want 1", reads)
	}
	if got := leader.Classes(); got.Loads[ClassObject] != 1 || got.MemoryHits[ClassObject] != 0 {
		t.Fatalf("leader: %+v, want one load", got)
	}
	if got := follower.Classes(); got.MemoryHits[ClassObject] != 1 || got.Loads[ClassObject] != 0 {
		t.Fatalf("follower: %+v, want one memory hit", got)
	}
	if got := c.ClassCounts(); got.Loads[ClassObject] != 1 || got.MemoryHits[ClassObject] != 1 {
		t.Fatalf("process: %+v, want one load and one memory hit", got)
	}
}

// For its request, a hit on a key the request missed is a load, and a hit
// on a view it decoded is no lookup; the process counts both as hits.
func TestRequestChargesItsOwnLoadsAndViews(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	ctx, stats := WithRequestStats(context.Background())
	MarkMissed(ctx, "loaded")
	NoteBuilt(ctx, "view/1")
	c.Note(ctx, "loaded", MemoryHit)
	c.Note(ctx, "view/1", MemoryHit)
	c.Note(ctx, "other", DiskHit)
	want := ClassCounts{}
	want.Loads[ClassObject], want.DiskHits[ClassObject] = 1, 1
	if got := stats.Classes(); got != want {
		t.Fatalf("request: %+v, want %+v", got, want)
	}
	want = ClassCounts{}
	want.MemoryHits[ClassObject], want.DiskHits[ClassObject] = 2, 1
	if got := c.ClassCounts(); got != want {
		t.Fatalf("process: %+v, want %+v", got, want)
	}
}

// TestARequestsOwnFillIsNoHit: a key a request missed stays a load for
// that request when its later lookups find the fill in memory, whichever
// worker's lookup came first; another request, and another key, hit it.
func TestARequestsOwnFillIsNoHit(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	c.Memory.Put("warm", []byte("cached"), c.Memory.GenerationOf("warm"))
	ctx, own := WithRequestStats(context.Background())
	load := func(context.Context) ([]byte, error) { return []byte("block"), nil }
	for range 3 {
		if _, err := c.FetchWith(ctx, "cold", load); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.FetchWith(ctx, "warm", load); err != nil {
		t.Fatal(err)
	}
	if got := own.Classes(); got.Loads[ClassObject] != 3 || got.MemoryHits[ClassObject] != 1 {
		t.Fatalf("own fill: %+v, want 3 loads and 1 memory hit", got)
	}
	other, stats := WithRequestStats(context.Background())
	if _, err := c.FetchWith(other, "cold", load); err != nil {
		t.Fatal(err)
	}
	if got := stats.Classes(); got.MemoryHits[ClassObject] != 1 || got.Loads[ClassObject] != 0 {
		t.Fatalf("another request: %+v, want 1 memory hit", got)
	}
}
