package cache

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentRequestStatsStayIsolated(t *testing.T) {
	c := New(nil, 1024, nil, Keys{})
	c.Memory.Put("hot", []byte("cached"), c.Memory.Generation.Load())
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
					return make([]byte, 2048), nil // larger than cache capacity
				}); err != nil {
					t.Errorf("fetch %s: %v", tc.key, err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if h, m := hot.Counts(); h != 100 || m != 0 {
		t.Fatalf("hot query hits/misses=%d/%d", h, m)
	}
	if h, m := cold.Counts(); h != 0 || m != 100 {
		t.Fatalf("cold query hits/misses=%d/%d", h, m)
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
	// The first request loads the key, the second finds it on disk; a
	// request's own fill is no hit for it (TestARequestsOwnFillIsNoHit).
	for i, want := range [][2]int64{{0, 1}, {1, 0}} {
		ctx, stats := WithRequestStats(context.Background())
		if _, err := c.FetchWith(ctx, "disk-key", load); err != nil {
			t.Fatal(err)
		}
		if hits, misses := stats.Counts(); hits != want[0] || misses != want[1] {
			t.Fatalf("request %d hits/misses=%d/%d, want %d/%d", i, hits, misses, want[0], want[1])
		}
	}
	if loads != 1 {
		t.Fatalf("store loads=%d, want 1", loads)
	}
}

// A load that cuts its bytes from an object a cache tier already held is a
// request hit; one cut from an object this request itself read from the
// store, or one that reports no cached source, is a miss.
func TestRequestStatsCountSlicesOfCachedObjects(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	for _, tc := range []struct {
		name     string
		loadedBy bool // the request read "obj" from the store first
		cached   bool // the load reports a cached source
		want     int64
	}{
		{"slice of an object cached before the request", false, true, 1},
		{"slice of an object this request loaded", true, true, 0},
		{"load with no cached source", false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stats := WithRequestStats(context.Background())
			if tc.loadedBy {
				NoteStoreLoad(ctx, "obj")
			}
			if _, err := c.FetchWith(ctx, "obj#col#"+tc.name, func(ctx context.Context) ([]byte, error) {
				if tc.cached {
					MarkServedFromCache(ctx, "obj")
				}
				return []byte("col"), nil
			}); err != nil {
				t.Fatal(err)
			}
			if hits, misses := stats.Counts(); hits != tc.want || misses != 1-tc.want {
				t.Fatalf("hits/misses=%d/%d, want %d/%d", hits, misses, tc.want, 1-tc.want)
			}
		})
	}
}

func TestClassCountsSplitMissesByKeyAndDiskHits(t *testing.T) {
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
	for range 2 { // the second fetch is a memory hit and counts nothing
		if _, err := cold.FetchWith(context.Background(), key, load); err != nil {
			t.Fatal(err)
		}
	}
	if got := cold.ClassCounts(); got.Misses[ClassBlock] != 1 || got.DiskHits[ClassBlock] != 0 {
		t.Fatalf("cold process: %+v, want one probe miss, no disk hit", got)
	}
	// A new process over the same disk tier: its memory misses, the disk answers.
	warm := New(nil, 1<<20, disk, keys)
	if _, err := warm.FetchWith(context.Background(), key, func(context.Context) ([]byte, error) {
		t.Fatal("store read on a disk hit")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := warm.ClassCounts(); got.Misses[ClassBlock] != 1 || got.DiskHits[ClassBlock] != 1 {
		t.Fatalf("disk-backed process: %+v, want one probe miss answered by disk", got)
	}
}

// TestARequestsOwnFillIsNoHit: a key a request missed stays a miss for
// that request when its later lookups find the fill in memory, whichever
// worker's lookup came first; another request, and another key, hit it.
func TestARequestsOwnFillIsNoHit(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	c.Memory.Put("warm", []byte("cached"), c.Memory.Generation.Load())
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
	if h, m := own.Counts(); h != 1 || m != 3 {
		t.Fatalf("own fill hits/misses=%d/%d, want 1/3", h, m)
	}
	if got := own.Classes().Misses[ClassObject]; got != 3 {
		t.Fatalf("own fill charged %d memory misses, want 3", got)
	}
	other, stats := WithRequestStats(context.Background())
	if _, err := c.FetchWith(other, "cold", load); err != nil {
		t.Fatal(err)
	}
	if h, m := stats.Counts(); h != 1 || m != 0 {
		t.Fatalf("another request's hits/misses=%d/%d, want 1/0", h, m)
	}
}
