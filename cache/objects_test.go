package cache

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
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
	raw, err := objstore.New(ctx, "file://"+tb.TempDir(), "cache-bench")
	if err != nil {
		tb.Fatal(err)
	}
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
	keys := make([]string, 0, objects)
	for key := range oracle {
		keys = append(keys, key)
	}
	slices.Sort(keys)
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
	results := make([]cacheScenarioResult, 0, len(scenarios))
	for _, sc := range scenarios {
		disk, err := NewDisk(tb.TempDir(), sc.diskBytes)
		if err != nil {
			tb.Fatal(err)
		}
		memory := NewByteCache(1) // force this matrix to exercise the SSD tier
		if sc.warm {
			warmKeys := keys
			if sc.name == "partial" {
				warmKeys = keys[:len(keys)/2]
			}
			for _, key := range warmKeys {
				disk.Put(DiskKey(key, 0), oracle[key])
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
			got, ok := memory.Get(key)
			if !ok {
				got, ok = disk.Get(DiskKey(key, 0))
				if !ok {
					if os.Getenv("OBJSTORE_CACHE_BENCH_LONG") == "1" {
						time.Sleep(20 * time.Millisecond)
					}
					got, err = s.Get(ctx, key)
					if err != nil {
						tb.Fatal(err)
					}
					disk.Put(DiskKey(key, 0), got)
				}
				memory.Put(key, got, 0)
			}
			if !slices.Equal(got, oracle[key]) {
				tb.Fatalf("%s query %d differs from oracle", sc.name, i)
			}
			latencies = append(latencies, time.Since(start))
		}
		slices.Sort(latencies)
		ops, stats := fault.Ops(), disk.Stats()
		mh, mm := memory.Stats()
		results = append(results, cacheScenarioResult{name: sc.name, bytes: fault.ReadBytes(), gets: ops[storetest.OpGet], ranges: ops[storetest.OpGetRange], p50: latencies[len(latencies)/2], p99: latencies[len(latencies)*99/100], memoryHits: mh, memoryMisses: mm, diskHits: stats.Hits, diskMisses: stats.Misses, evictions: stats.Evictions})
		disk.Close()
	}
	return results
}

func TestWALPagesHaveTheirOwnBudget(t *testing.T) {
	cache := New(nil, 256<<20, nil, Keys{WAL: func(key string) bool { return strings.Contains(key, "/wal/") }})
	if cache.ByteCacheFor("ns/x/wal/page-1") != cache.WAL {
		t.Fatal("WAL page did not select its bounded cache")
	}
	probe := "ns/x/probe"
	cache.Memory.Put(probe, []byte("probe"), cache.Memory.Generation.Load())
	for i := range 80 {
		key := "ns/x/wal/page-" + strconv.Itoa(i)
		cache.WAL.Put(key, bytes.Repeat([]byte{1}, 1<<20), cache.WAL.Generation.Load())
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
