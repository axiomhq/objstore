package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
)

func BenchmarkSplitBatch(b *testing.B) {
	const rows, size = 1024, 512
	records := make([]Bytes, rows)
	for i := range records {
		r := make(Bytes, size)
		for j := range r {
			r[j] = byte(i + j)
		}
		records[i] = r
	}
	for _, bc := range []struct {
		name      string
		pageLimit int // zero: one page
		pages     int
	}{{"single-page", 0, 1}, {"multi-page", 64 << 10, 9}} {
		b.Run(bc.name, func(b *testing.B) {
			batch := &batch[Bytes]{records: records, nonce: "benchmark", pageLimit: bc.pageLimit}
			b.SetBytes(rows * size)
			b.ReportAllocs()
			for b.Loop() {
				if pages, _, err := splitBatch(1, batch); err != nil || len(pages) != bc.pages {
					b.Fatal(len(pages), err)
				}
			}
		})
	}
}

// sizedRecord is a record of exactly size bytes that starts with id.
func sizedRecord(id string, size int) Bytes {
	r := make(Bytes, size)
	for i := copy(r, id); i < size; i++ {
		r[i] = 'x'
	}
	return r
}

// appendLoop runs b.N appends spread over callers, each caller sending its
// next append as soon as the last is acked, and reports throughput and ack
// latency once. next builds a caller's next batch.
func appendLoop(b *testing.B, w *Writer[Bytes], callers int, next func(caller int) []Bytes) (written int, latencies []time.Duration, elapsed time.Duration) {
	b.Helper()
	ctx := context.Background()
	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var mu sync.Mutex
	var wg sync.WaitGroup
	b.ResetTimer()
	start := time.Now()
	for caller := range callers {
		wg.Go(func() {
			var own []time.Duration
			count := 0
			for remaining.Add(-1) >= 0 {
				batch := next(caller)
				began := time.Now()
				for {
					err := w.Append(ctx, batch)
					if errors.Is(err, ErrOverloaded) {
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil {
						b.Errorf("append: %v", err)
						return
					}
					break
				}
				own = append(own, time.Since(began))
				count += len(batch)
			}
			mu.Lock()
			latencies = append(latencies, own...)
			written += count
			mu.Unlock()
		})
	}
	wg.Wait()
	elapsed = time.Since(start)
	b.StopTimer()
	if len(latencies) == 0 {
		b.Fatal("no completed appends")
	}
	slices.Sort(latencies)
	return written, latencies, elapsed
}

func reportLatencies(b *testing.B, latencies []time.Duration) {
	b.ReportMetric(float64(latencies[len(latencies)/2].Microseconds())/1e3, "p50_ms")
	b.ReportMetric(float64(latencies[(len(latencies)*99+99)/100-1].Microseconds())/1e3, "p99_ms")
}

// BenchmarkWALGroupCommit measures sustained, acknowledged writes: b.N
// appends spread over the callers, each sending its next batch as soon as
// the last is acked. Small batches carry 256 records, large ones 8, keeping
// the entry-rate target feasible.
func BenchmarkWALGroupCommit(b *testing.B) {
	for _, backend := range []string{"file", "fault100ms"} {
		for _, interval := range []time.Duration{time.Second, 100 * time.Millisecond} {
			for _, callers := range []int{8, 64} {
				for _, size := range []int{512, 500_000} {
					name := fmt.Sprintf("%s/%s/%dcallers/%dB", backend, interval, callers, size)
					b.Run(name, func(b *testing.B) {
						benchmarkGroupCommit(b, backend, interval, callers, size)
					})
				}
			}
		}
	}
}

func benchmarkGroupCommit(b *testing.B, backend string, interval time.Duration, callers, size int) {
	ctx := context.Background()
	s := fs.Open(b.TempDir(), "benchmark", objstore.Config{})
	if err := s.EnsureBucket(ctx); err != nil {
		b.Fatal(err)
	}
	if backend == "fault100ms" {
		var fault *storetest.Fault
		s, fault = storetest.NewFault(s)
		fault.SetShape(storetest.Shape{Latency: 100 * time.Millisecond})
	}
	batchSize := 256
	if size == 500_000 {
		batchSize = 8
	}
	batches := make([][]Bytes, callers)
	for caller := range batches {
		batches[caller] = make([]Bytes, batchSize)
		for i := range batches[caller] {
			batches[caller][i] = sizedRecord(fmt.Sprintf("%d-%d", caller, i), size)
		}
	}
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	written, latencies, elapsed := appendLoop(b, w, callers, func(caller int) []Bytes { return batches[caller] })
	w.Close()
	keys, err := s.List(ctx, testPrefix)
	if err != nil {
		b.Fatal(err)
	}
	seconds := elapsed.Seconds()
	b.ReportMetric(float64(written)/seconds, "records/s")
	b.ReportMetric(float64(written*size)/seconds/1e6, "MB/s")
	b.ReportMetric(float64(len(keys))/seconds, "entries/s")
	reportLatencies(b, latencies)
}

// BenchmarkWALGroupCommitUncontended separates idle write latency from the
// sustained matrix, where a caller immediately queues its next write: each
// append finds the writer idle, a commit interval after the last.
func BenchmarkWALGroupCommitUncontended(b *testing.B) {
	const interval = 10 * time.Millisecond
	for _, size := range []int{512, 500_000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			s := fs.Open(b.TempDir(), "uncontended", objstore.Config{})
			if err := s.EnsureBucket(context.Background()); err != nil {
				b.Fatal(err)
			}
			s, f := storetest.NewFault(s)
			f.SetShape(storetest.Shape{Latency: 100 * time.Millisecond})
			w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
			defer w.Close()
			record := []Bytes{sizedRecord("row", size)}
			samples := make([]time.Duration, 0, b.N)
			for range b.N {
				b.StopTimer()
				time.Sleep(interval) // idle again
				b.StartTimer()
				start := time.Now()
				if err := w.Append(context.Background(), record); err != nil {
					b.Fatal(err)
				}
				samples = append(samples, time.Since(start))
			}
			b.StopTimer()
			slices.Sort(samples)
			reportLatencies(b, samples)
		})
	}
}

// BenchmarkWALCommitUnderBulkLoad is a streaming writer's commit beside a
// bulk writer on the same store: `callers` clients append b.N 10k-row
// entries of 128 random int8 bytes plus a small id (~1.4 MB) back to back
// while `bulk` goroutines Put 32 MiB objects as fast as the store takes
// them. Latency is Append to ack. OBJSTORE_WAL_BENCH_DIR puts the store on
// a real disk; the default temp dir may be tmpfs.
func BenchmarkWALCommitUnderBulkLoad(b *testing.B) {
	for _, callers := range []int{1, 4} {
		for _, bulk := range []int{0, 2, 6} {
			b.Run(fmt.Sprintf("%dcallers/%dbulk", callers, bulk), func(b *testing.B) {
				benchmarkCommitUnderBulkLoad(b, callers, bulk)
			})
		}
	}
}

func benchmarkCommitUnderBulkLoad(b *testing.B, callers, bulk int) {
	ctx := context.Background()
	dir := os.Getenv("OBJSTORE_WAL_BENCH_DIR")
	if dir == "" {
		dir = b.TempDir()
	} else {
		var err error
		if dir, err = os.MkdirTemp(dir, "walbulk-"); err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(dir)
	}
	s := fs.Open(dir, "bench", objstore.Config{})
	if err := s.EnsureBucket(ctx); err != nil {
		b.Fatal(err)
	}
	const rows, dims = 10_000, 128
	stop := make(chan struct{})
	var bulkBytes atomic.Int64
	var bg sync.WaitGroup
	object := make([]byte, 32<<20)
	for i := range object {
		object[i] = byte(i * 7)
	}
	for f := range bulk {
		bg.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("bulk/%d/%d", f, n)
				if err := s.Put(ctx, key, object); err != nil {
					b.Errorf("bulk put: %v", err)
					return
				}
				bulkBytes.Add(int64(len(object)))
				_ = s.Delete(ctx, key)
			}
		})
	}
	rngs := make([]*rand.Rand, callers)
	counts := make([]int, callers)
	for c := range rngs {
		rngs[c] = rand.New(rand.NewPCG(uint64(c), 1))
	}
	next := func(c int) []Bytes {
		records := make([]Bytes, rows)
		for i := range records {
			r := strconv.AppendInt(make(Bytes, 0, 16+dims), int64(c*1e9+counts[c]+i), 10)
			for range dims {
				r = append(r, byte(int8(rngs[c].IntN(255)-127)))
			}
			records[i] = r
		}
		counts[c] += rows
		return records
	}
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	written, latencies, elapsed := appendLoop(b, w, callers, next)
	close(stop)
	bg.Wait()
	w.Close()
	keys, err := s.List(ctx, testPrefix)
	if err != nil {
		b.Fatal(err)
	}
	seconds := elapsed.Seconds()
	b.ReportMetric(float64(written)/seconds, "rows/s")
	b.ReportMetric(float64(bulkBytes.Load())/seconds/1e6, "bulk_MB/s")
	b.ReportMetric(float64(len(keys))/seconds, "entries/s")
	reportLatencies(b, latencies)
}
