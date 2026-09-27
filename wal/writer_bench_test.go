package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
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
	batch := &batch[Bytes]{records: records, nonce: "benchmark"}
	b.SetBytes(rows * size)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := splitBatch(1, batch); err != nil {
			b.Fatal(err)
		}
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

// BenchmarkWALGroupCommit measures sustained, acknowledged writes. Each caller
// sends batches back-to-back for five seconds; small batches contain 256
// records and large batches contain 8, keeping the entry-rate target feasible.
func BenchmarkWALGroupCommit(b *testing.B) {
	for _, backend := range []string{"file", "fault100ms"} {
		for _, interval := range []time.Duration{time.Second, 100 * time.Millisecond} {
			for _, callers := range []int{8, 64} {
				for _, size := range []int{512, 500_000} {
					name := fmt.Sprintf("%s/%s/%dcallers/%dB", backend, interval, callers, size)
					b.Run(name, func(b *testing.B) {
						for range b.N {
							benchmarkGroupCommitRun(b, backend, interval, callers, size)
						}
					})
				}
			}
		}
	}
}

func benchmarkGroupCommitRun(b *testing.B, backend string, interval time.Duration, callers, size int) {
	b.Helper()
	ctx := context.Background()
	s, err := objstore.New(ctx, "file://"+b.TempDir(), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		b.Fatal(err)
	}
	var fault *storetest.Fault
	if backend == "fault100ms" {
		s, fault = storetest.NewFault(s)
		fault.SetShape(storetest.Shape{Latency: 100 * time.Millisecond})
	}
	batchSize := 256
	if size == 500_000 {
		batchSize = 8
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var latencies []time.Duration
	var written int
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	start := time.Now()
	stop := start.Add(5 * time.Second)
	for caller := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]Bytes, batchSize)
			for i := range batch {
				batch[i] = sizedRecord(fmt.Sprintf("%d-%d", caller, i), size)
			}
			var own []time.Duration
			var count int
			for time.Now().Before(stop) {
				began := time.Now()
				if err := w.Append(ctx, batch); err != nil {
					if errors.Is(err, ErrOverloaded) {
						time.Sleep(time.Millisecond)
						continue
					}
					b.Errorf("append: %v", err)
					break
				}
				own = append(own, time.Since(began))
				count += len(batch)
			}
			mu.Lock()
			latencies = append(latencies, own...)
			written += count
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	w.Close()
	keys, err := s.List(ctx, testPrefix)
	if err != nil {
		b.Fatal(err)
	}
	if len(latencies) == 0 {
		b.Fatal("no completed appends")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(written)/elapsed, "records/s")
	b.ReportMetric(float64(written*size)/elapsed/1e6, "MB/s")
	b.ReportMetric(float64(len(keys))/elapsed, "entries/s")
	b.ReportMetric(float64(latencies[len(latencies)/2].Microseconds())/1e3, "p50_ms")
	b.ReportMetric(float64(latencies[(len(latencies)*99+99)/100-1].Microseconds())/1e3, "p99_ms")
}

// BenchmarkWALGroupCommitUncontended separates idle write latency from the
// sustained matrix, where a caller immediately queues its next write.
func BenchmarkWALGroupCommitUncontended(b *testing.B) {
	for _, size := range []int{512, 500_000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			for range b.N {
				s, err := objstore.New(context.Background(), "file://"+b.TempDir(), "uncontended")
				if err != nil {
					b.Fatal(err)
				}
				if err := s.EnsureBucket(context.Background()); err != nil {
					b.Fatal(err)
				}
				s, f := storetest.NewFault(s)
				f.SetShape(storetest.Shape{Latency: 100 * time.Millisecond})
				w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(time.Second))
				record := []Bytes{sizedRecord("row", size)}
				var samples []time.Duration
				for range 5 {
					start := time.Now()
					if err := w.Append(context.Background(), record); err != nil {
						b.Fatal(err)
					}
					samples = append(samples, time.Since(start))
					time.Sleep(time.Second)
				}
				w.Close()
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				b.ReportMetric(float64(samples[len(samples)/2].Microseconds())/1e3, "p50_ms")
				b.ReportMetric(float64(samples[len(samples)-1].Microseconds())/1e3, "p99_ms")
			}
		})
	}
}

// BenchmarkWALCommitUnderBulkLoad is a streaming writer's commit beside a
// bulk writer on the same store: `callers` clients each append a 10k-row
// entry of 128 random int8 bytes plus a small id (~1.4 MB) back to back
// while `bulk` goroutines Put 32 MiB objects as fast as the store takes
// them. Latency is Append to ack. OBJSTORE_WAL_BENCH_DIR puts the store on
// a real disk; the default temp dir may be tmpfs. OBJSTORE_WAL_BENCH_SECONDS
// (default 20) is each cell's length.
func BenchmarkWALCommitUnderBulkLoad(b *testing.B) {
	seconds := 20
	if v, err := strconv.Atoi(os.Getenv("OBJSTORE_WAL_BENCH_SECONDS")); err == nil && v > 0 {
		seconds = v
	}
	for _, callers := range []int{1, 4} {
		for _, bulk := range []int{0, 2, 6} {
			b.Run(fmt.Sprintf("%dcallers/%dbulk", callers, bulk), func(b *testing.B) {
				for range b.N {
					benchmarkCommitUnderBulkLoad(b, callers, bulk, time.Duration(seconds)*time.Second)
				}
			})
		}
	}
}

func benchmarkCommitUnderBulkLoad(b *testing.B, callers, bulk int, length time.Duration) {
	b.Helper()
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
	s, err := objstore.New(ctx, "file://"+dir, "bench")
	if err != nil {
		b.Fatal(err)
	}
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
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	var mu sync.Mutex
	var latencies []time.Duration
	var written int
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(length)
	for c := range callers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(c), 1))
			var own []time.Duration
			count := 0
			for time.Now().Before(end) {
				records := make([]Bytes, rows)
				for i := range records {
					r := strconv.AppendInt(make(Bytes, 0, 16+dims), int64(c*1e9+count+i), 10)
					for range dims {
						r = append(r, byte(int8(rng.IntN(255)-127)))
					}
					records[i] = r
				}
				began := time.Now()
				if err := w.Append(ctx, records); err != nil {
					b.Errorf("append: %v", err)
					return
				}
				own = append(own, time.Since(began))
				count += rows
			}
			mu.Lock()
			latencies = append(latencies, own...)
			written += count
			mu.Unlock()
		})
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	close(stop)
	bg.Wait()
	w.Close()
	keys, err := s.List(ctx, testPrefix)
	if err != nil {
		b.Fatal(err)
	}
	if len(latencies) == 0 {
		b.Fatal("no completed appends")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(written)/elapsed, "rows/s")
	b.ReportMetric(float64(bulkBytes.Load())/elapsed/1e6, "bulk_MB/s")
	b.ReportMetric(float64(len(keys))/elapsed, "entries/s")
	b.ReportMetric(float64(latencies[len(latencies)/2].Microseconds())/1e3, "p50_ms")
	b.ReportMetric(float64(latencies[(len(latencies)*99+99)/100-1].Microseconds())/1e3, "p99_ms")
}
