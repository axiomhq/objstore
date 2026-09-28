package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

// summary is a page as a test decoder sees it: its records (nil for a
// Scan-only decode), their count and the page's wire size.
type summary struct {
	records [][]byte
	rows    int
	bytes   int
}

func decodeSummary(data []byte) (Header, summary, error) {
	h, records, err := Decode(data)
	return h, summary{records: records, rows: len(records), bytes: len(data)}, err
}

func scanSummary(data []byte) (Header, summary, error) {
	rows := 0
	h, err := Scan(data, func([]byte) error { rows++; return nil })
	return h, summary{rows: rows, bytes: len(data)}, err
}

func TestWalkParallelMatchesWalk(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	rng := rand.New(rand.NewSource(23))
	type page struct {
		h  Header
		id string
	}
	var pages []page
	add := func(n int, nonce string, have int) {
		for i := 0; i < have; i++ {
			seq := uint64(len(pages) + 1)
			p := page{h: Header{Seq: seq}, id: fmt.Sprintf("id-%d-%d", seq, rng.Intn(1000))}
			if n > 1 {
				p.h.Nonce, p.h.BatchPages, p.h.BatchIndex = nonce, uint64(n), uint64(i)
			}
			pages = append(pages, p)
		}
	}
	for i := 0; i < 20; i++ {
		if rng.Intn(2) == 0 {
			add(1, "", 1)
		} else {
			n := 2 + rng.Intn(3)
			add(n, fmt.Sprintf("batch-%d", i), n)
		}
	}
	add(3, "abandoned", 1)
	add(2, "replacement", 2)
	for i := 0; i < 20; i++ {
		add(1, "", 1)
	}
	partialStart := uint64(len(pages) + 1)
	add(3, "tail", 2)
	for _, p := range pages {
		if ok, err := put(ctx, s, testPrefix, p.h, Bytes(p.id)); !ok || err != nil {
			t.Fatalf("append %d: %v", p.h.Seq, err)
		}
	}
	for _, tc := range []struct {
		name    string
		through uint64
	}{
		{"unbounded", 0},
		{"completed", partialStart - 1},
		{"inside-batch", partialStart},
		{"missing-bounded", uint64(len(pages) + 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, decoder := range []struct {
				name string
				fn   func([]byte) (Header, summary, error)
			}{{"decode", decodeSummary}, {"scan", scanSummary}} {
				var want []Entry[summary]
				wantErr := Walk(ctx, s, testPrefix, 0, tc.through, decoder.fn, func(e Entry[summary]) error {
					want = append(want, e)
					return nil
				})
				for _, workers := range []int{1, 3, 8} {
					var got []Entry[summary]
					gotErr := WalkParallel(ctx, s, testPrefix, 0, tc.through, workers, decoder.fn, nil, func(e Entry[summary]) error {
						got = append(got, e)
						return nil
					})
					if !reflect.DeepEqual(got, want) || errorText(gotErr) != errorText(wantErr) {
						t.Fatalf("%s workers=%d: visits=%+v, want=%+v; error=%v, want=%v", decoder.name, workers, got, want, gotErr, wantErr)
					}
				}
			}
		})
	}
}

func TestWalkParallelVisitError(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	for seq := uint64(1); seq <= 20; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes(fmt.Sprint(seq))); !ok || err != nil {
			t.Fatal(err)
		}
	}
	stop := errors.New("stop visiting")
	var visited []uint64
	err := WalkParallel(ctx, s, testPrefix, 0, 0, 8, Decode, nil, func(e entry) error {
		visited = append(visited, e.Seq)
		if e.Seq == 4 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || !reflect.DeepEqual(visited, []uint64{1, 2, 3, 4}) {
		t.Fatalf("visited=%v, error=%v", visited, err)
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestWalkParallelStopsAtFirstError(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	for seq := uint64(1); seq <= 12; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes(fmt.Sprint(seq))); !ok || err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(ctx, Key(testPrefix, 7), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	var want, got []uint64
	wantErr := Walk(ctx, s, testPrefix, 0, 0, Decode, func(e entry) error { want = append(want, e.Seq); return nil })
	before := runtime.NumGoroutine()
	gotErr := WalkParallel(ctx, s, testPrefix, 0, 0, 8, Decode, nil, func(e entry) error { got = append(got, e.Seq); return nil })
	if !reflect.DeepEqual(got, want) || errorText(gotErr) != errorText(wantErr) || !errors.Is(gotErr, ErrCorrupt) {
		t.Fatalf("visits=%v want=%v, error=%v want=%v", got, want, gotErr, wantErr)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before+1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+1 {
		t.Fatalf("goroutines after walk: %d, before %d", n, before)
	}
}

func TestWalkParallelCancellation(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	for seq := uint64(1); seq <= 64; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes(fmt.Sprint(seq))); !ok || err != nil {
			t.Fatal(err)
		}
	}
	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	visits := 0
	err := WalkParallel(walkCtx, s, testPrefix, 0, 0, 8, Decode, nil, func(entry) error {
		visits++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || visits != 1 || time.Since(start) > time.Second {
		t.Fatalf("visits=%d, error=%v, elapsed=%s", visits, err, time.Since(start))
	}
}

// TestWalkParallelWithGetFetchesConcurrently: the GETs themselves run on
// the workers. The first four reads wait for each other; a walk that
// fetched serially would never let the first one finish.
func TestWalkParallelWithGetFetchesConcurrently(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	const pages, workers = 8, 4
	for seq := uint64(1); seq <= pages; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes(fmt.Sprint(seq))); !ok || err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	entered := 0
	all := make(chan struct{})
	get := func(ctx context.Context, key string) ([]byte, error) {
		seq, err := SeqFromKey(key)
		if err != nil {
			return nil, err
		}
		if seq <= workers {
			mu.Lock()
			if entered++; entered == workers {
				close(all)
			}
			mu.Unlock()
			select {
			case <-all:
			case <-time.After(10 * time.Second):
				return nil, errors.New("reads were not concurrent")
			}
		}
		return s.Get(ctx, key)
	}
	var got []uint64
	err := WalkParallelWithGet(ctx, get, testPrefix, 0, pages, workers, Decode, nil, func(e entry) error {
		got = append(got, e.Seq)
		return nil
	})
	if err != nil || len(got) != pages || !slices.IsSorted(got) {
		t.Fatalf("visits %v, err %v", got, err)
	}
}

func BenchmarkWalk(b *testing.B) {
	ctx := context.Background()
	s := storetest.New(b)
	const pages = 64
	for seq := uint64(1); seq <= pages; seq++ {
		records := make([]Bytes, 256)
		for i := range records {
			records[i] = filled(byte(i), 512)
		}
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, records...); !ok || err != nil {
			b.Fatal(err)
		}
	}
	for _, workers := range []int{0, 1, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				visit := func(entry) error { n++; return nil }
				var err error
				if workers == 0 {
					err = Walk(ctx, s, testPrefix, 0, 0, Decode, visit)
				} else {
					err = WalkParallel(ctx, s, testPrefix, 0, 0, workers, Decode, nil, visit)
				}
				if err != nil || n != pages {
					b.Fatal(n, err)
				}
			}
		})
	}
}

// syntheticPages serves pages of size bytes for seq <= last and not-found
// past it, counting GETs; decodeSynthetic reads the sequence back.
type syntheticPages struct {
	last, size    uint64
	gets, missing atomic.Int64
	missDelay     time.Duration
}

func (p *syntheticPages) get(ctx context.Context, key string) ([]byte, error) {
	seq, err := SeqFromKey(key)
	if err != nil {
		return nil, err
	}
	if seq > p.last {
		p.missing.Add(1)
		time.Sleep(p.missDelay)
		return nil, objstore.ErrNotFound
	}
	p.gets.Add(1)
	b := make([]byte, p.size)
	binary.BigEndian.PutUint64(b, seq)
	return b, nil
}

func decodeSynthetic(b []byte) (Header, int, error) {
	return Header{Seq: binary.BigEndian.Uint64(b)}, len(b), nil
}

// TestWalkParallelBoundsRetainedBytes: with the visitor stalled on the
// first page, a walk holds the pages whose permits fill the pool and at
// most window = workers more fetched pages waiting for one; that is, what
// it retains is at most the pool plus window × maxPageBytes.
func TestWalkParallelBoundsRetainedBytes(t *testing.T) {
	const workers = 8
	const size = 4 << 20                                   // wire bytes; a permit is 4x
	const permitted = int64(maxInFlightBytes / (4 * size)) // pages the pool holds at once
	pages := &syntheticPages{last: 100, size: size}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stalled, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		first := true
		done <- WalkParallelWithGet(ctx, pages.get, testPrefix, 0, 0, workers, decodeSynthetic, nil, func(Entry[int]) error {
			if first {
				first = false
				close(stalled)
				<-resume
			}
			return nil
		})
	}()
	<-stalled
	// Let the walk run as far ahead as it can.
	for last := int64(-1); ; {
		time.Sleep(50 * time.Millisecond)
		n := pages.gets.Load()
		if n == last {
			break
		}
		last = n
	}
	if got, bound := pages.gets.Load(), permitted+workers; got > bound {
		t.Fatalf("%d pages fetched with the visitor stalled on the first; want at most %d (pool %d + window %d)", got, bound, permitted, workers)
	}
	close(resume)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestWalkParallelTailCost: an unbounded walk spends at most window (=
// workers) not-found GETs past the end of the log, even when those GETs
// are slow enough for every worker to start one.
func TestWalkParallelTailCost(t *testing.T) {
	const workers = 8
	pages := &syntheticPages{last: 3, size: 16, missDelay: 20 * time.Millisecond}
	visits := 0
	err := WalkParallelWithGet(context.Background(), pages.get, testPrefix, 0, 0, workers, decodeSynthetic, nil, func(Entry[int]) error {
		visits++
		return nil
	})
	if err != nil || visits != 3 {
		t.Fatalf("visits %d, err %v", visits, err)
	}
	if n := pages.missing.Load(); n < 1 || n > workers {
		t.Fatalf("%d not-found GETs past the end, want 1..%d", n, workers)
	}
}
