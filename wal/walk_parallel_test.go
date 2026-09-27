package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

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
