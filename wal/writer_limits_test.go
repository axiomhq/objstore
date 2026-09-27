package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore/storetest"
)

func TestWriterCommitsAtOnceWhenIdle(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	f.SetShape(storetest.Shape{Latency: 20 * time.Millisecond})
	const interval = time.Second
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	defer w.Close()
	start := time.Now()
	if err := w.Append(context.Background(), rows("one")); err != nil {
		t.Fatal(err)
	}
	// An idle writer PUTs at once instead of waiting out the interval. The
	// bound leaves room for a real store's PUT on top of the injected 20ms.
	if elapsed := time.Since(start); elapsed >= interval/2 {
		t.Fatalf("idle append took %s, want < %s", elapsed, interval/2)
	}
}

func TestWriterRateLimitsEntries(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(250*time.Millisecond))
	defer w.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	want := map[string]bool{}
	stop := time.Now().Add(1500 * time.Millisecond)
	for caller := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				id := fmt.Sprintf("%d-%d", caller, i)
				if err := w.Append(ctx, rows(id)); err != nil {
					t.Errorf("append %s: %v", id, err)
					return
				}
				mu.Lock()
				want[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if n := f.Ops()[storetest.OpPutIfAbsent]; n > 7 {
		t.Fatalf("%d WAL PUTs in 1.5 seconds at 250ms, want at most 7", n)
	}
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	last := make([]int, 8)
	for i := range last {
		last[i] = -1
	}
	for _, e := range entries {
		for _, id := range ids(e) {
			if seen[id] || !want[id] {
				t.Fatalf("duplicate or unexpected record %q", id)
			}
			seen[id] = true
			var caller, index int
			if _, err := fmt.Sscanf(id, "%d-%d", &caller, &index); err != nil {
				t.Fatal(err)
			}
			if index <= last[caller] {
				t.Fatalf("caller %d out of order: %d after %d", caller, index, last[caller])
			}
			last[caller] = index
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("replayed %d records, acknowledged %d", len(seen), len(want))
	}
}

func TestWriterCloseKeepsEntryRate(t *testing.T) {
	s := storetest.New(t)
	const interval = 100 * time.Millisecond
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	// The interval runs from the first PUT's start, so start the clock
	// before it: timed from Append's return, a slow store's PUT latency
	// came off the measured gap.
	start := time.Now()
	if err := w.Append(context.Background(), rows("first")); err != nil {
		t.Fatal(err)
	}
	receipt, err := w.Enqueue(context.Background(), rows("second"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if err := <-receipt; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < interval {
		t.Fatalf("Close drained next entry after %s, before rate limit", elapsed)
	}
}

func TestEntryBoundedByBytes(t *testing.T) {
	ctx := context.Background()
	t.Run("one_megabyte_records", func(t *testing.T) {
		s := storetest.New(t)
		w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		defer w.Close()
		records := make([]Bytes, 100)
		for i := range records {
			records[i] = filled(byte(i), 1<<20)
		}
		if err := w.Append(ctx, records); err != nil {
			t.Fatal(err)
		}
		keys, err := s.List(ctx, testPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) < 4 {
			t.Fatalf("100 MiB used only %d entries", len(keys))
		}
		for _, key := range keys {
			data, err := s.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > maxEntryBytes {
				t.Fatalf("%s is %d bytes, exceeds %d", key, len(data), maxEntryBytes)
			}
		}
	})
	t.Run("twenty_thousand_small_records", func(t *testing.T) {
		s := storetest.New(t)
		w := NewWriter[Bytes](s, testPrefix, 1, nil)
		defer w.Close()
		records := make([]Bytes, 20_000)
		for i := range records {
			records[i] = append(Bytes(fmt.Sprint(i)), filled('x', 150)...)
		}
		if err := w.Append(ctx, records); err != nil {
			t.Fatal(err)
		}
		keys, err := s.List(ctx, testPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 1 {
			t.Fatalf("20,000 small records made %d entries, want one", len(keys))
		}
	})
}

func TestWriterUnackedBoundRefuses(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(50*time.Millisecond))
	w.mu.Lock()
	w.unackedByteLimit = 1 << 20 // exercise the byte path without a 128 MiB fixture
	w.mu.Unlock()
	w.SetAttemptTimeout(250 * time.Millisecond)
	defer w.Close()
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Hang})
	record := []Bytes{filled('x', 400<<10)}
	first, err := w.Enqueue(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for f.Fired() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.Fired() == 0 {
		t.Fatal("first PUT did not enter the store")
	}
	second, err := w.Enqueue(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = w.Enqueue(context.Background(), record)
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("third enqueue: %v, want ErrOverloaded", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("overloaded enqueue blocked")
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}
