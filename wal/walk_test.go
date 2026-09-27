package wal

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/axiomhq/objstore/storetest"
)

func TestWalkVisitsEntriesInOrderAndStopsAtThrough(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	// Two single-page entries, one two-page batch, one more: seqs 1, 2, 3+4, 5.
	pages := []struct {
		h  Header
		id string
	}{
		{Header{Seq: 1}, "a"},
		{Header{Seq: 2}, "b"},
		{Header{Seq: 3, Nonce: "n1", BatchPages: 2, BatchIndex: 0}, "c"},
		{Header{Seq: 4, Nonce: "n1", BatchPages: 2, BatchIndex: 1}, "d"},
		{Header{Seq: 5}, "e"},
	}
	for _, p := range pages {
		if ok, err := put(ctx, s, testPrefix, p.h, Bytes(p.id)); err != nil || !ok {
			t.Fatalf("append seq %d: %v, %v", p.h.Seq, ok, err)
		}
	}

	got, err := collect(ctx, s, testPrefix, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Seq != 1 || got[1].Seq != 2 || got[2].Seq != 4 {
		t.Fatalf("walk visits: %+v", got)
	}
	// The two-page batch coalesces into one entry at its final page.
	if got[2].Incomplete || len(got[2].Pages) != 2 || !slices.Equal(ids(got[2]), []string{"c", "d"}) {
		t.Fatalf("coalesced batch: %+v", got[2])
	}
	// through=3 stops after the batch's first page: a marker for the
	// unfinished batch, never its partial records and never Incomplete.
	if got, err = collect(ctx, s, testPrefix, 0, 3); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2].Seq != 3 || got[2].BatchPages != 0 || got[2].Pages != nil || got[2].Incomplete {
		t.Fatalf("bounded walk excludes the unfinished batch: %+v", got)
	}
	// A missing page inside a bounded range is corruption, not end-of-log.
	if _, err := collect(ctx, s, testPrefix, 4, 6); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("bounded walk over a gap: %v", err)
	}
	// An unbounded walk stops at the first missing page.
	if got, err = collect(ctx, s, testPrefix, 5, 0); err != nil || len(got) != 0 {
		t.Fatalf("walk past end: %v, %+v", err, got)
	}
	// A visitor error aborts the walk.
	stop := errors.New("stop")
	got = nil
	err = Walk(ctx, s, testPrefix, 0, 0, Decode, func(e entry) error {
		got = append(got, e)
		return stop
	})
	if !errors.Is(err, stop) || len(got) != 1 {
		t.Fatalf("visitor error: %v, %d visited", err, len(got))
	}
}

// TestWalkStreamsOnePageAtATime pins the walker's memory contract: each
// entry is visited before the next page is read, so a walk never holds
// more than one page regardless of suffix length.
func TestWalkStreamsOnePageAtATime(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	const pages = 64
	for i := 0; i < pages; i++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: uint64(i + 1)}, Bytes{byte('a' + i%26)}); err != nil || !ok {
			t.Fatalf("append %d: %v, %v", i+1, ok, err)
		}
	}
	visited := 0
	var readAt []int // visits completed when each page was read
	err := WalkWithGet(ctx, func(ctx context.Context, key string) ([]byte, error) {
		readAt = append(readAt, visited)
		return s.Get(ctx, key)
	}, testPrefix, 0, 0, Decode, func(entry) error {
		visited++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if visited != pages || len(readAt) != pages+1 { // +1: the miss that ends the log
		t.Fatalf("visited %d entries over %d reads, want %d over %d", visited, len(readAt), pages, pages+1)
	}
	for i, n := range readAt {
		if n != i {
			t.Fatalf("page %d was read after %d visits, want %d: the walk read ahead", i+1, n, i)
		}
	}
}

func TestKeyAcceptsAnyPrefix(t *testing.T) {
	for _, prefix := range []string{"", "log/", "no-slash", "a/b/c-", "ns/x/wal/0"} {
		for _, seq := range []uint64{0, 1, 42, 1 << 40, ^uint64(0)} {
			key := Key(prefix, seq)
			if len(key) != len(prefix)+20 || !strings.HasPrefix(key, prefix) {
				t.Fatalf("Key(%q, %d) = %q", prefix, seq, key)
			}
			if got, err := SeqFromKey(key); err != nil || got != seq {
				t.Fatalf("SeqFromKey(%q) = %d, %v", key, got, err)
			}
		}
	}
	for _, bad := range []string{"", "log/42", "log/0000000000000000000x", "log/99999999999999999999"} {
		if _, err := SeqFromKey(bad); err == nil {
			t.Fatalf("SeqFromKey(%q) accepted", bad)
		}
	}
}
