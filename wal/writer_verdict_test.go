package wal

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

var errBoom = errors.New("boom")

// testRecord is a Record that can misbehave: fail AppendTo, or report a
// Size (size, when non-zero) other than the bytes it appends.
type testRecord struct {
	b    string
	size int
	fail bool
}

func (r testRecord) Size() int {
	if r.size != 0 {
		return r.size
	}
	return len(r.b)
}

func (r testRecord) AppendTo(dst []byte) ([]byte, error) {
	if r.fail {
		return nil, errBoom
	}
	return append(dst, r.b...), nil
}

// wedge holds the writer's next PUT until the returned release is called,
// so that what is enqueued meanwhile forms one batch behind it.
func wedge(t *testing.T, w interface {
	Enqueue(context.Context, []testRecord) (<-chan error, error)
}, f *storetest.Fault) (receipt <-chan error, release func()) {
	t.Helper()
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Pause})
	receipt, err := w.Enqueue(context.Background(), []testRecord{{b: "wedge"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFired(t, f)
	return receipt, f.Resume
}

func waitFired(t *testing.T, f *storetest.Fault) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); f.Fired() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the planned fault never fired")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestBadRecordFailsOnlyItsCall: a record that fails to encode fails the
// Append that carried it, not the others coalesced into the same batch.
func TestBadRecordFailsOnlyItsCall(t *testing.T) {
	for name, bad := range map[string]testRecord{
		"append-error":  {b: "bad", fail: true},
		"size-mismatch": {b: "bad", size: 7},
	} {
		t.Run(name, func(t *testing.T) {
			s, f := storetest.NewFaulty(t)
			var mu sync.Mutex
			var applied []string
			w := NewWriter(s, testPrefix, 1, func(_ uint64, _ time.Time, records []testRecord) {
				mu.Lock()
				defer mu.Unlock()
				for _, r := range records {
					applied = append(applied, r.b)
				}
			}, WithCommitInterval(MinCommitInterval))
			defer w.Close()
			first, release := wedge(t, w, f)
			ctx := context.Background()
			a, errA := w.Enqueue(ctx, []testRecord{{b: "a1"}, {b: "a2"}})
			b, errB := w.Enqueue(ctx, []testRecord{{b: "b1"}, bad})
			c, errC := w.Enqueue(ctx, []testRecord{{b: "c1"}})
			if err := errors.Join(errA, errB, errC); err != nil {
				t.Fatal(err)
			}
			release()
			if err := errors.Join(<-first, <-a, <-c); err != nil {
				t.Fatalf("good calls failed with the bad one: %v", err)
			}
			if err := <-b; !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("bad call: %v, want ErrInvalidRecord", err)
			}
			entries, err := replay(ctx, s, testPrefix, 0)
			if err != nil || len(entries) != 2 || !slices.Equal(ids(entries[1]), []string{"a1", "a2", "c1"}) {
				t.Fatalf("replay: %+v, %v", entries, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(applied, []string{"wedge", "a1", "a2", "c1"}) {
				t.Fatalf("onCommit applied %v", applied)
			}
		})
	}
}

// TestBadRecordAloneClaimsNothing: a batch whose every call failed to
// encode claims no sequence; the next batch takes it.
func TestBadRecordAloneClaimsNothing(t *testing.T) {
	s := storetest.New(t)
	w := NewWriter[testRecord](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
	defer w.Close()
	ctx := context.Background()
	if err := w.Append(ctx, []testRecord{{b: "x", fail: true}}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("append: %v, want ErrInvalidRecord", err)
	}
	if err := w.Append(ctx, []testRecord{{b: "ok"}}); err != nil {
		t.Fatal(err)
	}
	if entries, err := replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 1 || entries[0].Seq != 1 {
		t.Fatalf("replay: %+v, %v", entries, err)
	}
}

// TestEnqueueRefusesOversizedRecord: a record no page can hold, or one
// with a negative size, is refused synchronously: nothing is queued and
// the store is not touched.
func TestEnqueueRefusesOversizedRecord(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	w := NewWriter[testRecord](s, testPrefix, 1, nil)
	defer w.Close()
	f.ResetOps()
	for _, tc := range []struct {
		records []testRecord
		want    error
	}{
		{[]testRecord{{b: "ok"}, {size: maxPageBytes}}, ErrRecordTooLarge},
		{[]testRecord{{size: -1}}, ErrInvalidRecord},
	} {
		if _, err := w.Enqueue(context.Background(), tc.records); !errors.Is(err, tc.want) {
			t.Fatalf("enqueue: %v, want %v", err, tc.want)
		}
	}
	if !strings.Contains(ErrRecordTooLarge.Error(), "exceeds page size limit") {
		t.Fatal("ErrRecordTooLarge text changed")
	}
	if ops := f.Ops(); len(ops) != 0 {
		t.Fatalf("a refused enqueue reached the store: %v", ops)
	}
	if st := w.Stats(); st.Pending != 0 || st.Rejected != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestMaxUnackedCountsIncomingRecords: the record bound counts the records
// being enqueued, not only those already held.
func TestMaxUnackedCountsIncomingRecords(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	w := NewWriter[testRecord](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
	w.SetMaxUnacked(3)
	defer w.Close()
	first, release := wedge(t, w, f)
	defer func() { release(); <-first }()
	if _, err := w.Enqueue(context.Background(), []testRecord{{b: "1"}, {b: "2"}, {b: "3"}}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("1 held + 3 incoming over a bound of 3: %v, want ErrOverloaded", err)
	}
	if _, err := w.Enqueue(context.Background(), []testRecord{{b: "1"}, {b: "2"}}); err != nil {
		t.Fatalf("1 held + 2 incoming within a bound of 3: %v", err)
	}
}

// TestLostRaceLatches: once another writer is proven to own the log, the
// writer refuses everything, queued or new, without touching the store.
func TestLostRaceLatches(t *testing.T) {
	ctx := context.Background()
	seedForeign := func(t *testing.T) (*Writer[Bytes], *storetest.Fault) {
		s, f := storetest.NewFaulty(t)
		if ok, err := put(ctx, s, testPrefix, Header{Seq: 1, Nonce: "other-writer"}, Bytes("theirs")); !ok || err != nil {
			t.Fatal(ok, err)
		}
		w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		t.Cleanup(w.Close)
		return w, f
	}
	t.Run("later", func(t *testing.T) {
		w, f := seedForeign(t)
		if err := w.Append(ctx, rows("mine")); !errors.Is(err, ErrLostRace) {
			t.Fatalf("append: %v, want ErrLostRace", err)
		}
		f.ResetOps()
		if err := w.Append(ctx, rows("again")); !errors.Is(err, ErrLostRace) {
			t.Fatalf("append after losing: %v, want ErrLostRace", err)
		}
		if ops := f.Ops(); len(ops) != 0 {
			t.Fatalf("a latched writer touched the store: %v", ops)
		}
		if st := w.Stats(); !st.Lost || st.LostRace != 1 || st.Pending != 0 {
			t.Fatalf("stats: %+v", st)
		}
	})
	t.Run("queued", func(t *testing.T) {
		w, f := seedForeign(t)
		f.Set(storetest.Plan{Op: storetest.OpGet, N: 1, Mode: storetest.Pause}) // the losing read-back
		first, err := w.Enqueue(ctx, rows("mine"))
		if err != nil {
			t.Fatal(err)
		}
		waitFired(t, f)
		queued, err := w.Enqueue(ctx, rows("queued"))
		if err != nil {
			t.Fatal(err)
		}
		f.Resume()
		if err := <-first; !errors.Is(err, ErrLostRace) {
			t.Fatalf("losing batch: %v", err)
		}
		if err := <-queued; !errors.Is(err, ErrLostRace) {
			t.Fatalf("queued behind the losing batch: %v, want ErrLostRace", err)
		}
		if st := w.Stats(); !st.Lost || st.LostRace != 2 || st.Pending != 0 {
			t.Fatalf("stats: %+v", st)
		}
	})
}

// TestMultiPageBatchWithinOneAttempt: a batch's pages are one entry and are
// paced once, so a multi-page batch fits an attempt no longer than the
// commit interval (per-page pacing needed two intervals and retried
// forever).
func TestMultiPageBatchWithinOneAttempt(t *testing.T) {
	const interval = 500 * time.Millisecond
	s, f := storetest.NewFaulty(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	w.SetAttemptTimeout(interval)
	defer w.Close()
	f.ResetOps()
	start := time.Now()
	records := []Bytes{filled('a', 17<<20), filled('b', 17<<20), filled('c', 17<<20)}
	if err := w.Append(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("three pages took %s", elapsed)
	}
	if ops := f.Ops(); ops[storetest.OpPutIfAbsent] != 3 || ops[storetest.OpGet] != 0 {
		t.Fatalf("ops %v, want 3 conditional PUTs and no read-back", ops)
	}
}

// TestAdoptionDoesNotRearmPacing: adopting our own earlier PUT starts no
// new entry, so it does not hold the next batch back another interval.
func TestAdoptionDoesNotRearmPacing(t *testing.T) {
	const interval = 500 * time.Millisecond
	s, f := storetest.NewFaulty(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	defer w.Close()
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Ambiguous})
	ctx := context.Background()
	start := time.Now()
	if err := w.Append(ctx, rows("a")); err != nil { // lands, answer lost; adopted at the retry
		t.Fatal(err)
	}
	if err := w.Append(ctx, rows("b")); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > interval+250*time.Millisecond {
		t.Fatalf("second append landed after %s; adoption re-armed pacing", elapsed)
	}
	if entries, err := replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 2 {
		t.Fatalf("replay: %+v, %v", entries, err)
	}
}

// TestPartialBatchAdvancesPastLandedPages: a batch that fails for good after
// its first page landed leaves that page as an abandoned batch; the writer
// moves past it rather than contending with its own page, and the walk
// reports a marker, not corruption.
func TestPartialBatchAdvancesPastLandedPages(t *testing.T) {
	ctx := context.Background()
	records := []Bytes{filled('a', 17<<20), filled('b', 17<<20)} // two pages
	t.Run("lost-race", func(t *testing.T) {
		s := storetest.New(t)
		if ok, err := put(ctx, s, testPrefix, Header{Seq: 2, Nonce: "other-writer"}, Bytes("theirs")); !ok || err != nil {
			t.Fatal(ok, err)
		}
		w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		defer w.Close()
		if err := w.Append(ctx, records); !errors.Is(err, ErrLostRace) {
			t.Fatalf("append: %v, want ErrLostRace", err)
		}
		if w.nextSeq != 2 {
			t.Fatalf("nextSeq %d, want 2: past the landed page", w.nextSeq)
		}
		assertMarkerThen(t, s, "theirs")
	})
	t.Run("floor", func(t *testing.T) {
		s := storetest.New(t)
		w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		defer w.Close()
		var mu sync.Mutex
		calls := 0
		w.SetFloor(func(context.Context, bool) (uint64, error) {
			mu.Lock()
			defer mu.Unlock()
			if calls++; calls == 2 {
				return 100, nil // the second page's claim is covered
			}
			return 0, nil
		})
		if err := w.Append(ctx, records); !errors.Is(err, ErrUnresolved) {
			t.Fatalf("append: %v, want ErrUnresolved", err)
		}
		if err := w.Append(ctx, rows("next")); err != nil {
			t.Fatalf("the next batch contended with the writer's own page: %v", err)
		}
		assertMarkerThen(t, s, "next")
	})
}

// assertMarkerThen checks the log is an abandoned batch at seq 1 and then
// a single-page entry at seq 2 holding id.
func assertMarkerThen(t *testing.T, s *objstore.Store, id string) {
	t.Helper()
	entries, err := replay(context.Background(), s, testPrefix, 0)
	if err != nil || len(entries) != 2 {
		t.Fatalf("replay: %+v, %v", entries, err)
	}
	if entries[0].Seq != 1 || entries[0].Pages != nil || entries[0].Incomplete {
		t.Fatalf("seq 1 is not an abandoned-batch marker: %+v", entries[0].Header)
	}
	if entries[1].Seq != 2 || !slices.Equal(ids(entries[1]), []string{id}) {
		t.Fatalf("seq 2: %+v", entries[1].Header)
	}
}

// TestSinglePageAfterAbandonedBatch: a single page is BatchPages 0 (Encode)
// or 1 (Writer); after an abandoned batch either one ends it with a marker.
func TestSinglePageAfterAbandonedBatch(t *testing.T) {
	for _, pages := range []uint64{0, 1} {
		s := storetest.New(t)
		ctx := context.Background()
		if ok, err := put(ctx, s, testPrefix, Header{Seq: 1, Nonce: "abandoned", BatchPages: 3}, Bytes("x")); !ok || err != nil {
			t.Fatal(ok, err)
		}
		if ok, err := put(ctx, s, testPrefix, Header{Seq: 2, Nonce: "single", BatchPages: pages}, Bytes("y")); !ok || err != nil {
			t.Fatal(ok, err)
		}
		entries, err := replay(ctx, s, testPrefix, 0)
		if err != nil || len(entries) != 2 || entries[0].Pages != nil || !slices.Equal(ids(entries[1]), []string{"y"}) {
			t.Fatalf("BatchPages %d: %+v, %v", pages, entries, err)
		}
	}
}

func TestWriteErrorIsAnError(t *testing.T) {
	var err error = WriteError{At: time.Unix(0, 0).UTC(), Op: "put", Key: "log/1", Err: errBoom}
	if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "put log/1") {
		t.Fatalf("%v", err)
	}
}
