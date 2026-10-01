package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

var errBoom = errors.New("boom")

// testRecord is a Record that can misbehave: fail AppendTo, or report a
// Size (size, when non-zero) other than the bytes it appends.
type testRecord struct {
	b    string
	size int
	fail bool
}

// Weight is the record's length, what pageRecords gives a decoded record.
func (r testRecord) Weight() uint64 { return uint64(len(r.b)) }

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
			s, f := bucket.NewFaulty(t)
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
	s := bucket.New(t)
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

func TestBadRecordPreservesCause(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		b := &batch[testRecord]{records: []testRecord{{fail: true}}}
		if _, _, err := splitBatch(1, b); !errors.Is(err, ErrInvalidRecord) || !errors.Is(err, errBoom) {
			t.Fatalf("splitBatch: %v, want ErrInvalidRecord and boom", err)
		}
	})
	t.Run("writer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		w := NewWriter[testRecord](bucket.NewFS(t), testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		defer w.Close()
		if err := w.Append(ctx, []testRecord{{fail: true}}); !errors.Is(err, ErrInvalidRecord) || !errors.Is(err, errBoom) {
			t.Fatalf("Append: %v, want ErrInvalidRecord and boom", err)
		}
	})
}

func TestEnqueueRefusesOverflowingSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	// Admission alone: accepting the hostile size must not start encoding.
	w := &Writer[testRecord]{unackedByteLimit: maxUnackedBytes, kick: make(chan struct{}, 1)}
	if _, err := w.Enqueue(ctx, []testRecord{{size: math.MaxInt}}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Enqueue: %v, reserved bytes %d; want ErrRecordTooLarge", err, w.Stats().UnackedBytes)
	}
	if st := w.Stats(); st.Pending != 0 || st.UnackedBytes != 0 {
		t.Fatalf("refused record was queued: %+v", st)
	}
}

func TestReservedEntryBytesOverflow(t *testing.T) {
	for _, tc := range []struct{ bytes, records int }{{math.MaxInt, 1}, {0, math.MaxInt}} {
		if n := reservedEntryBytes(tc.bytes, tc.records); n != math.MaxInt {
			t.Fatalf("reservedEntryBytes(%d, %d) = %d, want saturation at math.MaxInt", tc.bytes, tc.records, n)
		}
	}
}

func TestWriterRefusesSequenceOverflow(t *testing.T) {
	for _, seq := range []uint64{0, math.MaxUint64} {
		t.Run(fmt.Sprint(seq), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			s, f := storetest.Faulty(t, bucket.NewFS(t))
			w := NewWriter[Bytes](s, testPrefix, seq, nil, WithCommitInterval(MinCommitInterval))
			defer w.Close()
			for range 2 {
				if err := w.Append(ctx, rows("not-written")); !errors.Is(err, ErrWriterFailed) || errors.Is(err, ErrUnresolved) {
					t.Fatalf("Append at sequence %d: %v, want ErrWriterFailed", seq, err)
				}
			}
			if ops := f.Ops(); len(ops) != 0 {
				t.Fatalf("refused sequence reached the store: %v", ops)
			}
			if st := w.Stats(); !errors.Is(st.Terminal, ErrWriterFailed) || st.Unresolved != 0 {
				t.Fatalf("stats: %+v", st)
			}
		})
	}
	t.Run("multi-page", func(t *testing.T) {
		b := &batch[testRecord]{records: named("r", 3, 1000), pageLimit: 1100}
		if pages, _, err := splitBatch(math.MaxUint64-1, b); !errors.Is(err, ErrWriterFailed) || pages != nil {
			t.Fatalf("splitBatch: %d pages, %v; want ErrWriterFailed and no pages", len(pages), err)
		}
	})
}

func TestWriterRechecksFloorAfterClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s := bucket.NewFS(t)
	var watermark atomic.Uint64
	watermark.Store(1)
	captured, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	w := NewWriter[Bytes](s, testPrefix, 2, nil, WithCommitInterval(MinCommitInterval))
	defer w.Close()
	// Release the oracle before Close, including on assertion failures.
	defer release.Do(func() { close(resume) })
	first := true
	w.SetFloor(func(ctx context.Context, retry bool) (uint64, error) {
		f := watermark.Load()
		if first {
			first = false
			close(captured)
			select {
			case <-resume:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		} else if !retry {
			return 0, errors.New("post-claim floor must request a fresh watermark")
		}
		return f, nil
	})
	receipt, err := w.Enqueue(ctx, rows("stale"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-captured:
	case <-ctx.Done():
		t.Fatal("floor was not read:", ctx.Err())
	}
	replacement := NewWriter[Bytes](s, testPrefix, 2, nil, WithCommitInterval(MinCommitInterval))
	defer replacement.Close()
	if err := replacement.Append(ctx, rows("checkpointed")); err != nil {
		t.Fatal(err)
	}
	watermark.Store(2)
	if err := s.Delete(ctx, Key(testPrefix, 2)); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(resume) })
	select {
	case err := <-receipt:
		if !errors.Is(err, ErrUnresolved) {
			t.Fatalf("stale claim below checkpoint: %v, want ErrUnresolved", err)
		}
	case <-ctx.Done():
		t.Fatal("stale claim did not finish:", ctx.Err())
	}
	if err := w.Append(ctx, rows("later")); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Append after covered claim: %v, want ErrWriterFailed", err)
	}
}

// TestEnqueueRefusesOversizedRecord: a record no page can hold, or one
// with a negative size, is refused synchronously: nothing is queued and
// the store is not touched.
func TestEnqueueRefusesOversizedRecord(t *testing.T) {
	s, f := bucket.NewFaulty(t)
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
	s, f := bucket.NewFaulty(t)
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
// Only the batch that lost gets (and counts) ErrLostRace; the rest were
// never written and get ErrWriterFailed.
func TestLostRaceLatches(t *testing.T) {
	ctx := context.Background()
	seedForeign := func(t *testing.T) (*Writer[Bytes], *storetest.Fault) {
		s, f := bucket.NewFaulty(t)
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
		if err := w.Append(ctx, rows("again")); !errors.Is(err, ErrWriterFailed) || errors.Is(err, ErrLostRace) || !strings.Contains(err.Error(), "split brain") {
			t.Fatalf("append after losing: %v, want ErrWriterFailed naming the split brain", err)
		}
		if ops := f.Ops(); len(ops) != 0 {
			t.Fatalf("a latched writer touched the store: %v", ops)
		}
		if st := w.Stats(); !st.Lost || !errors.Is(st.Terminal, ErrLostRace) || st.LostRace != 1 || st.Pending != 0 {
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
		if err := <-queued; !errors.Is(err, ErrWriterFailed) || errors.Is(err, ErrLostRace) || !strings.Contains(err.Error(), "split brain") {
			t.Fatalf("queued behind the losing batch: %v, want ErrWriterFailed naming the split brain", err)
		}
		if st := w.Stats(); !st.Lost || st.LostRace != 1 || st.Pending != 0 || st.Unresolved != 0 {
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
	// The file backend: the property is that the pages of one batch go out
	// back to back within one attempt, which does not depend on the store,
	// and a real S3 endpoint's PUT of a 17 MiB page can outlast the short
	// attempt used here.
	s, f := storetest.Faulty(t, bucket.NewFS(t))
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
	s, f := bucket.NewFaulty(t)
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

// TestPartialBatchFinishesWriterAndWalksAsMarker: a batch that fails for
// good after its first page landed finishes the writer, and leaves that
// page as an abandoned batch the walk reports as a marker, not corruption.
func TestPartialBatchFinishesWriterAndWalksAsMarker(t *testing.T) {
	ctx := context.Background()
	records := []Bytes{filled('a', 17<<20), filled('b', 17<<20)} // two pages
	t.Run("lost-race", func(t *testing.T) {
		s := bucket.New(t)
		if ok, err := put(ctx, s, testPrefix, Header{Seq: 2, Nonce: "other-writer"}, Bytes("theirs")); !ok || err != nil {
			t.Fatal(ok, err)
		}
		w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
		defer w.Close()
		if err := w.Append(ctx, records); !errors.Is(err, ErrLostRace) {
			t.Fatalf("append: %v, want ErrLostRace", err)
		}
		if err := w.Append(ctx, rows("next")); !errors.Is(err, ErrWriterFailed) {
			t.Fatalf("append after losing: %v, want ErrWriterFailed", err)
		}
		assertMarkerThen(t, s, "theirs")
	})
	// A covered floor is terminal: the writer cannot know the next sequence,
	// and a later batch could land under the watermark (acknowledged, never
	// replayed) or contend with its own landed page. The refused append
	// was never written: ErrWriterFailed, not counted as unresolved.
	t.Run("floor", func(t *testing.T) {
		s, f := bucket.NewFaulty(t)
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
		f.ResetOps()
		if err := w.Append(ctx, rows("next")); !errors.Is(err, ErrWriterFailed) || errors.Is(err, ErrUnresolved) || !strings.Contains(err.Error(), "watermark") {
			t.Fatalf("append after a covered floor: %v, want ErrWriterFailed naming the cause", err)
		}
		if ops := f.Ops(); len(ops) != 0 {
			t.Fatalf("a terminal writer touched the store: %v", ops)
		}
		if st := w.Stats(); st.Lost || !errors.Is(st.Terminal, ErrUnresolved) || st.Unresolved != 1 {
			t.Fatalf("stats: %+v", st)
		}
		entries, err := replay(ctx, s, testPrefix, 0)
		if err != nil || len(entries) != 1 || entries[0].Seq != 1 || !entries[0].Incomplete {
			t.Fatalf("replay: %+v, %v; want the landed page as an incomplete batch", entries, err)
		}
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
		s := bucket.New(t)
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

// TestStatsDuringDroppedRecordCommit: dropping a bad record's call from a
// batch updates the batch's records and bytes under the writer's lock, so
// Stats and Enqueue's admission polled throughout never race with the
// commit (run under -race), and the dropped records stop counting. The
// record bound of 1 keeps every probe refused until the writer is idle.
func TestStatsDuringDroppedRecordCommit(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	w := NewWriter[testRecord](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
	defer w.Close()
	first, release := wedge(t, w, f)
	ctx := context.Background()
	good, err := w.Enqueue(ctx, []testRecord{{b: "good"}})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := w.Enqueue(ctx, []testRecord{{b: "x"}, {b: "y", fail: true}, {b: "z"}})
	if err != nil {
		t.Fatal(err)
	}
	w.SetMaxUnacked(1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if st := w.Stats(); st.Pending > 5 || st.UnackedBytes < 0 {
				t.Errorf("stats: %+v", st)
				return
			}
			// Admission reads the in-flight batch's records and bytes.
			probe, err := w.Enqueue(ctx, []testRecord{{b: "probe"}})
			switch {
			case errors.Is(err, ErrOverloaded):
			case err == nil: // admitted: nothing else is held any more
				if err := <-probe; err != nil {
					t.Error(err)
				}
				return
			default:
				t.Error(err)
				return
			}
		}
	})
	// A probe is refused against the wedged batch.
	for deadline := time.Now().Add(10 * time.Second); w.Stats().Rejected == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			close(stop)
			wg.Wait()
			release()
			t.Fatal("no probe was refused against the wedged batch")
		}
	}
	release()
	errFirst, errGood, errBad := <-first, <-good, <-bad
	close(stop)
	wg.Wait()
	if errFirst != nil || errGood != nil || !errors.Is(errBad, ErrInvalidRecord) {
		t.Fatalf("verdicts: first %v, good %v, bad %v", errFirst, errGood, errBad)
	}
	if st := w.Stats(); st.Pending != 0 || st.UnackedBytes != 0 {
		t.Fatalf("stats after the commit: %+v", st)
	}
}

// TestEnqueueRefusesBatchPastUnackedBound: an append that alone exceeds a
// bound can never be admitted, so it is ErrRecordTooLarge, not the
// retryable ErrOverloaded, and it is not counted as shed load.
func TestEnqueueRefusesBatchPastUnackedBound(t *testing.T) {
	w := NewWriter[testRecord](bucket.New(t), testPrefix, 1, nil)
	defer w.Close()
	huge := []testRecord{{size: 50 << 20}, {size: 50 << 20}, {size: 50 << 20}} // each fits a page; together past the byte bound
	if _, err := w.Enqueue(context.Background(), huge); !errors.Is(err, ErrRecordTooLarge) || errors.Is(err, ErrOverloaded) ||
		!strings.Contains(err.Error(), "batch exceeds the unacked bound") {
		t.Fatalf("past the byte bound: %v, want ErrRecordTooLarge", err)
	}
	w.SetMaxUnacked(2)
	if _, err := w.Enqueue(context.Background(), []testRecord{{b: "1"}, {b: "2"}, {b: "3"}}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("past the record bound: %v, want ErrRecordTooLarge", err)
	}
	if st := w.Stats(); st.Rejected != 0 || st.Pending != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// splitCalls builds a batch of calls, each a group of records, and splits
// it; want is the records of the calls that encode.
func splitCalls(t *testing.T, pageLimit int, groups ...[]testRecord) (pages [][]byte, kept []testRecord, b *batch[testRecord]) {
	t.Helper()
	b = &batch[testRecord]{nonce: "nonce", at: time.UnixMilli(1234).UTC(), pageLimit: pageLimit}
	for _, g := range groups {
		b.records = append(b.records, g...)
		b.calls = append(b.calls, call{n: len(g)})
	}
	pages, kept, err := splitBatch(7, b)
	if err != nil {
		t.Fatal(err)
	}
	return pages, kept, b
}

// checkPages decodes pages, checks each is exactly what Encode writes for
// its header and records, and that together they carry want in order.
func checkPages(t *testing.T, pages [][]byte, want []testRecord) {
	t.Helper()
	var got []string
	for i, page := range pages {
		h, recs, err := Decode(page)
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		if h.Seq != 7+uint64(i) || h.BatchIndex != uint64(i) || (len(pages) > 1 && h.BatchPages != uint64(len(pages))) {
			t.Fatalf("page %d header: %+v", i, h)
		}
		if canonical, _ := Encode(h, pageRecords(recs)); !bytes.Equal(canonical, page) {
			t.Fatalf("page %d differs from Encode", i)
		}
		for _, r := range recs {
			got = append(got, string(r))
		}
	}
	var wantIDs []string
	for _, r := range want {
		wantIDs = append(wantIDs, r.b)
	}
	if !slices.Equal(got, wantIDs) {
		t.Fatalf("pages carry %d records, want %d", len(got), len(wantIDs))
	}
}

func named(prefix string, n, size int) []testRecord {
	out := make([]testRecord, n)
	for i := range out {
		id := fmt.Sprintf("%s-%d-", prefix, i)
		out[i] = testRecord{b: id + strings.Repeat("x", max(size-len(id), 0))}
	}
	return out
}

// TestSplitBatchDropShrinksCountVarint: 200 records need a two-byte count;
// dropping a call leaves 100, a one-byte count. The page is still exactly
// what Encode writes.
func TestSplitBatchDropShrinksCountVarint(t *testing.T) {
	good := named("good", 100, 8)
	bad := append(named("bad", 99, 8), testRecord{b: "boom", fail: true})
	pages, kept, b := splitCalls(t, 0, good, bad)
	if len(pages) != 1 || len(kept) != 100 || len(b.records) != 200 {
		t.Fatalf("%d pages, %d kept, batch now %d records (want 1, 100, and the batch untouched)", len(pages), len(kept), len(b.records))
	}
	if !errors.Is(b.calls[1].err, ErrInvalidRecord) || b.calls[0].err != nil {
		t.Fatalf("call verdicts: %v, %v", b.calls[0].err, b.calls[1].err)
	}
	checkPages(t, pages, good)
}

// TestSplitBatchDropInsideMultiPageBatch: a dropped call in the middle of
// a batch that spans several pages leaves canonical pages carrying the
// other calls' records in order.
func TestSplitBatchDropInsideMultiPageBatch(t *testing.T) {
	a, c := named("a", 6, 1000), named("c", 6, 1000)
	bad := append(named("b", 3, 1000), testRecord{b: "boom", size: 7}) // Size disagrees with AppendTo
	pages, kept, _ := splitCalls(t, 4096, a, bad, c)
	if len(pages) < 3 || len(kept) != 12 {
		t.Fatalf("%d pages, %d kept; want several pages and 12 records", len(pages), len(kept))
	}
	checkPages(t, pages, append(slices.Clone(a), c...))
}

// TestSplitBatchRefusesRecordLargerThanAnyPage: a record past every
// page's budget fails the batch before any page exists, wherever it sits.
func TestSplitBatchRefusesRecordLargerThanAnyPage(t *testing.T) {
	for name, at := range map[string]int{"first": 0, "middle": 2, "last": 4} {
		t.Run(name, func(t *testing.T) {
			records := named("r", 5, 100)
			records[at] = testRecord{b: strings.Repeat("h", 2000)}
			b := &batch[testRecord]{records: records, nonce: "nonce", pageLimit: 1024}
			if pages, _, err := splitBatch(7, b); !errors.Is(err, ErrRecordTooLarge) || pages != nil {
				t.Fatalf("%d pages, %v; want ErrRecordTooLarge and no pages", len(pages), err)
			}
		})
	}
}

// TestCloseDrainPacesRetries: the final drain calls flush directly, not
// through the loop's timer, so commit itself waits out the retry pacing: a
// store refusing every PUT gets one attempt per commit interval from the
// drain, not two back to back.
func TestCloseDrainPacesRetries(t *testing.T) {
	const interval = 200 * time.Millisecond
	s, f := bucket.NewFaulty(t)
	f.SetShape(storetest.Shape{ErrorRate: 1, Seed: 1})
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(interval))
	receipt, err := w.Enqueue(context.Background(), rows("refused"))
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); w.Stats().LastError.At.IsZero(); {
		if time.Now().After(deadline) {
			t.Fatal("the first attempt never failed")
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	w.Close()
	if elapsed := time.Since(start); elapsed < 2*interval-50*time.Millisecond {
		t.Fatalf("Close took %s: the drain retried without waiting an interval", elapsed)
	}
	if err := <-receipt; !errors.Is(err, ErrUnresolved) {
		t.Fatalf("receipt: %v, want ErrUnresolved", err)
	}
}
