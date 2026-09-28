package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestWriterCoalescesAndCommits(t *testing.T) {
	s := bucket.New(t)
	ctx := context.Background()
	var mu sync.Mutex
	var committed []string
	var seqs []uint64
	w := NewWriter(s, testPrefix, 1, func(seq uint64, at time.Time, records []Bytes) {
		mu.Lock()
		if at.IsZero() {
			t.Error("committed batch without a commit time")
		}
		seqs = append(seqs, seq)
		for _, r := range records {
			committed = append(committed, string(r))
		}
		mu.Unlock()
	})
	defer w.Close()

	const n = 10
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Append(ctx, rows(fmt.Sprintf("row-%d", i))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	// All records are durable and were delivered to onCommit before Append returned.
	mu.Lock()
	if len(committed) != n {
		t.Fatalf("onCommit saw %d records, want %d", len(committed), n)
	}
	mu.Unlock()
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, e := range entries {
		total += len(ids(e))
	}
	if total != n {
		t.Fatalf("replayed %d records, want %d", total, n)
	}
	// Coalescing: 10 near-simultaneous appends land in fewer than 10 entries.
	// (100ms window vs <1ms of appends; flaky only under extreme scheduler delay.)
	if len(entries) >= n {
		t.Fatalf("no coalescing: %d entries for %d appends", len(entries), n)
	}
	mu.Lock()
	for i, s := range seqs {
		if s != uint64(i)+1 {
			t.Fatalf("onCommit seqs not contiguous from 1: %v", seqs)
		}
	}
	mu.Unlock()
}

// assertContiguous is the invariant the walk depends on: the pages in the
// bucket are exactly 1..want, with no gap anywhere.
func assertContiguous(t *testing.T, s *objstore.Store, prefix string, want int) {
	t.Helper()
	keys, err := s.List(context.Background(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != want {
		t.Fatalf("%d pages, want %d: %v", len(keys), want, keys)
	}
	for i, k := range keys {
		seq, err := SeqFromKey(k)
		if err != nil || seq != uint64(i)+1 {
			t.Fatalf("page %d is %q (seq %d, %v): the log has a hole", i, k, seq, err)
		}
	}
}

// TestCrashedWriterLeavesNoHole is the construction the walk's "GET until
// the first miss" rests on. A writer claims sequentially and advances only
// on a PROVEN outcome, so a failed, ambiguous, fenced or abandoned batch
// leaves the log SHORT — never holey. Every way a batch can end badly is
// exercised, and after each one the pages in the bucket are still 1..n.
func TestCrashedWriterLeavesNoHole(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	ctx := context.Background()
	fast := WithCommitInterval(10 * time.Millisecond) // the cadence is not under test
	w := NewWriter[Bytes](s, testPrefix, 1, nil, fast)
	if err := w.Append(ctx, rows("a")); err != nil {
		t.Fatal(err)
	}
	// The PUT never reaches storage. The batch stays in flight and retries
	// the SAME sequence; burning the sequence instead would be the hole.
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Fail})
	if err := w.Append(ctx, rows("b")); err != nil {
		t.Fatalf("a retried batch must still commit: %v", err)
	}
	if f.Fired() != 1 {
		t.Fatal("the transport-failure plan never fired")
	}
	f.Clear()
	assertContiguous(t, s, testPrefix, 2)

	// The PUT lands and the response is lost: the retry reads its own nonce
	// back and adopts the entry rather than claiming the next sequence.
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Ambiguous})
	if err := w.Append(ctx, rows("c")); err != nil {
		t.Fatalf("an ambiguous PUT of our own batch must resolve: %v", err)
	}
	f.Clear()
	assertContiguous(t, s, testPrefix, 3)

	// A second writer on the same log: it loses at the contested sequence
	// and writes nothing above it.
	loser := NewWriter[Bytes](s, testPrefix, 3, nil, fast)
	if err := loser.Append(ctx, rows("z")); !errors.Is(err, ErrLostRace) {
		t.Fatalf("want ErrLostRace, got %v", err)
	}
	loser.Close()
	assertContiguous(t, s, testPrefix, 3)

	// The crash itself: the process goes away mid-life and another one
	// replays what is durable and continues from there.
	w.Close()
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 3 {
		t.Fatalf("replay after the crash: %+v (%v)", entries, err)
	}
	next := entries[len(entries)-1].Seq + 1
	w2 := NewWriter[Bytes](s, testPrefix, next, nil, fast)
	defer w2.Close()
	if err := w2.Append(ctx, rows("d")); err != nil {
		t.Fatal(err)
	}
	assertContiguous(t, s, testPrefix, 4)
	if entries, err = replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 4 {
		t.Fatalf("the restarted writer's page is not on the walk: %+v (%v)", entries, err)
	}
}

func TestWriterSplitBrain(t *testing.T) {
	s := bucket.New(t)
	ctx := context.Background()
	a := NewWriter[Bytes](s, testPrefix, 1, nil)
	defer a.Close()
	if err := a.Append(ctx, rows("from-a")); err != nil {
		t.Fatal(err)
	}
	// B believes it also owns seq 1 — its claim must fail loudly.
	b := NewWriter[Bytes](s, testPrefix, 1, nil)
	defer b.Close()
	err := b.Append(ctx, rows("from-b"))
	if !errors.Is(err, ErrLostRace) {
		t.Fatalf("want ErrLostRace, got %v", err)
	}
	// A's data is intact.
	entries, _ := replay(ctx, s, testPrefix, 0)
	if len(entries) != 1 || !slices.Equal(ids(entries[0]), []string{"from-a"}) {
		t.Fatalf("winner's data damaged: %+v", entries)
	}
}

func TestWriterAmbiguousPutOwnNonce(t *testing.T) {
	// Simulates a PUT that succeeded server-side while the response was
	// lost: the entry exists at nextSeq bearing THIS BATCH's nonce. commit
	// must treat it as committed — not split brain — and continue at seq+1.
	s := bucket.New(t)
	ctx := context.Background()
	prior := rows("already-durable")
	ok, err := put(ctx, s, testPrefix, Header{Seq: 1, Nonce: "batch-nonce-x"}, prior...)
	if err != nil || !ok {
		t.Fatalf("seed entry: ok=%v err=%v", ok, err)
	}
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	defer w.Close()
	w.mu.Lock()
	w.testNonce = "batch-nonce-x"
	w.mu.Unlock()
	if err := w.Append(ctx, prior); err != nil {
		t.Fatalf("ambiguous PUT with own batch nonce must succeed: %v", err)
	}
	if err := w.Append(ctx, rows("next")); err != nil {
		t.Fatalf("writer poisoned after ambiguity: %v", err)
	}
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 2 {
		t.Fatalf("replay: %+v, %v", entries, err)
	}
	if !slices.Equal(ids(entries[0]), []string{"already-durable"}) || !slices.Equal(ids(entries[1]), []string{"next"}) {
		t.Fatalf("history wrong: %+v", entries)
	}
}

// TestWriterCorruptReadbackIsUnknown: a corrupt page at the contested
// sequence proves nothing, so the batch is ErrUnresolved, and the writer is
// finished: it cannot tell which sequence is next, so every later Append
// gets ErrWriterFailed (never written, so not ErrUnresolved) without
// touching the store.
func TestWriterCorruptReadbackIsUnknown(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, Key(testPrefix, 1), []byte("corrupt page")); err != nil {
		t.Fatal(err)
	}
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
	defer w.Close()
	if err := w.Append(ctx, rows("a")); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("unreadable nonce cannot prove another writer won: %v", err)
	}
	f.ResetOps()
	if err := w.Append(ctx, rows("b")); !errors.Is(err, ErrWriterFailed) || errors.Is(err, ErrUnresolved) || !strings.Contains(err.Error(), "corrupt entry") {
		t.Fatalf("append after a corrupt read-back: %v, want ErrWriterFailed naming the cause", err)
	}
	if ops := f.Ops(); len(ops) != 0 {
		t.Fatalf("a terminal writer touched the store: %v", ops)
	}
	if st := w.Stats(); st.Lost || !errors.Is(st.Terminal, ErrUnresolved) || st.Unresolved != 1 || st.Pending != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// filled is a record of n bytes of c.
func filled(c byte, n int) Bytes { return Bytes(bytes.Repeat([]byte{c}, n)) }

func TestWriterSplitsAtomicBatchAndResolvesAmbiguousPages(t *testing.T) {
	for _, nth := range []int{1, 2} {
		t.Run(fmt.Sprint(nth), func(t *testing.T) {
			s, f := bucket.NewFaulty(t)
			f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: nth, Mode: storetest.Ambiguous, Key: testPrefix})
			w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(10*time.Millisecond))
			defer w.Close()
			// Each record is past the entry budget, together past one page.
			records := []Bytes{filled('x', 40<<20), filled('y', 40<<20)}
			if err := w.Append(context.Background(), records); err != nil {
				t.Fatal(err)
			}
			entries, err := replay(context.Background(), s, testPrefix, 0)
			if err != nil || len(entries) != 1 || entries[0].Seq != 2 || len(entries[0].Pages) != 2 || !sameRecords(entries[0], records) {
				t.Fatalf("replay: %d entries, %v", len(entries), err)
			}
		})
	}
}

// pageRecords re-types a decoded page's records for Encode.
func pageRecords(records [][]byte) []Bytes {
	out := make([]Bytes, len(records))
	for i, r := range records {
		out[i] = r
	}
	return out
}

// TestSplitBatchPagesAreCanonical: the pages splitBatch cuts must be exactly
// what Encode would write for each page, consecutively sequenced, with the
// batch's records in order, whether one record or several land on a page.
func TestSplitBatchPagesAreCanonical(t *testing.T) {
	var records []Bytes
	for i, size := range []int{20 << 20, 20 << 20, 30 << 20, 1 << 10, 40 << 20} {
		records = append(records, filled(byte('a'+i), size))
	}
	b := &batch[Bytes]{records: records, nonce: "nonce", at: time.UnixMilli(1234).UTC()}
	pages, _, err := splitBatch(7, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 3 {
		t.Fatalf("%d pages, want 3", len(pages))
	}
	var got []Bytes
	for i, page := range pages {
		if len(page) > maxPageBytes {
			t.Fatalf("page %d is %d bytes", i, len(page))
		}
		h, recs, err := Decode(page)
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		if h.Seq != 7+uint64(i) || h.BatchPages != 3 || h.BatchIndex != uint64(i) || h.Nonce != b.nonce || !h.At.Equal(b.at) {
			t.Fatalf("page %d header: %+v", i, h)
		}
		if canonical, _ := Encode(h, pageRecords(recs)); !bytes.Equal(canonical, page) {
			t.Fatalf("page %d differs from Encode", i)
		}
		got = append(got, pageRecords(recs)...)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatal("pages do not carry the batch's records in order")
	}
}

func TestSplitBatchSinglePageBytesMatchEncode(t *testing.T) {
	records := []Bytes{Bytes("plain"), Bytes{42, 0, 0x80, 0xff}, Bytes("<escaped>&"), Bytes{}}
	b := &batch[Bytes]{records: records, nonce: "canonical", at: time.UnixMilli(1234).UTC()}
	pages, _, err := splitBatch(7, b)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Encode(Header{Seq: 7, Nonce: b.nonce, At: b.at, BatchPages: 1}, records)
	if err != nil || len(pages) != 1 || !bytes.Equal(pages[0], want) {
		t.Fatalf("split bytes differ from Encode: pages=%d, err=%v", len(pages), err)
	}
}

// TestWriterRejectsOversizedRecordBeforePublish: a record larger than one
// page's budget must fail its batch before any page is PUT, wherever it
// sits in the batch — a page the decoder would reject must never be
// acknowledged.
func TestWriterRejectsOversizedRecordBeforePublish(t *testing.T) {
	small := func(id string) testRecord { return testRecord{b: id + strings.Repeat("x", 1<<10)} }
	huge := testRecord{size: maxPageBytes + 1} // Size alone; refused before AppendTo
	for name, records := range map[string][]testRecord{
		"small-then-oversized": {small("a"), huge},
		"oversized-middle":     {small("a"), huge, small("c")},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := fs.Open(t.TempDir(), "wal", objstore.Config{})
			if err := s.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			w := NewWriter(s, testPrefix, 1, func(_ uint64, _ time.Time, records []testRecord) {
				for _, r := range records {
					if r.Size() > maxPageBytes {
						t.Error("oversized batch was committed")
					}
				}
			})
			defer w.Close()
			err := w.Append(ctx, records)
			if err == nil || !strings.Contains(err.Error(), "exceeds page size limit") {
				t.Fatalf("append: %v, want page size error", err)
			}
			entries, err := replay(ctx, s, testPrefix, 0)
			if err != nil || len(entries) != 0 {
				t.Fatalf("WAL was published: %+v, %v", entries, err)
			}
			after := small("after")
			if err := w.Append(ctx, []testRecord{after}); err != nil {
				t.Fatalf("writer unusable after rejection: %v", err)
			}
			if entries, err := replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 1 || entries[0].Seq != 1 || !sameRecords(entries[0], []Bytes{Bytes(after.b)}) {
				t.Fatalf("rejected batch consumed a sequence: %+v, %v", entries, err)
			}
		})
	}
}

func TestRestartAfterCrashBetweenPagesAbandonsPartialBatch(t *testing.T) {
	s := bucket.New(t)
	ctx := context.Background()
	partial := Header{Seq: 1, Nonce: "crashed", BatchPages: 2, BatchIndex: 0}
	if ok, err := put(ctx, s, testPrefix, partial, Bytes("partial")); err != nil || !ok {
		t.Fatalf("seed partial page: %v, %v", ok, err)
	}
	w := NewWriter[Bytes](s, testPrefix, 2, nil)
	defer w.Close()
	if err := w.Append(ctx, rows("after-restart")); err != nil {
		t.Fatal(err)
	}
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 2 || entries[0].Pages != nil || !slices.Equal(ids(entries[1]), []string{"after-restart"}) {
		t.Fatalf("partial batch leaked or restart failed: %+v, %v", entries, err)
	}
}

func TestWriterUnresolvedOnClose(t *testing.T) {
	// Storage that never answers: a batch's outcome is unknowable. The
	// writer must neither lie nor hang — Close reports ErrUnresolved. The
	// first PUT hangs until its attempt deadline; every call after it fails.
	s, f := bucket.NewFaulty(t)
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Hang})
	f.SetShape(storetest.Shape{ErrorRate: 1, Seed: 1})
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(20*time.Millisecond))
	w.SetAttemptTimeout(20 * time.Millisecond)
	receipt, err := w.Enqueue(context.Background(), rows("limbo"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	select {
	case err := <-receipt:
		if !errors.Is(err, ErrUnresolved) {
			t.Fatalf("want ErrUnresolved, got %v", err)
		}
	default:
		t.Fatal("Close returned before answering the batch it could not resolve")
	}
	if f.Fired() != 1 {
		t.Fatal("the hang never fired: the writer never attempted a commit")
	}
	if st := w.Stats(); st.Unresolved != 1 {
		t.Fatalf("stats: %+v, want Unresolved=1", st)
	}
}

// TestWriterRejectsPastUnackedBound: a store that stopped acking must turn
// into fast, typed rejections, not into unbounded memory and callers who
// block forever. Past the bound Append refuses immediately, and the queue
// never grows past the bound it was given.
func TestWriterRejectsPastUnackedBound(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	const bound = 8
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(50*time.Millisecond))
	w.SetMaxUnacked(bound)
	// Bounds the hang below, and with it this test's Close: the wedged
	// attempt resolves at the deadline and the retry (the plan is spent by
	// then) succeeds.
	w.SetAttemptTimeout(time.Second)
	defer w.Close()

	// Wedge the writer mid-commit: nothing acks from here on.
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Hang, Key: testPrefix})
	acked := make(chan error, bound)
	go func() { acked <- w.Append(context.Background(), rows("wedge")) }()
	deadline := time.Now().Add(30 * time.Second)
	for f.Fired() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the planned hang never fired — the writer never attempted a commit")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Fill the queue behind the wedged batch, right up to the bound.
	for i := 0; i < bound-1; i++ {
		go func(i int) {
			acked <- w.Append(context.Background(), rows(fmt.Sprintf("q%d", i)))
		}(i)
	}
	for {
		st := w.Stats()
		if st.Pending > bound {
			t.Fatalf("pending %d grew past the bound %d", st.Pending, bound)
		}
		if st.Pending == bound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the queue never reached the bound: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	// The next append is refused, and refused PROMPTLY — that is the whole
	// point: an overloaded writer sheds instead of blocking.
	shed := make(chan error, 1)
	go func() { shed <- w.Append(context.Background(), rows("shed")) }()
	select {
	case err := <-shed:
		if !errors.Is(err, ErrOverloaded) {
			t.Fatalf("append past the bound: %v, want ErrOverloaded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append past the bound blocked instead of refusing")
	}
	st := w.Stats()
	if st.Pending != bound || st.Rejected != 1 || st.MaxUnacked != bound {
		t.Fatalf("stats after one shed: %+v, want Pending=%d Rejected=1 MaxUnacked=%d", st, bound, bound)
	}
	// Negative control: the bound refuses, it does not poison. Once the hang
	// resolves and the queue drains, the same writer accepts again.
	for i := 0; i < bound; i++ {
		if err := <-acked; err != nil {
			t.Fatalf("queued append: %v", err)
		}
	}
	if err := w.Append(context.Background(), rows("after")); err != nil {
		t.Fatalf("append after the queue drained: %v", err)
	}
}

// lineHandler collects log records so a test can count them.
type lineHandler struct {
	mu    sync.Mutex
	lines []string
}

func (h *lineHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *lineHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *lineHandler) WithGroup(string) slog.Handler            { return h }
func (h *lineHandler) Handle(_ context.Context, r slog.Record) error {
	line := r.Message
	r.Attrs(func(a slog.Attr) bool {
		line += " " + a.Key + "=" + a.Value.String()
		return true
	})
	h.mu.Lock()
	h.lines = append(h.lines, line)
	h.mu.Unlock()
	return nil
}

func (h *lineHandler) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.lines...)
}

// A store that refuses every PUT (an IAM or KMS misconfiguration) is, to a
// caller, indistinguishable from throttling: the batch stays in flight and
// is retried at the same seq with the same nonce — which is right. But the
// operator has to be told WHY: Stats names the step, key and cause of the
// newest failed attempt, the log says it once per window rather than once
// per tick, and the first commit that lands clears the record.
func TestWriterReportsBackendCause(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	h := &lineHandler{}
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(10*time.Millisecond))
	w.SetLogger(slog.New(h))
	defer w.Close()
	f.SetShape(storetest.Shape{ErrorRate: 1, Seed: 1}) // every call fails, until cleared

	done := make(chan error, 1)
	go func() { done <- w.Append(context.Background(), rows("a")) }()
	deadline := time.Now().Add(30 * time.Second)
	attempts := func() int { return f.Ops()[storetest.OpPutIfAbsent] }
	for attempts() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the writer never attempted a PUT")
		}
		time.Sleep(time.Millisecond)
	}
	// Within one attempt the cause is visible, with its age and key.
	st := w.Stats()
	le := st.LastError
	if le.At.IsZero() || le.Op != "put" || le.Key != Key(testPrefix, 1) || !errors.Is(le.Err, storetest.ErrFault) {
		t.Fatalf("Stats after a refused PUT does not name the cause: %+v", st)
	}
	if age := time.Since(le.At); age < 0 || age > 10*time.Second {
		t.Fatalf("LastError.At = %v (age %v) is not the attempt's time", le.At, age)
	}
	if st.Inflight != 1 || st.Pending != 1 {
		t.Fatalf("the refused batch must stay in flight: %+v", st)
	}
	// Several retries later there is still exactly one log line: the rate
	// window (errorLogEvery) is far longer than this loop.
	for attempts() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d attempts in 30s; the batch was not retried", attempts())
		}
		time.Sleep(time.Millisecond)
	}
	lines := h.snapshot()
	if len(lines) != 1 {
		t.Fatalf("%d log lines over %d failed attempts, want exactly 1 per window:\n%s", len(lines), attempts(), strings.Join(lines, "\n"))
	}
	for _, want := range []string{"op=put", "key=" + Key(testPrefix, 1), "err=" + storetest.ErrFault.Error()} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("log line %q lacks %q", lines[0], want)
		}
	}

	// The store recovers: the same batch lands and the record is gone.
	f.SetShape(storetest.Shape{})
	if err := <-done; err != nil {
		t.Fatalf("append after the store recovered: %v", err)
	}
	if st := w.Stats(); !st.LastError.At.IsZero() || st.Pending != 0 {
		t.Fatalf("a successful commit must clear the last error: %+v", st)
	}
	if got := len(h.snapshot()); got != 1 {
		t.Fatalf("recovery logged %d more lines", got-1)
	}
}

func TestWriterClosed(t *testing.T) {
	s := bucket.New(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	w.Close()
	if err := w.Append(context.Background(), rows("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("want ErrWriterClosed, got %v", err)
	}
}

func TestWriterEmptyAppend(t *testing.T) {
	// Regression: an empty append must return promptly, not strand a waiter
	// in a batch flush() skips.
	s := bucket.New(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- w.Append(context.Background(), nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("empty append: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("empty append hung — waiter stranded")
	}
	// Writer still functions afterward.
	if err := w.Append(context.Background(), rows("x")); err != nil {
		t.Fatal(err)
	}
}

func TestWriterKeepsBatchPendingUntilApplied(t *testing.T) {
	s := bucket.New(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	w := NewWriter(s, testPrefix, 1, func(uint64, time.Time, []Bytes) {
		close(entered)
		<-resume
	})
	t.Cleanup(w.Close)
	defer close(resume)
	receipt, err := w.Enqueue(context.Background(), rows("a"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not reach onCommit")
	}
	if st := w.Stats(); st.Pending != 1 || st.Inflight != 1 {
		t.Fatalf("durable but unapplied batch disappeared from pending state: %+v", st)
	}
	select {
	case err := <-receipt:
		t.Fatalf("acknowledged before application: %v", err)
	default:
	}
}

func TestWriterCloseDrains(t *testing.T) {
	// Regression: Close must not return until the final flush committed — a
	// caller may delete the prefix right after Close.
	s := bucket.New(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil)
	errCh, err := w.Enqueue(context.Background(), rows("last")) // accepted once Enqueue returns
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	// By the time Close returned, the batch must be durable.
	entries, err := replay(context.Background(), s, testPrefix, 0)
	if err != nil || len(entries) != 1 || !slices.Equal(ids(entries[0]), []string{"last"}) {
		t.Fatalf("final drain not durable at Close return: %+v, %v", entries, err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("parked append: %v", err)
	}
}

// TestCommitIntervalCoalescesIntoOnePage is the group-commit knob's contract:
// concurrent appends on an idle writer share an immediate entry. The next
// append is held until a full interval has passed since that commit.
func TestCommitIntervalCoalescesIntoOnePage(t *testing.T) {
	const interval = 300 * time.Millisecond
	s, f := bucket.NewFaulty(t)
	f.SetShape(storetest.Shape{Latency: 20 * time.Millisecond})
	ctx := context.Background()
	var mu sync.Mutex
	var committed []string
	var commits int
	w := NewWriter(s, testPrefix, 1, func(seq uint64, at time.Time, records []Bytes) {
		mu.Lock()
		commits++
		for _, r := range records {
			committed = append(committed, string(r))
		}
		mu.Unlock()
	}, WithCommitInterval(interval))
	defer w.Close()
	f.ResetOps()

	start := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, id := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.Append(ctx, rows(id))
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("idle concurrent appends took %s; want an immediate commit", elapsed)
	}
	mu.Lock()
	gotCommits, gotRecords := commits, append([]string(nil), committed...)
	mu.Unlock()
	if gotCommits != 1 {
		t.Fatalf("%d commits for two concurrent appends, want 1 (records %v)", gotCommits, gotRecords)
	}
	sort.Strings(gotRecords)
	if !reflect.DeepEqual(gotRecords, []string{"a", "b"}) {
		t.Fatalf("one commit carried %v, want both records", gotRecords)
	}
	// One durable PUT: the WAL page. Nothing else claims a sequence here.
	if n := f.Ops()[storetest.OpPutIfAbsent]; n != 1 {
		t.Fatalf("%d conditional PUTs for one batch, want 1", n)
	}
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 1 || len(ids(entries[0])) != 2 {
		t.Fatalf("two appends did not become one WAL entry: %+v, %v", entries, err)
	}
	start = time.Now()
	if err := w.Append(ctx, rows("c")); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < interval-50*time.Millisecond {
		t.Fatalf("next append committed after %s, before the %s rate limit", elapsed, interval)
	}
	entries, err = replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 2 || !slices.Equal(ids(entries[1]), []string{"c"}) {
		t.Fatalf("third append did not become the next entry: %+v, %v", entries, err)
	}
}

// TestWithCommitIntervalClampsValuesOutOfRange: the option is the writer's
// last line of defence. A caller should refuse a bad value up front; a
// writer handed one anyway keeps the default for <= 0 and clamps the rest,
// rather than spinning (below the minimum) or parking a lone write for a
// minute (above the maximum).
func TestWithCommitIntervalClampsValuesOutOfRange(t *testing.T) {
	for d, want := range map[time.Duration]time.Duration{
		0:                                   DefaultCommitInterval,
		-time.Second:                        DefaultCommitInterval,
		MinCommitInterval - time.Nanosecond: MinCommitInterval,
		MaxCommitInterval + time.Nanosecond: MaxCommitInterval,
		MinCommitInterval:                   MinCommitInterval,
		time.Second:                         time.Second,
		MaxCommitInterval:                   MaxCommitInterval,
	} {
		o := options{commitInterval: DefaultCommitInterval}
		WithCommitInterval(d)(&o)
		if o.commitInterval != want {
			t.Fatalf("WithCommitInterval(%s) set %s, want %s", d, o.commitInterval, want)
		}
	}
	w := NewWriter[Bytes](bucket.New(t), testPrefix, 1, nil, WithCommitInterval(200*time.Millisecond))
	defer w.Close()
	// Pacing precedes the attempt's deadline, so the deadline is not
	// raised to the commit interval.
	for d, want := range map[time.Duration]time.Duration{0: defaultAttemptTimeout, time.Millisecond: time.Millisecond, time.Minute: time.Minute} {
		w.SetAttemptTimeout(d)
		if w.attemptTimeout != want {
			t.Fatalf("SetAttemptTimeout(%s) set %s, want %s", d, w.attemptTimeout, want)
		}
	}
}

// TestDurableEntryIsOneConditionalPut is the WAL's request bill, pinned:
// the page IS the durable record and the floor oracle answers without a
// round trip while the owner's lease is fresh, so the happy path is ONE
// PutIfAbsent and nothing else.
//
// The floor is exercised too, in the shape a lapsed lease uses: an oracle
// that answers costs a round trip of its own and refuses a sequence the
// watermark already covers.
func TestDurableEntryIsOneConditionalPut(t *testing.T) {
	ctx := context.Background()
	s, f := bucket.NewFaulty(t)
	w := NewWriter[Bytes](s, testPrefix, 1, nil, WithCommitInterval(MinCommitInterval))
	defer w.Close()
	f.ResetOps()
	for i := range 4 {
		if err := w.Append(ctx, rows(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	ops := f.Ops()
	if ops[storetest.OpPutIfAbsent] != 4 {
		t.Fatalf("four entries took %d conditional PUTs, want 4 (%v)", ops[storetest.OpPutIfAbsent], ops)
	}
	for _, op := range []storetest.Op{storetest.OpGet, storetest.OpPut, storetest.OpPutIfMatch, storetest.OpList, storetest.OpDelete} {
		if ops[op] != 0 {
			t.Fatalf("the happy path took %d %v, want none (%v)", ops[op], op, ops)
		}
	}

	// A floor that answers: consulted on every claim, and a covered sequence
	// is refused rather than written.
	floorReads, sawRetry := 0, false
	w.SetFloor(func(ctx context.Context, retry bool) (uint64, error) {
		floorReads++
		sawRetry = sawRetry || retry
		return 100, nil
	})
	if err := w.Append(ctx, rows("covered")); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("a sequence under the watermark must be refused: %v", err)
	}
	if floorReads == 0 {
		t.Fatal("the floor oracle was not consulted once the writer asked for one")
	}
	if sawRetry {
		t.Fatal("a first attempt was reported to the oracle as a retry")
	}
}
