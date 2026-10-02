package wal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/axiomhq/objstore"
)

const (
	// DefaultCommitInterval limits WAL entry starts per writer: one entry
	// per second. An idle writer commits at once; later writes coalesce
	// until the next entry is eligible, so concurrent writers share one
	// entry. The pages of one multi-page batch are one entry: they go out
	// back to back.
	DefaultCommitInterval = time.Second
	// A scheduler-sized window lets concurrent idle callers join the first
	// entry without adding a meaningful delay to an uncontended append.
	idleCoalesceDelay = time.Millisecond
	// Encode a waiting batch shortly before its next PUT slot. Encoding a
	// large page only after the slot opens reduces useful entry throughput.
	precommitPrepareLead = 100 * time.Millisecond
	// MinCommitInterval/MaxCommitInterval bound WithCommitInterval. Below the
	// minimum retries can spin without coalescing; above the maximum queued
	// writes and Close's final drain stop being prompt.
	MinCommitInterval = time.Millisecond
	MaxCommitInterval = 5 * time.Second

	maxEntryBytes = 32 << 20 // encoded WAL page, including header and CRC
	// maxUnackedBytes is the byte admission bound. It holds four entries:
	// one batch can be in flight while three more queue behind it, which
	// covers a burst arriving during a single slow commit. Past that Append
	// refuses instead of buffering more. SetMaxUnacked adds a separate
	// record-count bound.
	maxUnackedBytes = 4 * maxEntryBytes
	// defaultAttemptTimeout bounds ONE commit attempt (see Writer.commit). A
	// batch is shared, so no caller's context may abort it — but a hung store
	// must not pin the flush goroutine, and therefore Close, forever.
	defaultAttemptTimeout = 30 * time.Second
	// errorLogEvery rate-limits the "commit attempt failed" log per writer:
	// a persistent AccessDenied retried every commit interval is one line per
	// window, not ten a second. Stats carries the current cause regardless.
	errorLogEvery = 10 * time.Second
)

var (
	// ErrWriterClosed refuses an Append after Close, and answers one Close
	// found queued but never attempted.
	ErrWriterClosed = errors.New("wal: writer closed")
	// ErrLostRace means another writer's entry occupies our next sequence
	// number: we are no longer the sole writer. Proven by nonce mismatch on
	// read-back — never a guess. Fail loudly, never diverge silently. The
	// writer is terminal from then on: every queued and later Append gets
	// ErrWriterFailed. Reopen from the checkpoint with a new Writer.
	ErrLostRace = errors.New("wal: sequence claimed by another writer (split brain)")
	// ErrUnresolved means durability could not be attributed: the batch may
	// already be durable. Reopen from the checkpoint and the remaining WAL.
	// It only ever answers a batch that claimed a sequence. From a commit (a
	// claim the floor already covers, or a corrupt page at the contested
	// sequence) it is terminal like ErrLostRace: the writer cannot tell
	// which sequence is next, so it is finished; every queued and later
	// Append gets ErrWriterFailed, and Stats.Terminal says why. From Close
	// it answers the batch the final drain could not resolve.
	ErrUnresolved = errors.New("wal: write outcome unknown; batch may already be committed")
	// ErrWriterFailed answers an Append the writer refused because it had
	// already finished (Stats.Terminal): queued behind the batch that
	// finished it, or enqueued later. It also refuses a batch whose sequence
	// range would exhaust the writer. Its records were not and will not be
	// written. The error names the terminal cause, but errors.Is matches
	// only ErrWriterFailed: ErrLostRace and ErrUnresolved answer the batch
	// that made the claim.
	ErrWriterFailed = errors.New("wal: writer finished; not written")
	// ErrOverloaded means this writer already holds maxUnackedBytes the
	// store has not acked: the queue is full, so the write is REFUSED rather
	// than queued behind a store that is not keeping up. Immediate and typed
	// — an overloaded process must degrade into fast rejections, never into
	// unbounded memory. Retryable: the caller may come back.
	ErrOverloaded = errors.New("wal: writer overloaded (unacked queue full)")
	// ErrRecordTooLarge refuses, at Enqueue, a record no page can hold, or
	// an append larger than the unacked bound, which no queue state admits.
	ErrRecordTooLarge = errors.New("wal: record exceeds page size limit")
	// ErrInvalidRecord refuses a record whose Size is negative (at Enqueue)
	// or whose AppendTo fails or disagrees with Size (at commit, failing
	// only the Append that carried it), or whose weight overflows its page.
	ErrInvalidRecord = errors.New("wal: invalid record")

	// errRetry is internal: this batch's outcome is unresolved; keep it in
	// flight and retry the same seq with the same nonce on the next tick.
	errRetry = errors.New("wal: unresolved, retrying")
)

// WriteError is a writer's most recent failed commit attempt: which step
// refused, on which key, what the backend said, and when. It is the
// diagnosis errRetry deliberately hides from callers (a batch is shared, and
// its outcome is unresolved, not failed): a persistent IAM or KMS refusal
// looks exactly like throttling from Pending/OldestPending alone. Bounded to
// one record, cause only — never a payload. Cleared by the next commit that
// succeeds.
type WriteError struct {
	At  time.Time
	Op  string // floor | put | readback
	Key string // the WAL key the step was claiming or verifying
	Err error
}

func (e WriteError) Error() string {
	return fmt.Sprintf("wal: %s %s at %s: %v", e.Op, e.Key, e.At.Format(time.RFC3339Nano), e.Err)
}

func (e WriteError) Unwrap() error { return e.Err }

// call is one Enqueue: its receipt, how many of the batch's records are its
// own, and its own verdict when one of its records failed to encode (the
// rest of the batch still commits).
type call struct {
	ch  chan error
	n   int
	err error
}

// deliver sends each call its own error, or verdict when it has none.
func deliver(calls []call, verdict error) {
	for _, c := range calls {
		if c.err != nil {
			c.ch <- c.err
		} else {
			c.ch <- verdict
		}
	}
}

// live counts the calls whose verdict is the batch's.
func live(calls []call) int64 {
	var n int64
	for _, c := range calls {
		if c.err == nil {
			n++
		}
	}
	return n
}

// entryHeaderReserve is a page header with a minted (32-byte) nonce.
var entryHeaderReserve = headerReserve(32)

// batch is one coalesced group of records bound for a single WAL entry. Its
// nonce is minted once and never reused: a read-back returning this nonce
// proves THIS batch (not merely this writer) is durable at the contested seq.
type batch[R Record] struct {
	records   []R
	bytes     int // framed records plus a conservative header reserve
	pageLimit int // zero means the codec's page limit (standalone callers)
	calls     []call
	nonce     string
	// enqueued is when the OLDEST record in this batch was accepted, carried
	// over from pendingSince. It is what "how old is the oldest un-acked
	// write" means: a batch retrying against a hung store keeps it.
	enqueued time.Time
	// at is the commit time written into the page, fixed when the batch is
	// promoted to flight so retries encode identical bytes. Millisecond
	// precision: what the page stores is what replay reads back.
	at time.Time
	// pages are the batch's encoded WAL pages, cut once on the first commit
	// attempt at the seq that attempt claims; retries PUT the same bytes.
	pages [][]byte
	// attempts counts commit attempts made for this batch. Past the first,
	// the batch's own pages may already be durable and already truncated, which
	// is what the floor oracle is told (see Writer.floor).
	attempts int
	// landed is how many leading pages are proven durable (ours, won or
	// adopted). A retry resumes after them. A terminal failure leaves them
	// as an abandoned batch; the writer is finished, so nothing contends
	// with them.
	landed int
}

// Writer coalesces concurrent Append calls into single WAL objects: at most
// one entry per commit interval, each one conditional PUT. It bounds what
// it holds on behalf of callers the store has not acked: past the bound
// Append returns ErrOverloaded instead of blocking, so a store that stops
// answering becomes a fast rejection rather than unbounded memory.
type Writer[R Record] struct {
	store  *objstore.Store
	prefix string
	ctx    context.Context // values for store and floor calls, not caller cancellation
	// onCommit applies a durable batch: its records in append order, and
	// the sequence of its last page. It must not wait on this writer's flush
	// goroutine (NewWriter).
	onCommit func(seq uint64, at time.Time, records []R)
	// floor, when set, reports the log's current watermark: pages at or
	// below it may have been truncated. A claim at or below it is refused as
	// ErrUnresolved: PutIfAbsent alone can no longer fence deleted keys (a
	// stale writer would win a sequence nobody holds and the entry would sit
	// below every future replay's start — an acknowledged, invisible write).
	//
	// retry says whether the batch may already be durable: a retry or the
	// post-claim check. That is the case the oracle CANNOT answer locally:
	// another process may have checkpointed and truncated the batch while
	// this writer still holds the lease. Only before a first attempt is it
	// false, when an owner can vouch for the watermark from its own state.
	floor func(ctx context.Context, retry bool) (uint64, error)
	// maxUnacked is an optional bound on accepted-but-unacked records.
	// The default admission bound is always measured in encoded bytes.
	maxUnacked       int
	unackedByteLimit int
	// attemptTimeout bounds one commit attempt. A timeout is errRetry — the
	// same unresolved outcome as a transport error, retried at the same seq
	// with the same nonce.
	attemptTimeout time.Duration
	// commitInterval is the minimum time between successful entries (see
	// DefaultCommitInterval). Read once, by loop, before anything can race
	// it; WithCommitInterval is the only writer.
	commitInterval time.Duration
	// log, when set, receives the rate-limited attempt-failure line
	// (SetLogger). Nil is silent: Stats still carries the cause.
	log *slog.Logger

	mu           sync.Mutex
	pending      []R
	pendingBytes int // exact framed record bytes; headers added at admission
	calls        []call
	inflight     *batch[R] // unresolved batch; always retried before pending is touched
	// nextSeq is the next sequence to claim. Only the flush goroutine
	// (flush, commit) touches it, so it needs no lock.
	nextSeq uint64
	closed  bool
	// terminal latches the error that finished the writer: ErrLostRace
	// (another writer owns the log) or ErrUnresolved (the next sequence is
	// unknowable), or ErrWriterFailed (sequence space exhausted). Nil while
	// the writer is usable.
	terminal error
	// pendingSince is when pending stopped being empty; zero when it is.
	// Promoted into batch.enqueued so the age survives the flush boundary.
	pendingSince time.Time
	// lostRace/unresolved count the verdicts ever RETURNED to waiters — the
	// two an operator must page on. Under mu with everything else here.
	lostRace, unresolved int64
	// rejected counts Appends refused at the unacked bound — the overload
	// signal an operator alerts on.
	rejected int64
	// lastErr is the newest failed commit attempt while the writer is
	// retrying; zero once a commit succeeds. lastLogged is when it was last
	// written to the log, touched only by the flush goroutine.
	lastErr    WriteError
	lastLogged time.Time
	lastCommit time.Time // start time of the last proven successful PUT, under mu
	nextRetry  time.Time // retry unresolved flight no earlier than this

	kick    chan struct{}
	done    chan struct{}
	stopped chan struct{} // closed when loop() exits; Close blocks on it
}

// Stats is one writer's queue state: what has been accepted but not acked,
// and the verdicts an operator pages on. Cheap — one mutex, no I/O.
type Stats struct {
	Pending         int        // records accepted and not yet acked (queued + in flight)
	Inflight        int        // 1 while a batch is mid-commit, else 0
	OldestPending   time.Time  // when the oldest un-acked record was accepted; zero when idle
	LostRace        int64      // ErrLostRace ever returned to a waiter (split brain); the batch that lost only
	Lost            bool       // the writer lost the race and refuses every Append (Terminal is ErrLostRace)
	Terminal        error      // what finished the writer (ErrLostRace, ErrUnresolved or ErrWriterFailed); nil while usable. Queued and later Appends get ErrWriterFailed
	Unresolved      int64      // ErrUnresolved ever returned to a waiter whose batch claimed a sequence: at Close, or a claim the floor or a corrupt read-back left unprovable. ErrWriterFailed is not counted
	Rejected        int64      // ErrOverloaded ever returned: Appends shed at the bound
	MaxUnacked      int        // optional record bound, zero when byte-only
	UnackedBytes    int        // reserved encoded bytes accepted but not yet acked
	MaxUnackedBytes int        // byte admission bound
	LastError       WriteError // newest failed attempt since the last successful commit; zero At when none
}

// Stats snapshots the writer under its own lock — the same lock flush and
// Append take, so a batch is never counted in both places.
func (w *Writer[R]) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := Stats{Pending: len(w.pending), OldestPending: w.pendingSince, LostRace: w.lostRace,
		Lost: errors.Is(w.terminal, ErrLostRace), Terminal: w.terminal, Unresolved: w.unresolved, Rejected: w.rejected, MaxUnacked: w.maxUnacked,
		UnackedBytes: reservedEntryBytes(w.pendingBytes, len(w.pending)), MaxUnackedBytes: w.unackedByteLimit, LastError: w.lastErr}
	if w.inflight != nil {
		// In flight is strictly older than anything still queued.
		s.Inflight = 1
		s.Pending += len(w.inflight.records)
		s.UnackedBytes += w.inflight.bytes
		s.OldestPending = w.inflight.enqueued
	}
	return s
}

// CommitInterval is the minimum interval between WAL entries (see
// DefaultCommitInterval). Fixed at NewWriter, so no lock is needed.
func (w *Writer[R]) CommitInterval() time.Duration { return w.commitInterval }

// SetFloor installs the watermark oracle commit consults before and after
// every claim (see Writer.floor). The post-claim check passes retry=true:
// the batch may now be durable, so the oracle must revalidate the watermark.
// Safe to call after NewWriter; nil disables the check,
// and so does an oracle that answers 0, which is how an owner skips the
// round trip while its lease is fresh. A claim the watermark covers is
// terminal: the batch gets ErrUnresolved, and every queued and later
// Append ErrWriterFailed. The writer is finished; reopen from the checkpoint.
func (w *Writer[R]) SetFloor(f func(ctx context.Context, retry bool) (uint64, error)) {
	w.mu.Lock()
	w.floor = f
	w.mu.Unlock()
}

// SetAttemptTimeout overrides the per-attempt deadline (see
// defaultAttemptTimeout); <= 0 restores the default. The deadline starts
// after the commit interval's pacing, so it bounds the store alone, and a
// multi-page batch's pages go out back to back inside it. Safe to call
// after NewWriter. Tests use it to make a hung store observable quickly.
func (w *Writer[R]) SetAttemptTimeout(d time.Duration) {
	if d <= 0 {
		d = defaultAttemptTimeout
	}
	w.mu.Lock()
	w.attemptTimeout = d
	w.mu.Unlock()
}

// SetMaxUnacked installs an additional record-count bound; <= 0 restores
// byte-only admission. Safe after NewWriter.
func (w *Writer[R]) SetMaxUnacked(n int) {
	if n <= 0 {
		n = 0
	}
	w.mu.Lock()
	w.maxUnacked = n
	w.mu.Unlock()
}

// SetLogger installs the logger the rate-limited attempt-failure line goes
// to (see errorLogEvery). The line carries op, key and cause; the caller
// attaches whatever it knows the writer by (slog.Logger.With). Safe to
// call after NewWriter; nil is silent.
func (w *Writer[R]) SetLogger(l *slog.Logger) {
	w.mu.Lock()
	w.log = l
	w.mu.Unlock()
}

// Option configures a Writer before its flush loop starts. Everything a
// caller may change AFTER that is a SetX method; the commit cadence is not,
// because the loop reads it once when it starts.
type Option func(*options)

type options struct {
	commitInterval time.Duration
	ctx            context.Context
}

// WithContext carries ctx's values into every store and floor oracle call.
// Its cancellation and deadline are ignored: Close governs the writer's
// lifetime, and SetAttemptTimeout bounds each attempt. ctx must not be nil.
func WithContext(ctx context.Context) Option {
	return func(o *options) { o.ctx = context.WithoutCancel(ctx) }
}

// WithCommitInterval overrides the group-commit cadence (see
// DefaultCommitInterval). <= 0 keeps the default; other values are clamped
// to [MinCommitInterval, MaxCommitInterval]. Refuse out-of-range values at
// configuration time, where the caller can still be told why.
func WithCommitInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.commitInterval = min(max(d, MinCommitInterval), MaxCommitInterval)
		}
	}
}

// NewWriter starts a writer that claims pages Key(prefix, nextSeq),
// Key(prefix, nextSeq+1), ... onCommit, if not nil, runs on the flush
// goroutine for every durable batch, before its callers are acked.
// It must not call Close or blocking Append on this writer, or wait for
// an Enqueue receipt from it: those operations need the flush goroutine.
// A batch starting at zero or exhausting the sequence space is refused
// with terminal ErrWriterFailed before any page is written.
func NewWriter[R Record](s *objstore.Store, prefix string, nextSeq uint64, onCommit func(seq uint64, at time.Time, records []R), opts ...Option) *Writer[R] {
	o := options{commitInterval: DefaultCommitInterval, ctx: context.Background()}
	for _, opt := range opts {
		opt(&o)
	}
	w := &Writer[R]{
		store: s, prefix: prefix, nextSeq: nextSeq, onCommit: onCommit, attemptTimeout: defaultAttemptTimeout,
		commitInterval: o.commitInterval, unackedByteLimit: maxUnackedBytes, ctx: o.ctx,
		kick: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
	}
	go w.loop()
	return w
}

// Append queues records and blocks until the batch containing them is
// durable: nil means every record is in a WAL page and onCommit has run.
// Any other error but ctx's means none of them will be (ErrUnresolved: may
// already be; see its doc). If ctx ends first Append returns ctx's error
// and the outcome is UNKNOWN: the batch may still commit. Use Enqueue to
// wait for the verdict past a cancellation. An empty append is a no-op.
func (w *Writer[R]) Append(ctx context.Context, records []R) error {
	if len(records) == 0 {
		return nil
	}
	ch, err := w.Enqueue(ctx, records)
	if err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		// The batch may still commit; the caller just stops waiting.
		return ctx.Err()
	}
}

// Enqueue accepts an indivisible append and returns its completion receipt.
// The receipt receives exactly one verdict, after onCommit on success, even
// if ctx is subsequently canceled. Callers can retain mutation locks until
// that verdict while allowing a canceled request to stop waiting promptly.
// The records are encoded on the flush goroutine; they must not change
// until the verdict. A record no page can hold, or an append larger than
// the unacked bound (ErrRecordTooLarge), or a record with a negative Size
// (ErrInvalidRecord) is refused here, before anything is queued. So is
// every append once the writer is terminal (Stats.Terminal): ErrWriterFailed,
// naming the cause; nothing was written.
func (w *Writer[R]) Enqueue(ctx context.Context, records []R) (<-chan error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ch := make(chan error, 1)
	if len(records) == 0 {
		ch <- nil
		return ch, nil
	}
	bytes := 0
	for i, r := range records {
		n := r.Size()
		if n < 0 {
			return nil, fmt.Errorf("%w: record %d: Size %d", ErrInvalidRecord, i, n)
		}
		if n > maxPageBytes || framedSize(n) > maxPageBytes-entryHeaderReserve {
			return nil, fmt.Errorf("%w: record %d is %d bytes", ErrRecordTooLarge, i, n)
		}
		if bytes > math.MaxInt-framedSize(n) {
			return nil, fmt.Errorf("%w: batch size overflows", ErrRecordTooLarge)
		}
		bytes += framedSize(n)
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrWriterClosed
	}
	if w.terminal != nil {
		err := w.failedLocked()
		w.mu.Unlock()
		return nil, err
	}
	// An append past a bound on its own can never be admitted: refusing it
	// as ErrOverloaded would invite a retry that cannot succeed.
	if reserved := reservedEntryBytes(bytes, len(records)); (w.maxUnacked > 0 && len(records) > w.maxUnacked) || reserved > w.unackedByteLimit {
		w.mu.Unlock()
		return nil, fmt.Errorf("%w: batch exceeds the unacked bound: %d records, %d encoded bytes", ErrRecordTooLarge, len(records), reserved)
	}
	// Include the incoming records and a header reserve per record, the
	// maximum number of pages they could need. Pending and flight are separate.
	// Refuse promptly even when the store has stopped acknowledging writes.
	n, reserved := len(w.pending), reservedEntryBytes(w.pendingBytes, len(w.pending))
	if w.inflight != nil {
		n += len(w.inflight.records)
		reserved += w.inflight.bytes
	}
	if (w.maxUnacked > 0 && len(records) > w.maxUnacked-n) || reserved > w.unackedByteLimit-reservedEntryBytes(bytes, len(records)) {
		w.rejected++
		w.mu.Unlock()
		return nil, fmt.Errorf("%w: %d records, %d encoded bytes reserved", ErrOverloaded, n, reserved)
	}
	if w.pendingSince.IsZero() {
		w.pendingSince = time.Now()
	}
	w.pending = append(w.pending, records...)
	w.pendingBytes += bytes
	w.calls = append(w.calls, call{ch: ch, n: len(records)})
	select {
	case w.kick <- struct{}{}:
	default:
	}
	w.mu.Unlock()
	return ch, nil
}

// failedLocked is the verdict for an append the finished writer refuses.
// %v, not %w, for the cause: the records were never claimed, so
// errors.Is(err, ErrUnresolved) must not say they may be durable. Caller
// holds w.mu.
func (w *Writer[R]) failedLocked() error {
	return fmt.Errorf("%w (cause: %v)", ErrWriterFailed, w.terminal)
}

// reservedEntryBytes bounds encoded size even if every record forces a page
// cut. Saturating on overflow keeps admission from accepting a wrapped size.
// Exact boundaries are determined at sealing.
func reservedEntryBytes(bytes, records int) int {
	if records == 0 {
		return 0
	}
	if records > (math.MaxInt-bytes)/entryHeaderReserve {
		return math.MaxInt
	}
	return bytes + records*entryHeaderReserve
}

// Close blocks until the final drain has finished: when it returns, every
// accepted batch is durable, failed, or reported ErrUnresolved, and no
// further objects will be written: safe to delete the prefix after.
//
// The drain is at most three commit attempts: the one in progress when
// Close is called and two final ones (the in-flight batch, then what queued
// behind it). Each waits at most one commit interval (after the last entry,
// or after the previous failed attempt: a drain does not retry back to
// back) and is bounded by the attempt timeout, so Close returns within
// 3 × (commit interval + attempt timeout), excluding record encoding and
// onCommit's own time. Neither can be interrupted by the attempt timeout.
func (w *Writer[R]) Close() {
	w.mu.Lock()
	already := w.closed
	w.closed = true
	w.mu.Unlock()
	if !already {
		close(w.done)
	}
	<-w.stopped
}

func (w *Writer[R]) loop() {
	defer close(w.stopped)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		w.mu.Lock()
		hasWork := w.inflight != nil || len(w.pending) > 0
		due := w.lastCommit.Add(w.commitInterval)
		if w.inflight == nil && !w.pendingSince.IsZero() && due.Before(w.pendingSince.Add(idleCoalesceDelay)) {
			due = w.pendingSince.Add(idleCoalesceDelay)
		}
		if due.Before(w.nextRetry) {
			due = w.nextRetry
		}
		ready := due
		if w.inflight == nil && !w.pendingSince.IsZero() {
			ready = due.Add(-precommitPrepareLead)
			if coalesced := w.pendingSince.Add(idleCoalesceDelay); ready.Before(coalesced) {
				ready = coalesced
			}
		}
		w.mu.Unlock()
		if hasWork && !time.Now().Before(ready) {
			select {
			case <-w.done:
				w.finalDrain()
				return
			default:
			}
			w.flush()
			continue
		}
		var timerC <-chan time.Time
		if hasWork {
			timer.Reset(time.Until(ready))
			timerC = timer.C
		}
		select {
		case <-timerC:
		case <-w.kick:
		case <-w.done:
			w.finalDrain()
			return
		}
		timer.Stop() // since Go 1.23 a stopped timer's channel holds nothing stale
	}
}

// flush resolves the in-flight batch if there is one, else promotes pending
// into flight, then attempts the commit. Unresolved outcomes keep the batch
// in flight — a batch is NEVER abandoned between its first PUT attempt and a
// proven outcome, which is what makes the nonce read-back exact.
func (w *Writer[R]) flush() {
	w.mu.Lock()
	if w.inflight == nil {
		if len(w.pending) == 0 {
			// Defense in depth: calls must be signaled on every path,
			// even an empty batch.
			calls := w.calls
			w.calls = nil
			w.mu.Unlock()
			deliver(calls, nil)
			return
		}
		w.inflight = &batch[R]{records: w.pending, bytes: reservedEntryBytes(w.pendingBytes, len(w.pending)),
			pageLimit: maxEntryBytes, calls: w.calls, nonce: w.mintNonceLocked(),
			enqueued: w.pendingSince, at: time.Now().UTC().Truncate(time.Millisecond)}
		w.pending, w.pendingBytes, w.calls, w.pendingSince = nil, 0, nil, time.Time{}
	}
	b := w.inflight
	w.mu.Unlock()

	err := w.commit(b)
	if errors.Is(err, errRetry) {
		w.mu.Lock()
		w.nextRetry = time.Now().Add(w.commitInterval)
		w.mu.Unlock()
		return // still in flight; same seq and nonce at the next retry
	}
	if err == nil && w.onCommit != nil && len(b.records) > 0 {
		w.onCommit(w.nextSeq-1, b.at, b.records) // keep pending until application completes
	}
	var queued []call
	var failed error
	w.mu.Lock()
	w.inflight = nil
	w.nextRetry = time.Time{}
	switch {
	case err == nil:
		if len(b.pages) > 0 {
			w.lastErr = WriteError{} // the store is answering again
		}
	case errors.Is(err, ErrLostRace), errors.Is(err, ErrUnresolved), errors.Is(err, ErrWriterFailed):
		// Terminal. ErrLostRace: another writer owns the log, and everything
		// queued would lose the same race. ErrUnresolved: the claim was
		// covered by the floor or met a corrupt page, so the next sequence
		// is unknowable; a later batch could land below the watermark
		// (acknowledged, never replayed) or contend with this batch's own
		// pages. Only b claimed a sequence: what queued behind it was never
		// written, so it gets ErrWriterFailed and is not counted.
		// Sequence exhaustion is also terminal, but nothing was claimed.
		w.terminal = err
		queued, failed = w.calls, w.failedLocked()
		w.pending, w.pendingBytes, w.calls, w.pendingSince = nil, 0, nil, time.Time{}
		if errors.Is(err, ErrLostRace) {
			w.lostRace += live(b.calls)
		} else if errors.Is(err, ErrUnresolved) {
			w.unresolved += live(b.calls)
		}
	}
	w.mu.Unlock()
	deliver(b.calls, err)
	deliver(queued, failed)
}

// finalDrain gives the in-flight and pending batches a last resolution
// attempt, then refuses to guess: still-unresolved waiters get
// ErrUnresolved; batches never attempted get ErrWriterClosed.
func (w *Writer[R]) finalDrain() {
	// commit paces each attempt itself; the drain does not wait on top.
	w.flush() // resolve inflight, or promote+commit pending
	w.flush() // if the first pass resolved inflight, commit any pending batch
	w.mu.Lock()
	b := w.inflight
	w.inflight = nil
	queued := w.calls
	w.pending, w.pendingBytes, w.calls, w.pendingSince = nil, 0, nil, time.Time{}
	if b != nil {
		w.unresolved += live(b.calls)
	}
	w.mu.Unlock()
	if b != nil {
		deliver(b.calls, ErrUnresolved)
	}
	deliver(queued, ErrWriterClosed)
}

// mintNonceLocked returns a fresh random identity for one batch. Caller
// holds w.mu.
func (w *Writer[R]) mintNonceLocked() string {
	buf := make([]byte, 16)
	rand.Read(buf) // never fails: since Go 1.24 it crashes the program instead
	return hex.EncodeToString(buf)
}

// commit claims nextSeq for batch b. In the common case that is ONE
// conditional PUT and nothing else: the page IS the durable record, and the
// floor oracle answers without a round trip while the writer's lease is
// fresh. Outcomes:
//   - claim won: durable, advance.
//   - claim lost, read-back shows b's nonce: our own earlier PUT for exactly
//     this batch landed and the response was lost — durable, advance.
//   - claim lost, foreign nonce: proven split brain — ErrLostRace.
//   - the store refusing the batch's first PUT on its first attempt
//     (objstore.ErrAccessDenied): nothing was written; the error, and the
//     writer goes on at the same sequence.
//   - anything unverifiable (PUT transport error, read-back failure, or this
//     attempt's deadline): errRetry; the batch stays in flight, retried with
//     the SAME seq and nonce, from the first page not yet proven.
//
// A batch that fails for good after some of its pages landed finishes the
// writer (ErrLostRace, ErrUnresolved): the log keeps those pages as an
// abandoned batch, which a walk reports as a marker.
func (w *Writer[R]) commit(b *batch[R]) error {
	if b.pages == nil {
		pages, kept, err := splitBatch(w.nextSeq, b)
		if kept != nil {
			// Records left the batch. Stats and Enqueue's admission read
			// both fields under w.mu.
			bytes := 0
			for _, r := range kept {
				bytes += framedSize(r.Size())
			}
			w.mu.Lock()
			b.records, b.bytes = kept, reservedEntryBytes(bytes, len(kept))
			w.mu.Unlock()
		}
		if err != nil {
			return err
		}
		if len(pages) == 0 {
			return nil // every call's records failed to encode; no sequence claimed
		}
		b.pages = pages
	}
	w.mu.Lock()
	floor := w.floor
	attempt := w.attemptTimeout
	// A retry also waits out nextRetry, so a drain (which calls flush
	// directly, not through loop's timer) does not retry back to back.
	due := w.lastCommit.Add(w.commitInterval)
	if due.Before(w.nextRetry) {
		due = w.nextRetry
	}
	w.mu.Unlock()
	// Pace the batch once, before its deadline starts: its pages are one
	// entry and go out back to back, so a multi-page batch fits one attempt.
	time.Sleep(time.Until(due))
	// A batch is shared by many callers; one caller's
	// cancellation must not abort everyone's durability. Bounded per ATTEMPT,
	// so a hung store is an unresolved retry, not a stuck flush goroutine.
	ctx, cancel := context.WithTimeout(w.ctx, attempt)
	defer cancel()
	b.attempts++
	err := w.putPages(ctx, b, floor)
	if err == nil {
		w.nextSeq += uint64(len(b.pages))
	}
	return err
}

// putPages claims b's pages from the first one not yet proven durable.
func (w *Writer[R]) putPages(ctx context.Context, b *batch[R], floor func(context.Context, bool) (uint64, error)) error {
	batchPages := uint64(len(b.pages))
	for i := b.landed; i < len(b.pages); i++ {
		seq := w.nextSeq + uint64(i)
		key := Key(w.prefix, seq)
		if floor != nil {
			f, err := floor(ctx, b.attempts > 1)
			if err != nil {
				return w.retry("floor", key, err)
			}
			if seq <= f {
				// Terminal (see flush): retrying cannot uncover the sequence.
				return fmt.Errorf("%w: sequence %d is covered by watermark %d", ErrUnresolved, seq, f)
			}
		}
		// Urgent: the acknowledged write path never waits behind bulk
		// object writes for a store slot.
		startedAt := time.Now()
		ok, err := w.store.PutIfAbsent(objstore.Urgent(ctx), key, b.pages[i])
		if err != nil && i == 0 && b.attempts == 1 && errors.Is(err, objstore.ErrAccessDenied) {
			// A refusal is an outcome: nothing of this batch was ever
			// written, so its callers fail now and the sequence stays free.
			// Retrying would hold every later write behind a revoked key.
			return fmt.Errorf("wal: put %s: %w", key, err)
		}
		if err != nil {
			return w.retry("put", key, err)
		}
		if ok {
			w.mu.Lock()
			w.lastCommit = startedAt
			w.mu.Unlock()
		} else {
			// Adopting our own earlier PUT starts no new entry: pacing
			// stays with the PUT that wrote it.
			data, gerr := w.store.Get(ctx, key)
			if gerr != nil {
				return w.retry("readback", key, gerr)
			}
			e, derr := Scan(data, nil)
			if derr != nil {
				return fmt.Errorf("%w: corrupt entry at contested seq %d: %v", ErrUnresolved, seq, derr)
			}
			if e.Nonce != b.nonce || e.Seq != seq || (e.BatchPages != 0 && (e.BatchIndex != uint64(i) || e.BatchPages != batchPages)) {
				return ErrLostRace
			}
		}
		if floor != nil {
			// A deleted claim can win after a replacement checkpointed it.
			// Revalidate now that our PUT may be durable, even on its first try.
			f, err := floor(ctx, true)
			if err != nil {
				return w.retry("floor", key, err)
			}
			if seq <= f {
				return fmt.Errorf("%w: sequence %d is covered by watermark %d", ErrUnresolved, seq, f)
			}
		}
		b.landed = i + 1
	}
	return nil
}

// retry records one unverifiable attempt and returns errRetry. The record
// is the operator's, not the caller's: the batch stays in flight, and
// nothing about how it is retried changes. The log line is rate-limited
// per writer (errorLogEvery); Stats always shows the newest cause.
func (w *Writer[R]) retry(op, key string, cause error) error {
	now := time.Now()
	w.mu.Lock()
	w.lastErr = WriteError{At: now, Op: op, Key: key, Err: cause}
	log := w.log
	w.mu.Unlock()
	if log != nil && now.Sub(w.lastLogged) >= errorLogEvery {
		w.lastLogged = now
		log.Warn("wal commit attempt failed; batch retried at the same sequence", "op", op, "key", key, "err", cause)
	}
	return errRetry
}

// splitBatch encodes b's records once and cuts them into bounded pages,
// sequenced from seq in append order. A batch that fits one page is
// returned without copying. A call whose record fails to encode gets
// ErrInvalidRecord and its records leave the batch: kept, non-nil only
// then, is the records that remain. b.records and b.bytes are not
// modified (the caller publishes kept under its lock); each failing call's
// err is set in b.calls. The others still commit. No pages and no
// error: every call failed. Without calls (standalone callers) any failing
// record fails the batch.
func splitBatch[R Record](seq uint64, b *batch[R]) (pages [][]byte, kept []R, err error) {
	// Keep the next sequence representable too: zero is outside replay.
	if seq == 0 || seq == math.MaxUint64 {
		return nil, nil, fmt.Errorf("%w: sequence space exhausted at %d", ErrWriterFailed, seq)
	}
	head := Header{Seq: seq, Nonce: b.nonce, At: b.at, BatchPages: 1}
	// Until pages are cut, an overflowing sum is only a header-size reserve.
	weight, _ := weightOf(b.records)
	header, err := appendHeader(nil, head, len(b.records), weight)
	if err != nil {
		return nil, nil, err
	}
	hdr := len(header)
	total := hdr
	for _, r := range b.records {
		n := r.Size()
		if err := checkRecordSize(n); err != nil {
			return nil, nil, err
		}
		if total > math.MaxInt-4-framedSize(n) {
			return nil, nil, ErrRecordTooLarge
		}
		total += framedSize(n)
	}
	buf := make([]byte, hdr, total+4)
	copy(buf, header)
	ends := make([]int, 0, len(b.records)) // buf offset just past each kept record
	groups := b.calls
	if len(groups) == 0 {
		groups = []call{{n: len(b.records)}}
	}
	dropped := false
	next := 0
	var sum uint64 // the kept calls' weight
	for gi := range groups {
		g := &groups[gi]
		recs := b.records[next : next+g.n]
		next += g.n
		mark, marks := len(buf), len(ends)
		for _, r := range recs {
			var nb []byte
			if nb, err = appendRecord(buf, r); err != nil {
				break
			}
			buf = nb
			ends = append(ends, len(buf))
		}
		if err == nil && len(b.calls) > 0 {
			// Calls whose weights sum within uint64 overflow no page however
			// they are cut, so the call that would overflow the sum is refused
			// here, before any cut, and no cut is ever redone for weight.
			w, werr := weightOf(recs)
			if werr != nil || w > math.MaxUint64-sum {
				g.err = fmt.Errorf("%w: batch weight overflows uint64", ErrInvalidRecord)
				buf, ends, dropped = buf[:mark], ends[:marks], true
				continue
			}
			sum += w
		}
		if err != nil {
			if len(b.calls) == 0 {
				return nil, nil, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
			}
			g.err = fmt.Errorf("%w: %w", ErrInvalidRecord, err)
			buf, ends, err, dropped = buf[:mark], ends[:marks], nil, true
		}
	}
	records := b.records
	if dropped {
		kept = make([]R, 0, len(ends))
		next = 0
		for _, c := range b.calls {
			if c.err == nil {
				kept = append(kept, b.records[next:next+c.n]...)
			}
			next += c.n
		}
		if len(kept) == 0 {
			return nil, kept, nil
		}
		records = kept
	}
	// The record count and weight are in the header, and their varints
	// may shrink. They never grow, so the new header is written to end
	// where the old one did and buf starts where it does: no record
	// byte moves. Only a single page uses this header; a multi-page cut
	// writes its own.
	weight, weightErr := weightOf(records)
	if header, err = appendHeader(header[:0], head, len(records), weight); err != nil {
		return nil, nil, err
	}
	shift := hdr - len(header)
	buf = buf[shift:]
	copy(buf, header)
	for i := range ends {
		ends[i] -= shift
	}
	hdr = len(header)
	limit := b.pageLimit
	if limit == 0 {
		limit = maxPageBytes
	}
	reserve := headerReserve(len(b.nonce))
	// Indivisible records up to the codec ceiling are allowed. They cannot
	// fit the entry budget; retain the ceiling for them and cap other
	// batches at maxEntryBytes.
	if limit == maxEntryBytes {
		for i := range records {
			from := hdr
			if i > 0 {
				from = ends[i-1]
			}
			if ends[i]-from > maxEntryBytes-reserve {
				limit = maxPageBytes
				break
			}
		}
	}
	bounds := []int{0} // first record of each page, then len(records)
	if len(buf) > limit-4 {
		// Cut so every page fits beneath the largest header it could carry.
		// Enqueue refuses records past this budget; guard standalone callers too.
		budget := limit - reserve
		start := hdr
		for i := range records {
			if ends[i]-start > budget && i > bounds[len(bounds)-1] {
				bounds = append(bounds, i)
				start = ends[i-1]
			}
			if ends[i]-start > budget {
				return nil, kept, ErrRecordTooLarge
			}
		}
	}
	bounds = append(bounds, len(records))
	if len(bounds) == 2 {
		if weightErr != nil { // standalone: calls were refused above
			return nil, nil, weightErr
		}
		return [][]byte{seal(buf)}, kept, nil
	}
	if uint64(len(bounds)-1) > math.MaxUint64-seq {
		return nil, kept, fmt.Errorf("%w: sequence space exhausted at %d", ErrWriterFailed, seq)
	}
	pages = make([][]byte, len(bounds)-1)
	head.BatchPages = uint64(len(pages))
	for i := range pages {
		lo, hi := bounds[i], bounds[i+1]
		from, to := hdr, ends[hi-1]
		if lo > 0 {
			from = ends[lo-1]
		}
		head.Seq, head.BatchIndex = seq+uint64(i), uint64(i)
		weight, err := weightOf(records[lo:hi])
		if err != nil {
			return nil, kept, err
		}
		page, err := appendHeader(make([]byte, 0, reserve+to-from), head, hi-lo, weight)
		if err != nil {
			return nil, nil, err
		}
		if pages[i] = seal(append(page, buf[from:to]...)); len(pages[i]) > limit {
			return nil, nil, fmt.Errorf("wal: page %d of %d exceeds size limit", i, len(pages))
		}
	}
	return pages, kept, nil
}
