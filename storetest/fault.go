package storetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
)

// ErrFault is the transport error an armed Plan returns. Tests match on it;
// production code never sees it (nothing constructs a Fault outside tests).
var ErrFault = errors.New("storetest: injected fault")

// Op names the backend operation a Plan targets. OpDelete matches both
// Delete and DeleteMany, since callers use both; a Plan.Key filter is the
// way to tell them apart. OpGet matches all three whole-object reads: Get,
// GetWithETag and GetIfChanged.
type Op string

const (
	OpPut         Op = "Put"
	OpPutIfAbsent Op = "PutIfAbsent"
	OpPutIfMatch  Op = "PutIfMatch"
	OpGet         Op = "Get"
	OpGetRange    Op = "GetRange"
	OpList        Op = "List"
	OpDelete      Op = "Delete"
)

// OpListPrefixes is the delimited listing (Store.ListPrefixes and
// ListPrefixesPage). It counts, and is planned, separately from OpList so a
// test can assert "no prefix LIST on this path" while discovery's
// delimited listing still happens.
const OpListPrefixes Op = "ListPrefixes"

// Mode is what the Nth matching call does instead of succeeding.
type Mode int

const (
	// Fail returns ErrFault without touching storage: the write never
	// happened, the caller knows nothing landed.
	Fail Mode = iota
	// Ambiguous performs the operation and THEN returns ErrFault: the bytes
	// landed but the caller cannot know it. This is the outcome a write
	// that reads back its own nonce exists to resolve.
	Ambiguous
	// Hang blocks until the call's context is cancelled. A call made with
	// a context that is never cancelled (context.Background) hangs for
	// good: nothing, not even Faulty's cleanup, releases it.
	Hang
	// Pause stops immediately before storage until Fault.Resume, or until
	// the call's context ends (the call then returns the context's error).
	// It models a process pause, including an operation already past its
	// lease check. Each Set of a Pause plan arms a fresh pause, so a Resume
	// issued before that Set never releases it; a Resume after the Set but
	// before the call arrives does, and the call then passes straight
	// through.
	Pause
)

// Plan arms exactly one fault: the N'th call to Op whose key contains Key.
// The zero Plan (N == 0) is disarmed. One plan at a time is deliberate: a
// crash point is one point. For listings, Key is matched against the
// prefix.
type Plan struct {
	Op   Op
	N    int // fire on the N'th matching call, 1-based; <=0 never fires
	Mode Mode
	Key  string // substring a key must contain to match; "" matches any
	// From narrows OpGetRange to reads starting at exactly this offset, so
	// a test can target one part of an object read by byte range without
	// hitting its header or its neighbours. It applies when MatchFrom is
	// set, or when From is non-zero (so From: 0 alone matches any offset;
	// set MatchFrom to target offset 0). Only OpGetRange has an offset:
	// Set panics on a plan for any other Op that sets From or MatchFrom.
	From      int64
	MatchFrom bool
}

// Shape adds deterministic transport conditions to every operation. A zero
// Shape disables shaping and leaves the normal Fault path unchanged.
//
// Shape and Plan compose. The plan is checked first; a call it fires on
// is not shaped. Every other call waits Latency plus its size over
// BytesPerSecond, then fails with ErrFault at ErrorRate (drawn from a
// generator seeded with Seed, so a sequential test sees the same failures
// on every run). Object reads (Get, GetWithETag, GetIfChanged, GetRange)
// are shaped after the backend answers, sized by the bytes returned, and a
// shaped failure returns no data. Every other call (writes, deletes, both
// listings, EnsureBucket, DropBucket) is shaped before it reaches the
// backend, with size zero except for a write's payload, so a shaped
// failure never lands.
type Shape struct {
	Latency        time.Duration
	BytesPerSecond int64   // 0 = unlimited
	ErrorRate      float64 // in [0, 1]
	Seed           int64
}

type faultShape struct {
	Shape
	mu     sync.Mutex
	rng    *rand.Rand
	errors int
}

// Fault wraps a backend and injects one planned failure. Counting is under a
// mutex and the wrapped call runs outside it, so a plan is deterministic
// even when the caller fans out. Build one with NewFault; the zero Fault
// has no backend, though its Set, Ops and ledger methods are safe to call.
type Fault struct {
	b      objstore.Backend
	mu     sync.Mutex
	plan   Plan
	seen   int
	fired  int
	paused chan struct{} // the armed Pause plan's; closed by Resume
	// held is every pause channel not yet closed, the armed one included:
	// Resume releases them all.
	held []chan struct{}
	// inflight counts backend calls in progress, from entry to return, so
	// cleanup can wait for a resumed call to reach the backend and finish.
	inflight atomic.Int64
	// ops counts every backend call by operation, armed or not — the
	// measurement half of this layer: "how many store requests did this
	// replay / refresh / compaction cycle cost". readBytes is the payload
	// side of the same question for Get, GetRange and GetWithETag: the
	// bytes a read handed back, which on S3 is what egress and the
	// per-request transfer time are billed on.
	ops        map[Op]int
	readBytes  int64
	writeBytes int64
	// readKeys is the per-key read ledger for the current ResetOps window,
	// so a test can assert WHICH objects a query touched — one attribute
	// directory shard and two ids columns — and not only how many.
	readKeys  map[string]int
	writeKeys map[string]int // backend-confirmed writes, before injected ambiguity
	// digest/rewritten are the WRITE-ONCE ledger (WatchRewrites): the bytes
	// last written under each key, and the keys whose bytes then changed.
	// nil until a caller asks for it.
	digest    map[string][32]byte
	rewritten map[string]bool
	shape     *faultShape
}

// WatchRewrites starts the write-once ledger: from here on the injector
// keeps a SHA-256 digest of the bytes last written under each key, and
// Rewrites reports every key later written with DIFFERENT bytes. Rewriting
// identical bytes is not a rewrite.
//
// A read cache that never invalidates (cache.ByteCache keyed by object
// name, say) is only correct if its keys are write-once: a key rewritten
// with different bytes is served stale for the life of the process and
// leaves no trace. This ledger is how a test catches one. A deleted key is
// forgotten: deleting a key and writing it again is not a rewrite.
//
// It watches every key, including ones mutable by design (a lease, a
// manifest head). Filter Rewrites by the keys your test considers
// write-once. The digest covers the bytes the backend is handed.
func (f *Fault) WatchRewrites() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.digest, f.rewritten = map[string][32]byte{}, map[string]bool{}
}

// Rewrites returns the keys written twice with different bytes, sorted.
func (f *Fault) Rewrites() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.rewritten))
	for k := range f.rewritten {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// wrote records one landed write against the ledger.
func (f *Fault) wrote(key string, data []byte) {
	f.mu.Lock()
	watching := f.digest != nil
	f.mu.Unlock()
	var sum [32]byte
	if watching { // hash outside the mutex: fanned-out writers do not queue on it
		sum = sha256.Sum256(data)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeKeys == nil {
		f.writeKeys = map[string]int{}
	}
	f.writeKeys[key]++
	f.writeBytes += int64(len(data))
	if f.digest == nil || !watching {
		return
	}
	if had, ok := f.digest[key]; ok && had != sum {
		f.rewritten[key] = true
	}
	f.digest[key] = sum
}

// forget drops keys from the ledger: after a delete the key is free to be
// minted again with different bytes.
func (f *Fault) forget(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.digest, k)
	}
}

// NewFault returns a Store sharing s's backend behind a Fault. With no Plan
// armed it is a pass-through — it passes the backend conformance suite.
func NewFault(s *objstore.Store) (*objstore.Store, *Fault) {
	var f *Fault
	// The wrapper stands in for the same bucket: it must report the same
	// encryption, or a fault-injected test would see a different namespace
	// metadata than the store it wraps.
	return s.WithBackend(func(b objstore.Backend) objstore.Backend {
		f = &Fault{b: b}
		return f
	}), f
}

// SupportsKMS preserves the wrapped backend's optional capability.
func (f *Fault) SupportsKMS() bool {
	k, ok := f.b.(interface{ SupportsKMS() bool })
	return ok && k.SupportsKMS()
}

// Close passes through to the wrapped backend's Close, if any (Store.Close).
func (f *Fault) Close() error {
	if c, ok := f.b.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// SetShape replaces the transport shape; the zero Shape turns shaping
// off. Calls already in flight keep the shape they started with.
func (f *Fault) SetShape(s Shape) {
	var shape *faultShape
	if s != (Shape{}) {
		shape = &faultShape{Shape: s, rng: rand.New(rand.NewPCG(uint64(s.Seed), 0))}
	}
	f.mu.Lock()
	f.shape = shape
	f.mu.Unlock()
}

// shapeSnapshot reads the current shaper under f.mu; SetShape swaps the
// pointer while calls are in flight, and each call keeps the one it saw.
func (f *Fault) shapeSnapshot() *faultShape {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shape
}

// ShapeErrors is how many calls the current Shape failed.
func (f *Fault) ShapeErrors() int {
	s := f.shapeSnapshot()
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errors
}

func (f *Fault) shapeCall(ctx context.Context, bytes int) error {
	s := f.shapeSnapshot()
	if s == nil {
		return nil
	}
	d := s.Latency
	if s.BytesPerSecond > 0 && bytes > 0 {
		d += time.Duration(int64(time.Second) * int64(bytes) / s.BytesPerSecond)
	}
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	s.mu.Lock()
	fail := s.ErrorRate > 0 && s.rng.Float64() < s.ErrorRate
	if fail {
		s.errors++
	}
	s.mu.Unlock()
	if fail {
		return ErrFault
	}
	return nil
}

// Ops returns the per-operation call counts so far. ResetOps zeroes them:
// the pair brackets one measured window (a replay, a refresh tick, a
// compaction cycle).
func (f *Fault) Ops() map[Op]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[Op]int, len(f.ops))
	for op, n := range f.ops {
		out[op] = n
	}
	return out
}

// ResetOps zeroes the call counts and the read and write ledgers (not the
// WatchRewrites ledger).
func (f *Fault) ResetOps() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.ops)
	clear(f.readKeys)
	clear(f.writeKeys)
	f.readBytes = 0
	f.writeBytes = 0
}

// ReadKeys returns how many reads each key took since the last ResetOps.
func (f *Fault) ReadKeys() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.readKeys))
	for k, n := range f.readKeys {
		out[k] = n
	}
	return out
}

// WriteKeys counts backend-confirmed successful writes per key since ResetOps.
// Failed conditional claims do not count. Injected Ambiguous writes count
// because the wrapped backend succeeded before the answer was suppressed.
// A real backend error remains unproven and does not count. Filter by a key
// prefix to measure one kind of object separately from the rest; these
// are physical pages, not acknowledged logical batches or caller requests.
func (f *Fault) WriteKeys() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.writeKeys))
	for key, n := range f.writeKeys {
		out[key] = n
	}
	return out
}

func (f *Fault) noteRead(key string) {
	f.mu.Lock()
	if f.readKeys == nil {
		f.readKeys = map[string]int{}
	}
	f.readKeys[key]++
	f.mu.Unlock()
}

// ReadBytes returns the payload bytes returned by successful reads since
// the last ResetOps.
func (f *Fault) ReadBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readBytes
}

// WriteBytes returns payload bytes in successful PUTs since ResetOps.
// Conditional PUTs count only when they land, including ambiguous replies.
func (f *Fault) WriteBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeBytes
}

func (f *Fault) read(b []byte) {
	f.mu.Lock()
	f.readBytes += int64(len(b))
	f.mu.Unlock()
}

// Set arms p and resets the match counter. A Pause plan gets a fresh
// pause, released by the next Resume. Set panics on a plan that sets From
// or MatchFrom for an Op other than OpGetRange: it could never fire.
func (f *Fault) Set(p Plan) {
	if p.Op != OpGetRange && (p.From != 0 || p.MatchFrom) {
		panic(fmt.Sprintf("storetest: Plan.From/MatchFrom apply to OpGetRange only, not %s", p.Op))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plan, f.seen, f.fired = p, 0, 0
	if p.Mode == Pause {
		f.paused = make(chan struct{})
		f.held = append(f.held, f.paused)
	}
}

// Clear disarms. Hang still waits for cancellation; Pause still waits for Resume.
func (f *Fault) Clear() { f.Set(Plan{}) }

// Fired reports how many times the armed plan fired (0 or 1).
func (f *Fault) Fired() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fired
}

// Resume releases every operation stopped by Pause, and the armed Pause
// plan if it has not fired yet. Like a process resuming, the operation then
// runs normally; the lease may have expired meanwhile. Resume with nothing
// paused is a no-op.
func (f *Fault) Resume() {
	f.mu.Lock()
	held := f.held
	f.paused, f.held = nil, nil
	f.mu.Unlock()
	for _, ch := range held {
		close(ch)
	}
}

// Faulty returns s wrapped in a fault injector, for crash-point tests.
// Disarmed until the caller sets a Plan. Cleanup resumes any call still
// held by a Pause plan and waits (up to 5s) for every call in flight,
// paused, hung or running, to return, so a failed test neither leaks a
// blocked goroutine nor lets one write into a removed TempDir; a call still
// in flight after that fails the test. A Hang call ends only with its
// context: use t.Context() (cancelled before cleanup), never
// context.Background().
func Faulty(t testing.TB, s *objstore.Store) (*objstore.Store, *Fault) {
	t.Helper()
	s, f := NewFault(s)
	t.Cleanup(func() {
		f.Resume()
		if !f.drain(5 * time.Second) {
			t.Errorf("storetest: %d calls still in flight 5s into cleanup (a Hang on a context that is never cancelled?)", f.inflight.Load())
		}
	})
	return s, f
}

// drain waits up to timeout for no call to be in flight, and reports
// whether that happened.
func (f *Fault) drain(timeout time.Duration) bool {
	for deadline := time.Now().Add(timeout); f.inflight.Load() > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			return false
		}
	}
	return true
}

// hit counts the call and reports whether it is the planned one. A Pause
// plan blocks here and then reports no hit, unless ctx ends first: that
// reports a Hang hit, which returns ctx's error at once.
func (f *Fault) hit(ctx context.Context, op Op, keys ...string) (Mode, bool) {
	return f.hitAt(ctx, op, -1, keys...)
}

// hitAt is hit for a ranged read starting at off; Plan.From narrows it.
func (f *Fault) hitAt(ctx context.Context, op Op, off int64, keys ...string) (Mode, bool) {
	f.mu.Lock()
	if f.ops == nil {
		f.ops = map[Op]int{}
	}
	f.ops[op]++
	p := f.plan
	if p.N <= 0 || p.Op != op ||
		(p.Key != "" && !anyContains(keys, p.Key)) ||
		((p.MatchFrom || p.From != 0) && p.From != off) {
		f.mu.Unlock()
		return 0, false
	}
	f.seen++
	if f.seen != p.N {
		f.mu.Unlock()
		return 0, false
	}
	f.fired++
	paused := f.paused
	f.mu.Unlock()
	if p.Mode != Pause {
		return p.Mode, true
	}
	if paused != nil {
		select {
		case <-paused:
		case <-ctx.Done():
			return Hang, true
		}
	}
	return 0, false
}

func anyContains(keys []string, sub string) bool {
	for _, k := range keys {
		if strings.Contains(k, sub) {
			return true
		}
	}
	return false
}

// pre is the injection for the modes that never reach storage.
func (f *Fault) pre(ctx context.Context, m Mode) error {
	if m == Hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return ErrFault
}

// Put is the backend's Put, counted and planned as OpPut.
func (f *Fault) Put(ctx context.Context, key string, data []byte) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, OpPut, key)
	if hit && m != Ambiguous {
		return f.pre(ctx, m)
	}
	if !hit {
		if err := f.shapeCall(ctx, len(data)); err != nil {
			return err
		}
	}
	err := f.b.Put(ctx, key, data)
	if err == nil {
		f.wrote(key, data)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return err
}

// PutIfAbsent is the backend's, counted and planned as OpPutIfAbsent.
func (f *Fault) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, OpPutIfAbsent, key)
	if hit && m != Ambiguous {
		return false, f.pre(ctx, m)
	}
	if !hit {
		if err := f.shapeCall(ctx, len(data)); err != nil {
			return false, err
		}
	}
	ok, err := f.b.PutIfAbsent(ctx, key, data)
	if ok && err == nil {
		f.wrote(key, data)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return ok, err
}

// PutIfMatch is the backend's, counted and planned as OpPutIfMatch.
func (f *Fault) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, OpPutIfMatch, key)
	if hit && m != Ambiguous {
		return false, f.pre(ctx, m)
	}
	if !hit {
		if err := f.shapeCall(ctx, len(data)); err != nil {
			return false, err
		}
	}
	ok, err := f.b.PutIfMatch(ctx, key, data, etag)
	if ok && err == nil {
		f.wrote(key, data)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return ok, err
}

// Get is the backend's Get, counted and planned as OpGet.
func (f *Fault) Get(ctx context.Context, key string) ([]byte, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	f.noteRead(key)
	// A read has no ambiguous outcome: nothing changed either way.
	if m, hit := f.hit(ctx, OpGet, key); hit {
		return nil, f.pre(ctx, m)
	}
	b, err := f.b.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := f.shapeCall(ctx, len(b)); err != nil {
		return nil, err
	}
	f.read(b)
	return b, nil
}

// ListPage is the backend's, counted and planned as OpList (Plan.Key
// matches the prefix).
func (f *Fault) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if m, hit := f.hit(ctx, OpList, prefix); hit {
		return nil, "", f.pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return nil, "", err
	}
	return f.b.ListPage(ctx, prefix, after, limit)
}

// Delete is the backend's, counted and planned as OpDelete.
func (f *Fault) Delete(ctx context.Context, key string) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, OpDelete, key)
	if hit && m != Ambiguous {
		return f.pre(ctx, m)
	}
	if !hit {
		if err := f.shapeCall(ctx, 0); err != nil {
			return err
		}
	}
	err := f.b.Delete(ctx, key)
	if err == nil {
		f.forget(key)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return err
}

// DeleteMany is the backend's, counted and planned as OpDelete; a plan
// fires when any of keys matches.
func (f *Fault) DeleteMany(ctx context.Context, keys ...string) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, OpDelete, keys...)
	if hit && m != Ambiguous {
		return f.pre(ctx, m)
	}
	if !hit {
		if err := f.shapeCall(ctx, 0); err != nil {
			return err
		}
	}
	err := f.b.DeleteMany(ctx, keys...)
	if err == nil {
		f.forget(keys...)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return err
}

// ListPrefixesPage is the backend's, counted and planned as
// OpListPrefixes (Plan.Key matches the prefix).
func (f *Fault) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if m, hit := f.hit(ctx, OpListPrefixes, prefix); hit {
		return nil, "", f.pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return nil, "", err
	}
	return f.b.ListPrefixesPage(ctx, prefix, after, limit)
}

// GetWithETag is the backend's, counted and planned as OpGet.
func (f *Fault) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	f.noteRead(key)
	if m, hit := f.hit(ctx, OpGet, key); hit {
		return nil, "", f.pre(ctx, m)
	}
	b, etag, err := f.b.GetWithETag(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if err := f.shapeCall(ctx, len(b)); err != nil {
		return nil, "", err
	}
	f.read(b)
	return b, etag, nil
}

// GetIfChanged is the backend's, counted and planned as OpGet.
func (f *Fault) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	f.noteRead(key)
	if m, hit := f.hit(ctx, OpGet, key); hit {
		return nil, "", false, f.pre(ctx, m)
	}
	b, current, unchanged, err := f.b.GetIfChanged(ctx, key, etag)
	if err != nil {
		return nil, "", false, err
	}
	if err := f.shapeCall(ctx, len(b)); err != nil {
		return nil, "", false, err
	}
	f.read(b)
	return b, current, unchanged, nil
}

// EnsureBucket is the backend's, shaped but never planned or counted.
func (f *Fault) EnsureBucket(ctx context.Context) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
	}
	return f.b.EnsureBucket(ctx)
}

// DropBucket is the backend's, shaped but never planned or counted.
func (f *Fault) DropBucket(ctx context.Context) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
	}
	return f.b.DropBucket(ctx)
}

// GetRange is the backend's, counted and planned as OpGetRange
// (Plan.From can narrow it to one offset).
func (f *Fault) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	f.noteRead(key)
	if m, hit := f.hitAt(ctx, OpGetRange, offset, key); hit {
		return nil, f.pre(ctx, m)
	}
	b, err := f.b.GetRange(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	if err := f.shapeCall(ctx, len(b)); err != nil {
		return nil, err
	}
	f.read(b)
	return b, nil
}
