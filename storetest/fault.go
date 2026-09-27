package storetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/objstore"
)

// ErrFault is the transport error an armed Plan returns. Tests match on it;
// production code never sees it (nothing constructs a Fault outside tests).
var ErrFault = errors.New("store: injected fault")

// Op names the backend operation a Plan targets. OpDelete matches both
// Delete and DeleteMany, since callers use both; a Plan.Key filter is the
// way to tell them apart.
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

// OpListPrefixes is the delimited listing (Store.ListPrefixes). It counts
// separately from OpList so a test can assert "no prefix LIST on this path"
// while discovery's delimited listing still happens.
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
	// Hang blocks until the call's context is cancelled.
	Hang
	// Pause stops immediately before storage until Fault.Resume. It models a
	// process pause, including an operation already past its lease check.
	Pause
)

// Plan arms exactly one fault: the N'th call to Op whose key contains Key.
// The zero Plan (N == 0) is disarmed. One plan at a time is deliberate — a
// crash point is one point.
type Plan struct {
	Op   Op
	N    int // fire on the N'th matching call, 1-based; <=0 never fires
	Mode Mode
	Key  string // substring a key must contain to match; "" matches any
	// From narrows OpGetRange to reads starting at exactly this offset, so
	// a test can target one part of an object read by byte range without
	// hitting its header or its neighbours. 0 matches any offset.
	From int64
}

// Shape adds deterministic transport conditions to every operation. A zero
// Shape disables shaping and leaves the normal Fault path unchanged.
type Shape struct {
	Latency        time.Duration
	BytesPerSecond int64
	ErrorRate      float64
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
// even when the caller fans out.
type Fault struct {
	b      objstore.Backend
	mu     sync.Mutex
	plan   Plan
	seen   int
	fired  int
	paused chan struct{}
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
// remembers a digest per key and records any key whose bytes CHANGE under it.
//
// Almost everything in this bucket is write-once, and a read cache
// may rest on it — it never invalidates, so a key rewritten with different bytes
// is served stale for the life of the process and leaves no trace to find
// afterwards. This is the only way to catch one. A key that is DELETED is
// forgotten: retiring a key and minting it again is not a rewrite, it is what
// Drop does.
//
// It watches EVERYTHING, including the three objects that are mutable by
// design, such as a manifest head or a lease. Filtering is the caller's,
// because which keys a given test considers write-once is a statement about
// the caller, not about the store.
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
	sort.Strings(out)
	return out
}

// wrote records one landed write against the ledger.
func (f *Fault) wrote(key string, data []byte) {
	sum := sha256.Sum256(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeKeys == nil {
		f.writeKeys = map[string]int{}
	}
	f.writeKeys[key]++
	f.writeBytes += int64(len(data))
	if f.digest == nil {
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
		f = &Fault{b: b, ops: map[Op]int{}}
		return f
	}), f
}

func (f *Fault) SetShape(s Shape) {
	var shape *faultShape
	if s != (Shape{}) {
		shape = &faultShape{Shape: s, rng: rand.New(rand.NewSource(s.Seed))}
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

func (f *Fault) count(op Op) {
	f.mu.Lock()
	f.ops[op]++
	f.mu.Unlock()
}

// Set arms p and resets the match counter.
func (f *Fault) Set(p Plan) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plan, f.seen, f.fired = p, 0, 0
	if p.Mode == Pause && f.paused == nil {
		f.paused = make(chan struct{})
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

// Resume releases an operation stopped by Pause. Like a process resuming,
// the operation then runs normally; the lease may have expired meanwhile.
func (f *Fault) Resume() {
	f.mu.Lock()
	paused := f.paused
	f.paused = nil
	f.mu.Unlock()
	if paused != nil {
		close(paused)
	}
}

// hit advances the counter and reports whether this call is the planned one.
func (f *Fault) hit(op Op, keys ...string) (Mode, bool) {
	return f.hitAt(op, 0, keys...)
}

// hitAt is hit for a ranged read starting at off; Plan.From narrows it.
func (f *Fault) hitAt(op Op, off int64, keys ...string) (Mode, bool) {
	f.mu.Lock()
	f.ops[op]++
	if f.plan.N <= 0 || f.plan.Op != op {
		f.mu.Unlock()
		return 0, false
	}
	if f.plan.Key != "" && !anyContains(keys, f.plan.Key) {
		f.mu.Unlock()
		return 0, false
	}
	if f.plan.From != 0 && f.plan.From != off {
		f.mu.Unlock()
		return 0, false
	}
	f.seen++
	if f.seen != f.plan.N {
		f.mu.Unlock()
		return 0, false
	}
	f.fired++
	mode, paused := f.plan.Mode, f.paused
	f.mu.Unlock()
	if mode == Pause {
		if paused != nil {
			<-paused
		}
		return 0, false
	}
	return mode, true
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
func pre(ctx context.Context, m Mode) error {
	if m == Hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return ErrFault
}

func (f *Fault) Put(ctx context.Context, key string, data []byte) error {
	m, hit := f.hit(OpPut, key)
	if hit && m != Ambiguous {
		return pre(ctx, m)
	}
	if err := f.shapeCall(ctx, len(data)); err != nil {
		return err
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

func (f *Fault) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	m, hit := f.hit(OpPutIfAbsent, key)
	if hit && m != Ambiguous {
		return false, pre(ctx, m)
	}
	if err := f.shapeCall(ctx, len(data)); err != nil {
		return false, err
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

func (f *Fault) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	m, hit := f.hit(OpPutIfMatch, key)
	if hit && m != Ambiguous {
		return false, pre(ctx, m)
	}
	if err := f.shapeCall(ctx, len(data)); err != nil {
		return false, err
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

func (f *Fault) Get(ctx context.Context, key string) ([]byte, error) {
	f.noteRead(key)
	// A read has no ambiguous outcome: nothing changed either way.
	if m, hit := f.hit(OpGet, key); hit {
		return nil, pre(ctx, m)
	}
	b, err := f.b.Get(ctx, key)
	if err == nil {
		err = f.shapeCall(ctx, len(b))
	}
	f.read(b)
	return b, err
}

func (f *Fault) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if m, hit := f.hit(OpList, prefix); hit {
		return nil, "", pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return nil, "", err
	}
	return f.b.ListPage(ctx, prefix, after, limit)
}

func (f *Fault) Delete(ctx context.Context, key string) error {
	m, hit := f.hit(OpDelete, key)
	if hit && m != Ambiguous {
		return pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
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

func (f *Fault) DeleteMany(ctx context.Context, keys ...string) error {
	m, hit := f.hit(OpDelete, keys...)
	if hit && m != Ambiguous {
		return pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
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

// ListPrefixes is a pass-through: no plan targets it (no crash-point test
// has needed a delimited listing to fail), but it is counted.
func (f *Fault) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	f.count(OpListPrefixes)
	if err := f.shapeCall(ctx, 0); err != nil {
		return nil, "", err
	}
	return f.b.ListPrefixesPage(ctx, prefix, after, limit)
}

func (f *Fault) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	f.noteRead(key)
	if m, hit := f.hit(OpGet, key); hit {
		return nil, "", pre(ctx, m)
	}
	b, etag, err := f.b.GetWithETag(ctx, key)
	if err == nil {
		err = f.shapeCall(ctx, len(b))
	}
	f.read(b)
	return b, etag, err
}

func (f *Fault) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	f.noteRead(key)
	if m, hit := f.hit(OpGet, key); hit {
		return nil, "", false, pre(ctx, m)
	}
	b, current, unchanged, err := f.b.GetIfChanged(ctx, key, etag)
	if err == nil {
		err = f.shapeCall(ctx, len(b))
	}
	f.read(b)
	if err != nil {
		return nil, "", false, err
	}
	return b, current, unchanged, nil
}

func (f *Fault) EnsureBucket(ctx context.Context) error {
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
	}
	return f.b.EnsureBucket(ctx)
}

func (f *Fault) DropBucket(ctx context.Context) error {
	if err := f.shapeCall(ctx, 0); err != nil {
		return err
	}
	return f.b.DropBucket(ctx)
}

func (f *Fault) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	f.noteRead(key)
	if m, hit := f.hitAt(OpGetRange, offset, key); hit {
		return nil, pre(ctx, m)
	}
	b, err := f.b.GetRange(ctx, key, offset, length)
	if err == nil {
		err = f.shapeCall(ctx, len(b))
	}
	f.read(b)
	return b, err
}
