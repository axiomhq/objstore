package lease

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/objstore"
)

// DefaultTTL is a validity interval that tolerates a few slow renewals on
// a public cloud store.
const DefaultTTL = 10 * time.Second

// ErrNotOwner means this process does not hold the lease: somebody else
// does, ours lapsed, was released or fenced, or the handle never held it.
// Retryable by definition: nothing about the request is wrong, this process
// is simply not the holder right now.
var ErrNotOwner = errors.New("lease: not held")

// Store is the part of an objstore.Store a lease uses: a read with the
// ETag and the two conditional writes. *objstore.Store implements it.
// GetWithETag must return an error wrapping objstore.ErrNotFound for a
// missing key.
type Store interface {
	GetWithETag(ctx context.Context, key string) ([]byte, string, error)
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)
	PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error)
}

// Body is the whole object. Expiry is wall-clock and absolute; Nonce
// identifies ONE write attempt, so an ambiguous CAS is attributable.
type Body struct {
	Owner  string    `json:"owner"`
	Nonce  string    `json:"nonce"`
	Expiry time.Time `json:"expiry"`
}

// OwnerID names this process in the bucket: hostname, pid, and a nonce so
// two runs at the same pid after a restart are never confused.
func OwnerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), rand.Text()[:8])
}

// Lease is one key's token held by this process. Several independent
// leases (say, one for writing and one for compacting) are just several
// keys; one process may hold any of them.
type Lease struct {
	store Store
	key   string
	owner string
	ttl   time.Duration

	mu  sync.Mutex
	log *slog.Logger // renewal failures, fences and handovers; nil is silent
	// started: Acquire has begun, so either Acquire itself (its Take failed,
	// or the lease was retired under it) or the renewal goroutine it starts
	// closes done. retire closes done only when started is false.
	started  bool
	cancel   context.CancelFunc // cancels the acquisition or renewer on retirement
	released bool               // Release was called: Valid refuses from here on
	nonce    string             // last attributed acquisition/renewal; owner alone is not continuity
	deadline time.Time          // LOCAL clock: this process stops serving here
	// pending is every write attempt whose answer was lost, by nonce, with
	// the time it started: a record later found in our name with one of
	// these nonces is that write landing late, and it is ours.
	pending map[string]time.Time
	fenced  bool
	// interrupted records that this token's ownership has been BROKEN at
	// least once (released, retired or fenced) and not re-proven since. A
	// writer holding an interrupted lease cannot vouch for its own next
	// sequence, however valid the token looks now, because the log may have
	// moved while it was not the holder. FloorProven clears it.
	interrupted bool
	fence       func() // what losing the lease costs; set by Start
	// owed: a fence came before Start, and holds a place in live for the
	// callback Start will run. Start gives the place back after the
	// callback returns; retire gives it back if Start never came.
	owed bool
	// checkHead runs after a successful renewal, outside mu.
	checkHead func(context.Context) error

	stopOnce sync.Once
	stop     chan struct{}
	doneOnce sync.Once
	done     chan struct{} // the renewer exited (or never ran and the lease ended)
	// live counts what Done waits for: 1 until done closes, plus 1 while a
	// fence callback runs or is owed (fenced before Start). ended (Done)
	// closes when it reaches zero. A callback is claimed only while
	// live > 0, so none starts after Done.
	live  atomic.Int32
	ended chan struct{}
}

// Acquire takes the lease at key and starts renewing it at once: renewal
// has to cover whatever the caller does before it can install a fence
// (a replay, say), which may outlast a TTL. The fence callback arrives
// later, via Start; call it even if nothing needs cancelling (Start(nil)
// works), because a fence before Start keeps Done open until Start runs.
// Every failure path between the two owes the lease a Release. A ttl <= 0
// means DefaultTTL.
//
// Each call uses a fresh handle. After a first write whose outcome was
// lost, a fresh handle cannot recognise that write as its own and waits
// it out (1.5 TTL) like anyone else's; a long-lived caller that retries
// should keep one handle from New and retry its Acquire method instead.
func Acquire(ctx context.Context, s Store, key, owner string, ttl time.Duration) (*Lease, error) {
	l := New(s, key, owner, ttl)
	if err := l.Acquire(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// New returns an unacquired lease on key. Take drives it by hand (no
// renewal goroutine); Acquire takes it and starts renewing. A ttl <= 0
// means DefaultTTL. A handle carries one acquisition. Retry a failed
// Acquire on the same handle, which remembers its unresolved writes and
// adopts one that landed late:
//
//	l := lease.New(s, key, owner, ttl)
//	for {
//		if err := l.Acquire(ctx); err == nil {
//			break
//		}
//		// back off, then retry on l
//	}
//
// A fresh New forgets them and waits out its own record (1.5 TTL).
func New(s Store, key, owner string, ttl time.Duration) *Lease {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	l := &Lease{store: s, key: key, owner: owner, ttl: ttl, stop: make(chan struct{}), done: make(chan struct{}), ended: make(chan struct{})}
	l.live.Store(1)
	return l
}

// Acquire takes l and starts its renewal goroutine. It returns an error,
// and does not touch the store, on a lease that was already acquired or
// has been retired or released: a handle carries one acquisition. A lease
// retired or released while Acquire runs is ErrNotOwner, never a success.
func (l *Lease) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.started || l.stopped() {
		l.mu.Unlock()
		return fmt.Errorf("lease %s: Acquire called twice, or after Release or Retire; use a fresh New", l.key)
	}
	l.started = true
	acquireCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.mu.Unlock()
	err := l.Take(acquireCtx)
	cancel()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped() {
		err = fmt.Errorf("%w: %s was released or retired during Acquire", ErrNotOwner, l.key)
	}
	if err != nil {
		l.started = false
		if l.stopped() { // retired meanwhile, expecting a renewer to close done
			l.finish()
		}
		return err
	}
	renewCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l.cancel = cancel
	go l.renew(renewCtx)
	return nil
}

// SetLogger directs renewal failures and fences to log; nil (the default)
// is silent. Safe to call at any time, including while renewing.
func (l *Lease) SetLogger(log *slog.Logger) {
	l.mu.Lock()
	l.log = log
	l.mu.Unlock()
}

func (l *Lease) logger() *slog.Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.log
}

// stopped reports whether Retire has begun.
func (l *Lease) stopped() bool {
	select {
	case <-l.stop:
		return true
	default:
		return false
	}
}

// finish closes done, once, whichever path ends the lease.
func (l *Lease) finish() {
	l.doneOnce.Do(func() {
		close(l.done)
		l.leave()
	})
}

// join claims a place for a fence callback; false once Done has closed.
func (l *Lease) join() bool {
	for {
		n := l.live.Load()
		if n == 0 {
			return false
		}
		if l.live.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// leave drops a place taken by New or join; the last one closes Done.
func (l *Lease) leave() {
	if l.live.Add(-1) == 0 {
		close(l.ended)
	}
}

// Start installs the fence callback, running it immediately (on the
// caller's goroutine) if the lease was fenced in the meantime and not
// since released or retired. Call it once, after Acquire; a nil fence
// installs an empty callback. A later fence runs the callback on its own
// goroutine, so the callback may call anything on the lease, Release and
// Retire included; Retire and Release do not wait for it, and it must not
// wait on Done(), which waits for it. A Release or Retire never runs it.
// For a Shared lease, call Start only after Join returns: an immediate
// callback that calls Join from inside mint would wait on its own mint.
func (l *Lease) Start(fence func()) {
	if fence == nil {
		fence = func() {}
	}
	l.mu.Lock()
	l.fence = fence
	owed := l.owed
	l.owed = false
	l.mu.Unlock()
	if owed {
		defer l.leave() // the place fenceFor took for this callback
		fence()
	}
}

// CheckHeadOnRenewal installs a probe that runs after every successful
// renewal CAS (not the initial acquisition), so a holder can notice a
// foreign write before its next admitted one. Its error becomes Take's
// error. The renewer fences at once only on an error wrapping
// ErrNotOwner. Any other error is logged and retried once; since the CAS
// before it already extended the lease, it never fences by itself.
func (l *Lease) CheckHeadOnRenewal(check func(context.Context) error) {
	l.mu.Lock()
	l.checkHead = check
	l.mu.Unlock()
}

// Take is one acquire-or-renew cycle: read the lease, decide whether it is
// ours to write, CAS it forward. Three outcomes: nil (held), ErrNotOwner
// (proven somebody else's), anything else (unresolved: retry, never
// assume).
func (l *Lease) Take(ctx context.Context) error {
	if l.stopped() {
		return fmt.Errorf("%w: %s was released or retired in this process", ErrNotOwner, l.key)
	}
	l.mu.Lock()
	nonce, deadline, fenced := l.nonce, l.deadline, l.fenced
	l.mu.Unlock()
	now := time.Now()
	if fenced || (nonce != "" && !Before(now, now.Round(0), deadline)) {
		return fmt.Errorf("%w: %s cannot renew an abandoned lease", ErrNotOwner, l.key)
	}
	initial := nonce == ""
	ctx = objstore.Urgent(ctx) // the heartbeat never queues behind bulk traffic
	cur, etag, err := Load(ctx, l.store, l.key)
	if err != nil {
		return fmt.Errorf("lease %s: %w", l.key, err)
	}
	// A record in our name carrying the nonce of an attempt whose answer
	// was lost is that attempt landing late (the provider stalled the
	// request, then completed it). Adopt it: it is this acquisition,
	// renewed. Seen on Hetzner Object Storage, where the lease fenced
	// itself as "taken by" its own owner string. This holds for a retried
	// first acquisition too: otherwise our own record locks us out.
	if cur.Owner == l.owner && cur.Nonce != nonce {
		if start, ok := l.pendingStart(cur.Nonce); ok {
			err := l.hold(start, cur.Nonce)
			switch {
			case err == nil:
				nonce = cur.Nonce
			case initial:
				// Ours, but landed too late to count: an expired record
				// like any other, taken over below once it is free.
				l.mu.Lock()
				delete(l.pending, cur.Nonce)
				l.mu.Unlock()
			default:
				return err
			}
		}
	}
	// Renewal must extend this exact acquisition, never acquire a free
	// lease after an intervening owner wrote and released it.
	if nonce != "" && (cur.Nonce != nonce || cur.Owner != l.owner) {
		return fmt.Errorf("%w: %s changed since its last renewal", ErrNotOwner, l.key)
	}
	// A fresh acquisition waits an extra half TTL for wall-clock skew.
	// Equal owner strings do not authorize replacing another acquisition. A
	// released lease (Expiry zero) and a lapsed one are both free here.
	if etag != "" && nonce == "" {
		if takeAfter := cur.Expiry.Add(l.ttl / 2); time.Now().Before(takeAfter) {
			return fmt.Errorf("%w: %s is held by %s until %s", ErrNotOwner, l.key, cur.Owner, takeAfter.UTC().Format(time.RFC3339Nano))
		}
	}
	if err := l.write(ctx, etag); err != nil {
		return err
	}
	if initial {
		return nil // the caller follows an initial acquisition with its own catch-up
	}
	l.mu.Lock()
	checkHead := l.checkHead
	l.mu.Unlock()
	if checkHead != nil {
		return checkHead(ctx)
	}
	return nil
}

// write CASes a fresh body in over etag ("" = nothing there, PutIfAbsent)
// and attributes an ambiguous or lost CAS by reading our nonce back.
func (l *Lease) write(ctx context.Context, etag string) error {
	// Timed from BEFORE the PUT: our local stop must never fall later than
	// the expiry a taker reads out of the object.
	start := time.Now()
	body := Body{Owner: l.owner, Nonce: rand.Text(), Expiry: start.Add(l.ttl)}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var ok bool
	if etag == "" {
		ok, err = l.store.PutIfAbsent(ctx, l.key, data)
	} else {
		ok, err = l.store.PutIfMatch(ctx, l.key, data, etag)
	}
	if err == nil && ok {
		return l.hold(start, body.Nonce)
	}
	// A lost CAS or an ambiguous transport error. Read back and attribute by
	// Nonce: our own bytes mean the write landed and only the answer was
	// lost, which is a held lease, not a lost one.
	back, _, gerr := Load(ctx, l.store, l.key)
	if gerr != nil {
		l.notePending(body.Nonce, start)
		if err != nil {
			return fmt.Errorf("lease %s: outcome unknown: PUT: %w; read-back: %w", l.key, err, gerr)
		}
		return fmt.Errorf("lease %s: outcome unknown: read-back: %w", l.key, gerr)
	}
	if back.Nonce == body.Nonce {
		return l.hold(start, body.Nonce)
	}
	if err != nil {
		l.notePending(body.Nonce, start)
		return fmt.Errorf("lease %s: outcome unknown: %w", l.key, err)
	}
	// Our CAS lost to our own earlier attempt landing late: held, not lost.
	if back.Owner == l.owner {
		if st, ok := l.pendingStart(back.Nonce); ok {
			return l.hold(st, back.Nonce)
		}
	}
	return fmt.Errorf("%w: %s was taken by %s", ErrNotOwner, l.key, back.Owner)
}

// notePending records a write attempt whose answer was lost.
func (l *Lease) notePending(nonce string, start time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pending == nil {
		l.pending = map[string]time.Time{}
	}
	// An attempt past its local interval cannot be adopted, even while
	// its stored expiry still keeps other takers out for half a TTL.
	now := time.Now()
	for n, st := range l.pending {
		if !Before(now, now.Round(0), st.Add(l.ttl)) {
			delete(l.pending, n)
		}
	}
	l.pending[nonce] = start
}

// pendingStart is when the lost-answer attempt with this nonce began.
func (l *Lease) pendingStart(nonce string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	start, ok := l.pending[nonce]
	return start, ok
}

// hold records a landed write. A write that landed while the lease was
// being released or retired still records its nonce, so Release's
// handover CAS finds it, but it does not extend the local deadline and
// reports ErrNotOwner: the process has already let go.
func (l *Lease) hold(start time.Time, nonce string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.fenced || !Before(now, now.Round(0), start.Add(l.ttl)) || (l.nonce != "" && !Before(now, now.Round(0), l.deadline)) {
		return fmt.Errorf("%w: %s acquisition completed after its validity interval", ErrNotOwner, l.key)
	}
	l.nonce = nonce
	l.pending = nil // every earlier attempt is superseded by this one
	// Release retires too, so stopped covers released.
	if l.stopped() {
		return fmt.Errorf("%w: %s was released or retired while this write was in flight", ErrNotOwner, l.key)
	}
	l.deadline = start.Add(l.ttl)
	return nil
}

// Valid reports whether this process may still act as the holder. Ask it
// before every guarded action.
//
// A nil *Lease is a handle that never held the lease (a read-only replica,
// say): it is never valid.
func (l *Lease) Valid() error {
	if l == nil {
		return fmt.Errorf("%w: this process holds no lease; only the holder writes", ErrNotOwner)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return fmt.Errorf("%w: %s was released in this process; a fresh acquisition is needed", ErrNotOwner, l.key)
	}
	if l.stopped() {
		return fmt.Errorf("%w: %s was retired in this process; a fresh acquisition is needed", ErrNotOwner, l.key)
	}
	if l.fenced {
		return fmt.Errorf("%w: %s was fenced in this process; a fresh acquisition is needed", ErrNotOwner, l.key)
	}
	if l.deadline.IsZero() {
		return fmt.Errorf("%w: %s was never acquired by this handle", ErrNotOwner, l.key)
	}
	now := time.Now()
	if !Before(now, now.Round(0), l.deadline) {
		return fmt.Errorf("%w: %s lapsed at %s", ErrNotOwner, l.key, l.deadline.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// Before reports now < deadline on both clocks. Test hook: exported so
// tests can model a suspended host; production code has no use for it. Local elapsed time can
// pause during suspend while the persisted wall expiry advances, so both
// must remain live. Separate readings let tests model suspend without
// changing the host clock; callers derive both from one sample
// (now, now.Round(0)).
func Before(now, wallNow, deadline time.Time) bool {
	return now.Before(deadline) && wallNow.Before(deadline.Round(0))
}

// renew re-takes the lease every TTL/4 for as long as it is ours. It fences
// on proof (a foreign owner) or on a lapse (renewals kept failing until the
// local deadline passed): one slow call, or two, is just a retry.
func (l *Lease) renew(ctx context.Context) {
	defer l.finish()
	defer l.cancel()
	every := l.ttl / 4
	if every <= 0 {
		every = time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
		}
		// A lease that has ALREADY lapsed locally is never renewed. Inside
		// the gap another process could have taken it, written, and let it
		// go again, and a renewal that then found the object free would
		// resume serving state missing those writes. Only a fresh
		// acquisition, followed by the caller's catch-up, may pick it back up.
		if err := l.Valid(); err != nil {
			if !l.stopped() {
				l.fenceFor(err)
			}
			return
		}
		// Bounded: a hung store makes a renewal unresolved, never a pinned
		// goroutine. Keep the acquisition's values, not its cancellation:
		// the lease outlives any one request. Half a TTL for the attempt
		// (the tick is a quarter), and a second attempt straight away on a
		// failure that was not a refusal: a renewal that only stalled must
		// not wait a whole tick to try again.
		var err error
		for attempt := 0; attempt < 2; attempt++ {
			ctx, cancel := context.WithTimeout(ctx, l.ttl/2)
			t0 := time.Now()
			err = l.Take(ctx)
			cancel()
			if err == nil || errors.Is(err, ErrNotOwner) {
				break // a refusal is logged once, as the fence below
			}
			if log := l.logger(); log != nil {
				log.Warn("lease renewal failed", "key", l.key, "attempt", attempt+1, "took", time.Since(t0).Round(time.Millisecond), "err", err)
			}
		}
		if err == nil {
			continue
		}
		// Released or retired while the renewal ran: a deliberate stop,
		// not a lost lease. Never fence it.
		if l.stopped() {
			return
		}
		if errors.Is(err, ErrNotOwner) {
			l.fenceFor(err)
			return
		}
		if verr := l.Valid(); verr != nil {
			l.fenceFor(fmt.Errorf("%w (last renewal: %w)", verr, err))
			return
		}
	}
}

// Fence ends the lease in this process for good: Valid refuses until a
// fresh acquisition. The fence callback runs once, on the first Fence, on
// a goroutine of its own (or on Start's, if Start has not been called
// yet); a Fence after Release, Retire or Done runs none.
func (l *Lease) Fence() { l.fenceFor(errors.New("fenced by caller")) }

// fenceFor is Fence with the reason logged at Warn. The callback runs on
// its own goroutine: it may Release, which waits for the renewer, which
// may be the caller here.
func (l *Lease) fenceFor(reason error) {
	l.mu.Lock()
	already, stopped, fn, log := l.fenced, l.stopped(), l.fence, l.log
	l.fenced, l.interrupted = true, true
	// Claimed under mu, so retire (which closes stop and gives back an owed
	// place under mu) and Start (which takes it under mu) see it whole. With
	// no callback yet, the place is owed: Done stays open for Start.
	claimed := !already && !stopped && l.join()
	if claimed && fn == nil {
		l.owed = true
	}
	l.mu.Unlock()
	if already || stopped {
		return // nothing to run or log: fenced before, or released or retired
	}
	if log != nil {
		log.Warn("lease fenced", "key", l.key, "owner", l.owner, "reason", reason)
	}
	if claimed && fn != nil {
		go func() {
			defer l.leave()
			fn()
		}()
	}
}

// Retire stops renewing without touching the object. The lease then simply
// expires for whoever wants it next. Valid returns ErrNotOwner from then on,
// although the stored expiry may not have passed: a retired holder must not
// act on time it no longer renews. It cancels any in-flight acquisition
// or renewal and waits for it to exit (at most about one TTL), not for a
// fence callback. A fence callback owed to Start (the lease was fenced
// before Start) is given up: Start will not run it. A nil lease is a no-op.
func (l *Lease) Retire() {
	if l == nil {
		return
	}
	l.retire(false)
	<-l.done
}

// retire stops renewal and, when no renewer was ever started, ends the
// lease itself. released also marks it released, so Valid refuses at once.
// A fence callback still owed to Start is given up: Start will not run it.
// It does not wait.
func (l *Lease) retire(released bool) {
	l.mu.Lock()
	l.interrupted = true
	l.released = l.released || released
	l.stopOnce.Do(func() { close(l.stop) })
	if l.cancel != nil {
		l.cancel()
	}
	if !l.started {
		l.finish()
	}
	if l.owed {
		l.owed = false
		l.leave()
	}
	l.mu.Unlock()
}

// Continuous reports that this process has held the token without a break
// since it was acquired: valid now, never fenced, never retired or
// released, and not waiting to re-prove itself (FloorProven). It is a
// purely LOCAL question and the only claim a writer can make about its own
// next sequence without a store read.
func (l *Lease) Continuous() bool {
	if l == nil {
		return false
	}
	if err := l.Valid(); err != nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.interrupted
}

// FloorProven records that the holder checked its position against the
// durable state and the check passed, which closes an interruption:
// nothing can have overtaken this holder between that read and its own
// next sequence without taking the lease, which fences it again.
func (l *Lease) FloorProven() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.interrupted = false
	l.mu.Unlock()
}

// Release is Retire plus a handover: a CAS writing an already-expired body,
// so the next process takes over immediately instead of waiting out the TTL.
// Best effort, bounded by ctx and by the TTL; a release that does not land
// just means the lease expires on its own. If ctx ends while the renewal
// goroutine is still finishing an attempt, Release stops the lease locally
// and skips the handover. The ETag CAS is what makes it safe to call on a
// lease we may have already lost. A skipped or failed handover is logged
// (SetLogger). Release never runs the fence callback and does not wait
// for one already running. A nil lease is a no-op.
func (l *Lease) Release(ctx context.Context) {
	if l == nil {
		return
	}
	ctx, cancel := context.WithTimeout(objstore.Urgent(ctx), l.ttl)
	defer cancel()
	// Local first, and unconditionally: having promised the lease to
	// whoever takes it next, this process must stop acting on it now, not
	// when the deadline it last renewed happens to run out. Valid refuses
	// a released lease; a renewal still in flight records its nonce (so the
	// handover below finds it) but cannot revive the lease.
	l.retire(true)
	log := l.logger()
	select {
	case <-l.done: // prefer done: a finished renewer is never "still finishing"
	default:
		select {
		case <-l.done:
		case <-ctx.Done():
			if log != nil {
				log.Warn("lease handover skipped: renewal still finishing", "key", l.key, "owner", l.owner, "err", ctx.Err())
			}
			return
		}
	}
	l.mu.Lock()
	nonce, unresolved := l.nonce, len(l.pending)
	l.mu.Unlock()
	if nonce == "" && unresolved == 0 {
		return // never held: nothing to hand over
	}
	// Urgent like Take: the handover must not queue behind bulk writes, or
	// the next holder waits the TTL instead of taking over now.
	cur, etag, err := Load(ctx, l.store, l.key)
	if err != nil {
		if log != nil {
			log.Warn("lease handover failed", "key", l.key, "owner", l.owner, "err", err)
		}
		return
	}
	// Ours: the last attributed write, or one whose answer was lost and
	// which landed after all.
	_, late := l.pendingStart(cur.Nonce)
	if etag == "" || cur.Owner != l.owner || (cur.Nonce != nonce && !late) {
		if log != nil {
			log.Info("lease handover skipped: no longer ours", "key", l.key, "owner", l.owner, "holder", cur.Owner)
		}
		return
	}
	cur.Expiry = time.Time{}
	data, err := json.Marshal(cur)
	if err == nil {
		var ok bool
		ok, err = l.store.PutIfMatch(ctx, l.key, data, etag)
		if err == nil && !ok {
			err = errors.New("lease changed before the handover CAS")
		}
	}
	if err != nil && log != nil {
		log.Warn("lease handover failed", "key", l.key, "owner", l.owner, "err", err)
	}
}

// Load reads the lease object and its ETag. A missing lease is (zero, "",
// nil): free. A CORRUPT lease is (zero, etag, nil): free, but taken over by
// CAS on the etag just read, so garbage at the key self-heals instead of
// blocking the key forever.
func Load(ctx context.Context, s Store, key string) (Body, string, error) {
	data, etag, err := s.GetWithETag(ctx, key)
	if errors.Is(err, objstore.ErrNotFound) {
		return Body{}, "", nil
	}
	if err != nil {
		return Body{}, "", err
	}
	var b Body
	if err := json.Unmarshal(data, &b); err != nil {
		return Body{}, etag, nil
	}
	return b, etag, nil
}

// Steal writes the lease at key in owner's name, bypassing local
// continuity, so tests and operators can simulate another process taking
// it. The victim's next renewal fails with ErrNotOwner. It retries while
// the victim's own CAS moves the ETag. Test hook: it breaks the protocol
// on purpose.
func Steal(ctx context.Context, s Store, key, owner string, ttl time.Duration) error {
	l := New(s, key, owner, ttl)
	l.finish() // no renewal goroutine: this lease is scenery
	return l.Steal(ctx)
}

// Steal rewrites l's own record under a fresh nonce, bypassing local
// continuity: the handle stays valid for its holder while any other
// holder's next renewal fails. Retried while a concurrent CAS moves the
// ETag. Test hook: it breaks the protocol on purpose.
func (l *Lease) Steal(ctx context.Context) error {
	if l.stopped() {
		return fmt.Errorf("%w: %s was released or retired in this process", ErrNotOwner, l.key)
	}
	var err error
	for range 20 {
		var etag string
		if _, etag, err = Load(ctx, l.store, l.key); err != nil {
			return err
		}
		l.mu.Lock()
		l.nonce = ""
		l.fenced = false
		l.mu.Unlock()
		if err = l.write(ctx, etag); err == nil {
			return nil
		}
	}
	return fmt.Errorf("steal %s: %w", l.key, err)
}

// Key is the object the lease lives at.
func (l *Lease) Key() string { return l.key }

// Owner is the owner string this lease writes.
func (l *Lease) Owner() string { return l.owner }

// Store is the store the lease lives in.
func (l *Lease) Store() Store { return l.store }

// TTL is the lease's validity interval.
func (l *Lease) TTL() time.Duration { return l.ttl }

// Done is closed when the lease has ended in this process and no fence
// callback is running or owed. Both must hold:
//
//   - the renewal goroutine exited, or, for a lease with no renewer (driven
//     by Take), it was retired or released;
//   - every fence callback has returned, including one Start runs itself
//     because the fence came first. A fence before Start keeps Done open
//     until Start's callback returns, or until Release or Retire gives the
//     callback up; a lease fenced and then never started, released nor
//     retired never closes Done.
//
// A fence after Done has closed runs no callback.
func (l *Lease) Done() <-chan struct{} { return l.ended }

// Expire moves the lease's local deadline into the past, so its next
// validity check fails as if the TTL had elapsed. Test hook: for tests of
// the fences that rest on the lease.
func (l *Lease) Expire() {
	l.mu.Lock()
	l.deadline = time.Now().Add(-time.Second)
	l.mu.Unlock()
}
