package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/axiomhq/objstore"
)

// DefaultTTL is a validity interval that tolerates a few slow renewals on
// a public cloud store.
const DefaultTTL = 10 * time.Second

// ErrNotOwner means this process does not hold the lease: either somebody
// else does, or ours lapsed and has not been re-taken. Retryable by
// definition: nothing about the request is wrong, this process is simply not
// the holder right now.
var ErrNotOwner = errors.New("lease: held elsewhere")

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
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), mint()[:8])
}

// mint returns a fresh nonce for one write attempt.
func mint() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic("lease: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// Lease is one key's token held by this process. Several independent
// leases (say, one for writing and one for compacting) are just several
// keys; one process may hold any of them.
type Lease struct {
	store *objstore.Store
	key   string
	owner string
	ttl   time.Duration
	// Log receives renewal failures; nil is silent. Set it before the
	// first renewal tick (TTL/4 after Acquire).
	Log *slog.Logger

	mu       sync.Mutex
	nonce    string    // last attributed acquisition/renewal; owner alone is not continuity
	deadline time.Time // LOCAL clock: this process stops serving here
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
	// checkHead runs after a successful renewal, outside mu.
	checkHead func(context.Context) error

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// Acquire takes the lease at key and starts renewing it at once: renewal
// has to cover whatever the caller does before it can install a fence
// (a replay, say), which may outlast a TTL. The fence callback arrives
// later, via Start. Every failure path between the two owes the lease a
// Release.
func Acquire(ctx context.Context, s *objstore.Store, key, owner string, ttl time.Duration) (*Lease, error) {
	return New(s, key, owner, ttl).Acquire(ctx)
}

// New returns an unacquired lease on key. Take drives it by hand (no
// renewal goroutine); Acquire takes it and starts renewing.
func New(s *objstore.Store, key, owner string, ttl time.Duration) *Lease {
	return &Lease{store: s, key: key, owner: owner, ttl: ttl, stop: make(chan struct{}), done: make(chan struct{})}
}

// Acquire takes l and starts its renewal goroutine.
func (l *Lease) Acquire(ctx context.Context) (*Lease, error) {
	if err := l.Take(ctx); err != nil {
		return nil, err
	}
	go l.renew()
	return l, nil
}

// Start installs the fence callback, running it immediately if the lease
// was already lost in the meantime. Call it once, after Acquire.
func (l *Lease) Start(fence func()) {
	l.mu.Lock()
	l.fence = fence
	already := l.fenced
	l.mu.Unlock()
	if already {
		fence()
	}
}

// CheckHeadOnRenewal installs a probe that runs after every successful
// renewal (not the initial acquisition). Its error is the renewal's error:
// a holder can use it to notice a foreign write before its next admitted
// one.
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
	l.mu.Lock()
	nonce, deadline, fenced := l.nonce, l.deadline, l.fenced
	l.mu.Unlock()
	now := time.Now()
	if fenced || (nonce != "" && !Before(now, now.Round(0), deadline)) {
		return fmt.Errorf("%w: %s cannot renew an abandoned lease", ErrNotOwner, l.key)
	}
	ctx = objstore.Urgent(ctx) // the heartbeat never queues behind bulk traffic
	cur, etag, err := Load(ctx, l.store, l.key)
	if err != nil {
		return fmt.Errorf("lease %s: %w", l.key, err)
	}
	// A record in our name carrying the nonce of an attempt whose answer
	// was lost is that attempt landing late (the provider stalled the
	// request, then completed it). Adopt it: it is this acquisition,
	// renewed. Seen on Hetzner Object Storage, where the lease fenced
	// itself as "taken by" its own owner string.
	if nonce != "" && cur.Owner == l.owner && cur.Nonce != nonce {
		if start, ok := l.pendingStart(cur.Nonce); ok {
			if err := l.hold(start, cur.Nonce); err != nil {
				return err
			}
			nonce = cur.Nonce
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
	if nonce == "" {
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
	body := Body{Owner: l.owner, Nonce: mint(), Expiry: start.Add(l.ttl)}
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
		return fmt.Errorf("lease %s: outcome unknown: %v", l.key, gerr)
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
	l.pending[nonce] = start
}

// pendingStart is when the lost-answer attempt with this nonce began.
func (l *Lease) pendingStart(nonce string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	start, ok := l.pending[nonce]
	return start, ok
}

func (l *Lease) hold(start time.Time, nonce string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.fenced || !Before(now, now.Round(0), start.Add(l.ttl)) || (l.nonce != "" && !Before(now, now.Round(0), l.deadline)) {
		return fmt.Errorf("%w: %s acquisition completed after its validity interval", ErrNotOwner, l.key)
	}
	l.nonce = nonce
	l.deadline = start.Add(l.ttl)
	l.pending = nil // every earlier attempt is superseded by this one
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
	if l.fenced {
		return fmt.Errorf("%w: %s was fenced in this process; a fresh acquisition is needed", ErrNotOwner, l.key)
	}
	now := time.Now()
	if !Before(now, now.Round(0), l.deadline) {
		return fmt.Errorf("%w: %s lapsed at %s", ErrNotOwner, l.key, l.deadline.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// Before reports now < deadline on both clocks. Local elapsed time can
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
func (l *Lease) renew() {
	defer close(l.done)
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
		if l.Valid() != nil {
			l.Fence()
			return
		}
		// Bounded: a hung store makes a renewal unresolved, never a pinned
		// goroutine. Background context on purpose: the lease outlives any
		// one request. Half a TTL for the attempt (the tick is a quarter),
		// and a second attempt straight away on a failure that was not a
		// refusal: a renewal that only stalled must not wait a whole tick to
		// try again.
		var err error
		for attempt := 0; attempt < 2; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), l.ttl/2)
			t0 := time.Now()
			err = l.Take(ctx)
			cancel()
			if err == nil {
				break
			}
			if l.Log != nil {
				l.Log.Warn("lease renewal failed", "key", l.key, "attempt", attempt+1, "took", time.Since(t0).Round(time.Millisecond), "err", err)
			}
			if errors.Is(err, ErrNotOwner) {
				break
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, ErrNotOwner) || l.Valid() != nil {
			l.Fence()
			return
		}
	}
}

// Fence retires the lease in this process for good: Valid refuses until a
// fresh acquisition. The fence callback runs once, on the first Fence.
func (l *Lease) Fence() {
	l.mu.Lock()
	already, fn := l.fenced, l.fence
	l.fenced, l.interrupted = true, true
	l.mu.Unlock()
	if !already && fn != nil {
		fn()
	}
}

// Retire stops renewing without touching the object. The lease then simply
// expires for whoever wants it next. Never call it from the fence callback:
// it joins the renewal goroutine. A nil lease is a no-op.
func (l *Lease) Retire() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.interrupted = true
	l.mu.Unlock()
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
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
// Best effort and bounded by the TTL; a release that does not land just
// means the lease expires on its own. The ETag CAS is what makes it safe to
// call on a lease we may have already lost. A nil lease is a no-op.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.Retire()
	// Local first, and unconditionally: having promised the lease to
	// whoever takes it next, this process must stop acting on it now, not
	// when the deadline it last renewed happens to run out.
	l.mu.Lock()
	l.deadline = time.Time{}
	l.mu.Unlock()
	// Urgent like Take: the handover must not queue behind bulk writes, or
	// the next holder waits the TTL instead of taking over now.
	ctx, cancel := context.WithTimeout(objstore.Urgent(context.Background()), l.ttl)
	defer cancel()
	cur, etag, err := Load(ctx, l.store, l.key)
	if err != nil || etag == "" || cur.Owner != l.owner || cur.Nonce != l.nonce {
		return
	}
	cur.Expiry = time.Time{}
	data, err := json.Marshal(cur)
	if err != nil {
		return
	}
	l.store.PutIfMatch(ctx, l.key, data, etag) //nolint:errcheck // best effort by contract
}

// Load reads the lease object and its ETag. A missing lease is (zero, "",
// nil): free. A CORRUPT lease is (zero, etag, nil): free, but taken over by
// CAS on the etag just read, so garbage at the key self-heals instead of
// blocking the key forever.
func Load(ctx context.Context, s *objstore.Store, key string) (Body, string, error) {
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
// the victim's own CAS moves the ETag.
func Steal(ctx context.Context, s *objstore.Store, key, owner string, ttl time.Duration) error {
	l := New(s, key, owner, ttl)
	close(l.done) // no renewal goroutine: this lease is scenery
	return l.Steal(ctx)
}

// Steal rewrites l's own record under a fresh nonce, bypassing local
// continuity: the handle stays valid for its holder while any other
// holder's next renewal fails. Retried while a concurrent CAS moves the
// ETag.
func (l *Lease) Steal(ctx context.Context) error {
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
func (l *Lease) Store() *objstore.Store { return l.store }

// TTL is the lease's validity interval.
func (l *Lease) TTL() time.Duration { return l.ttl }

// Done is closed when the renewal goroutine has exited.
func (l *Lease) Done() <-chan struct{} { return l.done }

// Expire moves the lease's local deadline into the past, so its next
// validity check fails as if the TTL had elapsed. For tests of the fences
// that rest on the lease.
func (l *Lease) Expire() {
	l.mu.Lock()
	l.deadline = time.Now().Add(-time.Second)
	l.mu.Unlock()
}
