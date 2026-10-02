package lease

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/axiomhq/objstore"

	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestLease(t *testing.T) {
	ctx := context.Background()
	const ttl = 300 * time.Millisecond

	runUnit(t, "LocalExpiry", func(t *testing.T) {
		l := New(newMemStore(), "expiry/lease", "owner", ttl)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		defer l.Release(ctx)
		start := time.Now()
		// Go 1.26 omits monotonic fields in bubbles; Before uses the fake wall clock.
		time.Sleep(ttl - time.Nanosecond)
		if err := l.Valid(); err != nil {
			t.Fatalf("lease expired before its deadline: %v", err)
		}
		time.Sleep(time.Nanosecond)
		if elapsed := time.Since(start); elapsed != ttl {
			t.Fatalf("elapsed = %v, want %v", elapsed, ttl)
		}
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("lease valid at its deadline: %v", err)
		}
	})

	runUnit(t, "AcquireRenewExpireTakeover", func(t *testing.T) {
		s := newMemStore()
		// First acquire: PutIfAbsent, nothing there yet.
		a, err := Acquire(ctx, s, "a/lease", "owner-a", ttl)
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}
		a.Start(func() {})
		if err := a.Valid(); err != nil {
			t.Fatalf("fresh lease is not valid: %v", err)
		}
		// A live lease refuses everyone else, and keeps refusing while a's
		// renewal (TTL/4) keeps it alive well past one TTL.
		if _, err := Acquire(ctx, s, "a/lease", "owner-b", ttl); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("second acquire against a live lease: %v, want ErrNotOwner", err)
		}
		time.Sleep(2 * ttl)
		synctest.Wait()
		if err := a.Valid(); err != nil {
			t.Fatalf("lease lapsed despite renewal: %v", err)
		}
		if _, err := Acquire(ctx, s, "a/lease", "owner-b", ttl); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("acquire against a RENEWED lease: %v, want ErrNotOwner", err)
		}
		// a is partitioned: it stops renewing without releasing. Nothing may
		// take the lease until its persisted expiry plus takeover grace.
		a.Retire()
		if _, err := Acquire(ctx, s, "a/lease", "owner-b", ttl); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("takeover BEFORE expiry: %v, want ErrNotOwner", err)
		}
		time.Sleep(2 * ttl)
		b, err := Acquire(ctx, s, "a/lease", "owner-b", ttl)
		if err != nil {
			t.Fatalf("takeover after expiry: %v", err)
		}
		b.Start(func() {})
		defer b.Release(ctx)
		// a stopped renewing at retire, so its own local deadline is long
		// gone: it would refuse to serve even before it learned about b.
		if err := a.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("a still considers itself the owner: %v", err)
		}
	})

	// Ambiguous CAS: the PUT lands and the answer is lost. The nonce
	// read-back is what turns that into a held lease instead of a spurious
	// ErrNotOwner.
	runUnit(t, "AmbiguousTakeover", func(t *testing.T) {
		s, f := storetest.Faulty(t, newMemStore())
		b, err := Acquire(ctx, s, "amb/lease", "owner-b", ttl)
		if err != nil {
			t.Fatal(err)
		}
		b.Retire()
		time.Sleep(2 * ttl)
		f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Mode: storetest.Ambiguous, Key: "amb/lease"})
		c, err := Acquire(ctx, s, "amb/lease", "owner-c", ttl)
		if f.Fired() != 1 {
			t.Fatalf("the ambiguous CAS never fired (fired=%d)", f.Fired())
		}
		f.Clear()
		if err != nil {
			t.Fatalf("acquire through an ambiguous CAS: %v; its own bytes at the key are a HELD lease", err)
		}
		c.Release(ctx)
	})

	runUnit(t, "AmbiguousFirstAcquire", func(t *testing.T) {
		s, f := storetest.Faulty(t, newMemStore())
		f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Ambiguous, Key: "fresh/lease"})
		fr, err := Acquire(ctx, s, "fresh/lease", "owner-a", ttl)
		if f.Fired() != 1 {
			t.Fatalf("the ambiguous first acquire never fired (fired=%d)", f.Fired())
		}
		f.Clear()
		if err != nil {
			t.Fatalf("first acquire through an ambiguous PutIfAbsent: %v", err)
		}
		fr.Release(ctx)
	})

	// The fence, from the renewal side: a lease stolen while its owner is
	// renewing must fence that owner on its next renewal.
	runUnit(t, "StolenFences", func(t *testing.T) {
		s := newMemStore()
		var logs lockedBuffer
		fenced := make(chan struct{})
		d, err := Acquire(ctx, s, "b/lease", "owner-d", ttl)
		if err != nil {
			t.Fatal(err)
		}
		d.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		d.Start(func() { close(fenced) })
		if err := Steal(ctx, s, "b/lease", "owner-e", ttl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-fenced:
		case <-time.After(10 * ttl):
			t.Fatal("an owner whose lease was taken never fenced itself")
		}
		if err := d.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("fenced lease still valid: %v", err)
		}
		await(t, d.Done(), "Done after a fence")
		if got := logs.String(); !strings.Contains(got, "level=WARN msg=\"lease fenced\"") || !strings.Contains(got, "reason=") {
			t.Fatalf("fence not logged at Warn with a reason:\n%s", got)
		}
		d.Release(ctx) // a no-op release: the object is owner-e's now
		cur, _, err := Load(ctx, s, "b/lease")
		if err != nil || cur.Owner != "owner-e" {
			t.Fatalf("release from a fenced lease clobbered the new owner: %+v (%v)", cur, err)
		}
	})

	runUnit(t, "RenewalAttributesALateLandingWrite", func(t *testing.T) {
		s := newMemStore()
		ttl := 2 * time.Second
		l := New(s, "a/lease", "owner-a", ttl)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		// The lost attempt: its bytes reach the store, its start is noted as
		// pending, and nothing else about it is known to the process.
		start := time.Now()
		late := Body{Owner: "owner-a", Nonce: rand.Text(), Expiry: start.Add(ttl)}
		plant(t, s, "a/lease", late)
		l.notePending(late.Nonce, start)
		if err := l.Take(ctx); err != nil {
			t.Fatalf("renewal after our own late-landing write: %v", err)
		}
		if l.nonce == late.Nonce {
			t.Fatal("the renewal adopted the late nonce without writing a fresh one")
		}
		if err := l.Valid(); err != nil {
			t.Fatal(err)
		}
		// A record in our name with a nonce we never wrote is not ours: the
		// conservative answer is still ErrNotOwner.
		plant(t, s, "a/lease", Body{Owner: "owner-a", Nonce: rand.Text(), Expiry: time.Now().Add(ttl)})
		if err := l.Take(ctx); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("a nonce this process never wrote must not be adopted: %v", err)
		}
		l.Release(ctx)
	})

	// The first acquisition landed but both its answer and the
	// read-back were lost. A retried Take must adopt our own record, not
	// wait 1.5 TTL for it as if somebody else held it.
	runUnit(t, "RetriedTakeAfterUnresolvedAcquire", func(t *testing.T) {
		ls := &lossy{Store: newMemStore()}
		ls.loseNextPut()
		l := New(ls, "retry/lease", "owner-a", time.Minute)
		if err := l.Take(ctx); err == nil || errors.Is(err, ErrNotOwner) {
			t.Fatalf("first Take: %v, want an unresolved outcome", err)
		}
		if err := l.Take(ctx); err != nil {
			t.Fatalf("retried Take locked itself out: %v", err)
		}
		if err := l.Valid(); err != nil {
			t.Fatal(err)
		}
		l.Release(ctx)
		if cur, _, err := Load(ctx, ls, "retry/lease"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("release did not hand back the adopted lease: %+v %v", cur, err)
		}
	})

	runUnit(t, "TakeoverHonorsClockSkewMargin", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		s := newMemStore()
		const ttl = time.Minute
		// Drive the holder by hand: Retire would invalidate it, not merely
		// freeze renewal while the test changes the two clock readings.
		a := New(s, "skew/lease", "owner", ttl)
		if err := a.Take(ctx); err != nil {
			t.Fatal(err)
		}
		defer a.Release(ctx)
		// The persisted wall-clock expiry is behind the taker's clock, while
		// the holder's monotonic validity interval has not elapsed. Skew is
		// < TTL/2.
		a.mu.Lock()
		a.deadline = time.Now().Add(ttl / 8)
		a.mu.Unlock()
		body, _, err := Load(ctx, s, a.key)
		if err != nil {
			t.Fatal(err)
		}
		body.Expiry = time.Now().Add(-ttl / 8)
		plant(t, s, a.key, body)
		b, err := Acquire(ctx, s, "skew/lease", "taker", ttl)
		if b != nil {
			defer b.Release(ctx)
		}
		if !errors.Is(err, ErrNotOwner) {
			t.Fatalf("takeover overlapped a still-valid holder: %v", err)
		}
		if err := a.Valid(); err != nil {
			t.Fatalf("holder fixture expired: %v", err)
		}
	})

	// A lease driven by Take has no renewer; Retire and Release must
	// still end it instead of waiting forever on done.
	runUnit(t, "NewTakeRelease", func(t *testing.T) {
		s := newMemStore()
		l := New(s, "take/lease", "owner-a", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		finished := make(chan struct{})
		go func() {
			l.Release(ctx)
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("Release on a Take-driven lease blocked")
		}
		select {
		case <-l.Done():
		default:
			t.Fatal("Done still open after Release")
		}
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("released lease still valid: %v", err)
		}
		if cur, _, err := Load(ctx, s, "take/lease"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("release did not hand back: %+v %v", cur, err)
		}
		l.Retire() // idempotent
	})

	runUnit(t, "AcquireTwice", func(t *testing.T) {
		s := newMemStore()
		l := New(s, "twice/lease", "owner-a", time.Minute)
		if err := l.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
		if err := l.Acquire(ctx); err == nil {
			t.Fatal("second Acquire on one handle succeeded")
		}
		l.Release(ctx)
		if err := l.Acquire(ctx); err == nil {
			t.Fatal("Acquire after Release succeeded")
		}
	})

	t.Run("ZeroTTLIsDefault", func(t *testing.T) {
		if got := New(nil, "k", "o", 0).TTL(); got != DefaultTTL {
			t.Fatalf("TTL() = %v, want DefaultTTL", got)
		}
		if got := New(nil, "k", "o", -time.Second).TTL(); got != DefaultTTL {
			t.Fatalf("TTL() = %v, want DefaultTTL", got)
		}
	})

	runUnit(t, "ReleaseHonorsContext", func(t *testing.T) {
		s := newMemStore()
		l, err := Acquire(ctx, s, "ctx/lease", "owner-a", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		l.Release(cancelled)
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) || !strings.Contains(err.Error(), "released") {
			t.Fatalf("Release with a dead ctx left the lease valid locally: %v", err)
		}
		if cur, _, err := Load(ctx, s, "ctx/lease"); err != nil || cur.Expiry.IsZero() {
			t.Fatalf("handover ran under a cancelled ctx: %+v %v", cur, err)
		}
	})

	t.Run("NotOwnerMessage", func(t *testing.T) {
		var l *Lease
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) || !strings.HasPrefix(err.Error(), "lease: not held") {
			t.Fatalf("nil lease: %v", err)
		}
		if err := New(nil, "k", "o", 0).Valid(); !errors.Is(err, ErrNotOwner) || strings.Contains(err.Error(), "0001") {
			t.Fatalf("unacquired lease: %v", err)
		}
	})
}

// TestCheckHeadOnRenewal: an ErrNotOwner from the probe fences at once;
// any other error leaves a lease whose CAS succeeded alone.
func TestCheckHeadOnRenewal(t *testing.T) {
	ctx := context.Background()
	const ttl = 200 * time.Millisecond

	runUnit(t, "NotCalledOnInitialTake", func(t *testing.T) {
		s := newMemStore()
		l := New(s, "probe/initial", "owner", time.Minute)
		calls := 0
		l.CheckHeadOnRenewal(func(context.Context) error { calls++; return nil })
		if err := l.Take(ctx); err != nil || calls != 0 {
			t.Fatalf("initial Take: err=%v calls=%d", err, calls)
		}
		if err := l.Take(ctx); err != nil || calls != 1 {
			t.Fatalf("renewal Take: err=%v calls=%d", err, calls)
		}
		probe := errors.New("probe")
		l.CheckHeadOnRenewal(func(context.Context) error { return probe })
		if err := l.Take(ctx); !errors.Is(err, probe) {
			t.Fatalf("probe error is not Take's error: %v", err)
		}
		l.Release(ctx)
	})

	runUnit(t, "NotOwnerFences", func(t *testing.T) {
		s := newMemStore()
		l, err := Acquire(ctx, s, "probe/fence", "owner", ttl)
		if err != nil {
			t.Fatal(err)
		}
		fenced := make(chan struct{})
		l.Start(func() { close(fenced) })
		l.CheckHeadOnRenewal(func(context.Context) error { return fmt.Errorf("%w: foreign head", ErrNotOwner) })
		select {
		case <-fenced:
		case <-time.After(20 * ttl):
			t.Fatal("ErrNotOwner from the probe did not fence")
		}
		l.Release(ctx)
	})

	runUnit(t, "OtherErrorDoesNotFence", func(t *testing.T) {
		s := newMemStore()
		l, err := Acquire(ctx, s, "probe/soft", "owner", ttl)
		if err != nil {
			t.Fatal(err)
		}
		fenced := make(chan struct{})
		l.Start(func() { close(fenced) })
		var mu sync.Mutex
		calls := 0
		l.CheckHeadOnRenewal(func(context.Context) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return errors.New("probe unavailable")
		})
		select {
		case <-fenced:
			t.Fatal("a non-ErrNotOwner probe error fenced the lease")
		case <-time.After(3 * ttl):
		}
		mu.Lock()
		n := calls
		mu.Unlock()
		if n == 0 {
			t.Fatal("probe never ran")
		}
		if err := l.Valid(); err != nil {
			t.Fatal(err)
		}
		l.Release(ctx)
	})
}

// TestSetLoggerRace: SetLogger while the renewer logs. -race is the
// assertion.
func TestSetLoggerRace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, f := storetest.Faulty(t, newMemStore())
		const ttl = 100 * time.Millisecond
		l, err := Acquire(ctx, s, "log/lease", "owner", ttl)
		if err != nil {
			t.Fatal(err)
		}
		// Every renewal fails, so the renewer logs on every tick until it fences.
		f.SetShape(storetest.Shape{ErrorRate: 1})
		var logs lockedBuffer
		entered, resume := make(chan struct{}), make(chan struct{})
		l.SetLogger(slog.New(slog.NewTextHandler(&pausedLog{logs: &logs, entered: entered, resume: resume}, nil)))
		await(t, entered, "renewal logging")
		// Race the renewer's lookup too: the handler's entry alone orders its first lookup.
		lookedUp := make(chan struct{})
		go func() {
			for range 50 {
				_ = l.logger()
			}
			close(lookedUp)
		}()
		// Replace the logger while the old handler is still writing, not between ticks.
		for range 50 {
			l.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		}
		await(t, lookedUp, "concurrent logger lookup")
		close(resume)
		// No Start: the fence owes Start its callback, so only the renewer's
		// exit is awaited here, and Done after the Release gives it up.
		await(t, l.done, "renewer exit after failing renewals")
		f.SetShape(storetest.Shape{})
		if !strings.Contains(logs.String(), "lease renewal failed") {
			t.Fatalf("no renewal failure logged:\n%s", logs.String())
		}
		l.Release(ctx)
		await(t, l.Done(), "Done after Release")
	})
}

type pausedLog struct {
	logs    *lockedBuffer
	entered chan struct{}
	resume  chan struct{}
}

func (w *pausedLog) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.resume
	return w.logs.Write(p)
}

// plant CASes body in at key over whatever is there.
func plant(t *testing.T, s Store, key string, body Body) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	_, etag, err := Load(context.Background(), s, key)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.PutIfMatch(context.Background(), key, data, etag); err != nil || !ok {
		t.Fatalf("planting %s: ok=%v err=%v", key, ok, err)
	}
}

// lossy is a Store whose next PutIfAbsent lands but loses its answer, and
// whose following GetWithETag fails too, so the write stays unresolved.
type lossy struct {
	Store
	mu       sync.Mutex
	losePut  bool
	loseRead bool
}

var errLost = errors.New("connection reset")

func (s *lossy) loseNextPut() {
	s.mu.Lock()
	s.losePut = true
	s.mu.Unlock()
}

func (s *lossy) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := s.Store.PutIfAbsent(ctx, key, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.losePut && err == nil {
		s.losePut, s.loseRead = false, true
		return false, errLost
	}
	return ok, err
}

func (s *lossy) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	lose := s.loseRead
	s.loseRead = false
	s.mu.Unlock()
	if lose {
		return nil, "", errLost
	}
	return s.Store.GetWithETag(ctx, key)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestReleaseDuringRenewal: A Release while a renewal PUT is in flight
// is a deliberate stop: no fence callback, no "lease fenced" log, and the
// handover still lands on the renewal's nonce, so the next holder takes
// over at once.
func TestReleaseDuringRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const ttl = 2 * time.Second // renew every 500ms, 1s per attempt
		base := newMemStore()
		entered, resume := make(chan struct{}), make(chan struct{})
		s := &stubStore{Store: base, match: func(ctx context.Context, key string, data []byte, etag string) (bool, error) {
			// A renewal already dispatched can land even after cancellation.
			close(entered)
			<-resume
			return base.PutIfMatch(context.WithoutCancel(ctx), key, data, etag)
		}}
		l, err := Acquire(ctx, s, "rel/lease", "owner-a", ttl)
		if err != nil {
			t.Fatal(err)
		}
		var logs lockedBuffer
		l.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		var fenced atomic.Bool
		l.Start(func() { fenced.Store(true) })
		await(t, entered, "renewal PUT")
		released := make(chan struct{})
		go func() { l.Release(ctx); close(released) }()
		await(t, l.stop, "Release retirement")
		synctest.Wait()
		// The renewer is paused; the handover must use the normal store.
		s.match = nil
		close(resume)
		await(t, released, "Release")
		await(t, l.Done(), "Done after Release") // a callback would have returned by now
		if fenced.Load() || strings.Contains(logs.String(), "lease fenced") {
			t.Fatalf("Release fenced the lease: callback=%v logs:\n%s", fenced.Load(), logs.String())
		}
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) || !strings.Contains(err.Error(), "released") {
			t.Fatalf("released lease: %v", err)
		}
		if cur, _, err := Load(ctx, s, "rel/lease"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("handover skipped after a renewal landed: %+v %v\n%s", cur, err, logs.String())
		}
		b, err := Acquire(ctx, s, "rel/lease", "owner-b", ttl)
		if err != nil {
			t.Fatalf("next holder could not take over at once: %v", err)
		}
		b.Release(ctx)
	})
}

// TestReleaseDuringFirstAcquire: A Release racing the first
// acquisition makes Acquire fail, not succeed on a retired handle, and
// still hands the landed write back.
func TestReleaseDuringFirstAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		base := newMemStore()
		entered, resume := make(chan struct{}), make(chan struct{})
		s := &stubStore{Store: base, absent: func(ctx context.Context, key string, data []byte) (bool, error) {
			close(entered)
			<-resume
			return base.PutIfAbsent(context.WithoutCancel(ctx), key, data)
		}}
		l := New(s, "rel/first", "owner-a", time.Minute)
		acquired := make(chan error, 1)
		go func() { acquired <- l.Acquire(ctx) }()
		await(t, entered, "acquisition PUT")
		released := make(chan struct{})
		go func() { l.Release(ctx); close(released) }()
		await(t, l.stop, "Release retirement")
		close(resume)
		if err := await(t, acquired, "Acquire"); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("Acquire racing Release: %v, want ErrNotOwner", err)
		}
		await(t, released, "Release")
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("released lease still valid: %v", err)
		}
		if cur, _, err := Load(ctx, s, "rel/first"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("handover skipped: %+v %v", cur, err)
		}
	})
}

func TestTakeAfterRetire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New(newMemStore(), "retired/lease", "owner-a", time.Minute)
		l.Retire()
		if err := l.Take(context.Background()); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("Take on a retired lease: %v", err)
		}
	})
}

// await receives from ch, failing the test after 10s.
func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out", what)
		panic("unreachable")
	}
}

// TestFenceCallbackDone: Done waits for a running fence callback;
// Release does not.
func TestFenceCallbackDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const ttl = 200 * time.Millisecond
		s := newMemStore()
		l, err := Acquire(ctx, s, "fence/slow", "owner-a", ttl)
		if err != nil {
			t.Fatal(err)
		}
		entered, unblock := make(chan struct{}), make(chan struct{})
		l.Start(func() { close(entered); <-unblock })
		if err := Steal(ctx, s, "fence/slow", "owner-b", ttl); err != nil {
			t.Fatal(err)
		}
		await(t, entered, "fence callback")
		released := make(chan struct{})
		go func() { l.Release(ctx); close(released) }()
		await(t, released, "Release while the callback runs")
		synctest.Wait()
		select {
		case <-l.Done():
			t.Fatal("Done closed while the fence callback was still running")
		default:
		}
		close(unblock)
		await(t, l.Done(), "Done after the callback returned")
		l.Fence() // after Done: no callback, no panic
	})
}

// TestDoneCoversStartCallback: the renewer fences and exits before Start;
// Start runs the callback itself, and Done waits for it to return.
func TestDoneCoversStartCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const ttl = 200 * time.Millisecond
		s := newMemStore()
		l, err := Acquire(ctx, s, "fence/early", "owner-a", ttl)
		if err != nil {
			t.Fatal(err)
		}
		if err := Steal(ctx, s, "fence/early", "owner-b", ttl); err != nil {
			t.Fatal(err)
		}
		await(t, l.done, "renewer exit after the steal")
		synctest.Wait()
		select {
		case <-l.Done():
			t.Fatal("Done closed with the fence callback still owed to Start")
		default:
		}
		entered, unblock, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			l.Start(func() { close(entered); <-unblock })
			close(returned)
		}()
		await(t, entered, "Start's own callback")
		synctest.Wait()
		select {
		case <-l.Done():
			t.Fatal("Done closed while Start's callback was still running")
		default:
		}
		close(unblock)
		await(t, returned, "Start")
		await(t, l.Done(), "Done after Start's callback returned")
	})
}

// TestReleaseGivesUpOwedCallback: fenced before Start, then released:
// Done closes, and a late Start runs nothing.
func TestReleaseGivesUpOwedCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		l := New(newMemStore(), "fence/owed", "owner-a", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		l.Fence()
		select {
		case <-l.Done():
			t.Fatal("Done closed with the fence callback still owed to Start")
		default:
		}
		l.Release(ctx)
		await(t, l.Done(), "Done after Release")
		l.Start(func() { t.Error("Start ran a callback Release gave up") })
	})
}

// TestFenceAfterRelease: a Fence on a released lease runs no callback.
func TestFenceAfterRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		l := New(newMemStore(), "fence/released", "owner-a", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		l.Start(func() { t.Error("a Fence after Release ran the callback") })
		if !l.join() { // stands in for a running callback: keeps Done open
			t.Fatal("join on a live lease")
		}
		l.Release(ctx)
		l.Fence()
		l.leave()
		await(t, l.Done(), "Done after Release") // a callback would have returned by now
	})
}

// TestReleaseAfterUnresolvedAcquire: The first write landed but its
// answer and read-back were lost; Release still hands it back.
func TestReleaseAfterUnresolvedAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		ls := &lossy{Store: newMemStore()}
		ls.loseNextPut()
		l := New(ls, "unres/lease", "owner-a", time.Minute)
		if err := l.Take(ctx); err == nil || errors.Is(err, ErrNotOwner) {
			t.Fatalf("first Take: %v, want an unresolved outcome", err)
		}
		l.Release(ctx)
		if cur, _, err := Load(ctx, ls, "unres/lease"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("release did not hand back the unresolved write: %+v %v", cur, err)
		}
	})
}

// TestReleasePrefersDone: With the renewer already gone, a dead ctx
// is a failed handover, not "renewal still finishing".
func TestReleasePrefersDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		l := New(newMemStore(), "done/lease", "owner-a", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		var logs lockedBuffer
		l.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		dead, cancel := context.WithCancel(ctx)
		cancel()
		for range 20 {
			l.Release(dead)
		}
		if strings.Contains(logs.String(), "still finishing") {
			t.Fatalf("Release picked ctx over a closed done:\n%s", logs.String())
		}
	})
}

func TestStealAfterRetire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New(newMemStore(), "steal/retired", "owner-a", time.Minute)
		l.Retire()
		if err := l.Steal(context.Background()); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("Steal on a retired lease: %v", err)
		}
	})
}

func runUnit(t *testing.T, name string, f func(*testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) { synctest.Test(t, f) })
}

// memStore implements only the lease operations; no flock or external I/O in a bubble.
type memStore struct {
	objstore.Backend
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *objstore.Store {
	return objstore.Open(&memStore{objects: make(map[string][]byte)}, objstore.Config{})
}

func memETag(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func (s *memStore) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	data, ok := s.objects[key]
	if !ok {
		return nil, "", objstore.ErrNotFound
	}
	return slices.Clone(data), memETag(data), nil
}

func (s *memStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok := s.objects[key]; ok {
		return false, nil
	}
	s.objects[key] = slices.Clone(data)
	return true, nil
}

func (s *memStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	cur, ok := s.objects[key]
	if !ok || memETag(cur) != etag {
		return false, nil
	}
	s.objects[key] = slices.Clone(data)
	return true, nil
}

// expireStored zeroes l's local deadline and the stored expiry, keeping the
// owner and nonce: the acquisition has lapsed and is takeable at once.
func expireStored(t *testing.T, l *Lease) {
	t.Helper()
	l.Expire()
	body, _, err := Load(context.Background(), l.Store(), l.Key())
	if err != nil {
		t.Fatal(err)
	}
	body.Expiry = time.Time{}
	plant(t, l.Store(), l.Key(), body)
}

func TestBeforeRequiresBothClocks(t *testing.T) {
	start := time.Now()
	deadline := start.Add(time.Second)
	for _, tc := range []struct {
		name          string
		elapsed, wall time.Duration
		want          bool
	}{
		{"live", 0, 0, true},
		{"suspend", 0, 2 * time.Second, false},
		{"wall boundary", 0, time.Second, false},
		{"backward wall step", 2 * time.Second, -time.Second, false},
		{"elapsed boundary", time.Second, 0, false},
		{"both expired", 2 * time.Second, 2 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Before(start.Add(tc.elapsed), start.Round(0).Add(tc.wall), deadline); got != tc.want {
				t.Fatalf("lease live = %v, want %v", got, tc.want)
			}
		})
	}
}

// Real backend coverage: wait for completed renewals, not a guessed sleep.
func TestBackendRenewalAndFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	s := bucket.New(t)
	const ttl = 4 * time.Second // Leave room for S3 and fsync tail latency.
	l, err := Acquire(ctx, s, "backend/lease", "owner-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release(ctx)
	initial, _, err := Load(ctx, s, l.Key())
	if err != nil {
		t.Fatal(err)
	}
	renewed, fenced := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l.CheckHeadOnRenewal(func(context.Context) error { once.Do(func() { close(renewed) }); return nil })
	l.Start(func() { close(fenced) })
	receiveWithin(t, ctx, renewed, "backend renewal")
	cur, _, err := Load(ctx, s, l.Key())
	if err != nil || cur.Nonce == initial.Nonce || !cur.Expiry.After(initial.Expiry) {
		t.Fatalf("renewal did not advance the stored lease: initial=%+v current=%+v err=%v", initial, cur, err)
	}
	if err := l.Valid(); err != nil {
		t.Fatalf("renewed backend lease invalid: %v", err)
	}
	if other, err := Acquire(ctx, s, l.Key(), "owner-b", ttl); !errors.Is(err, ErrNotOwner) {
		if other != nil {
			other.Release(ctx)
		}
		t.Fatalf("takeover of renewed backend lease: %v, want ErrNotOwner", err)
	}
	if err := Steal(ctx, s, l.Key(), "owner-b", ttl); err != nil {
		t.Fatal(err)
	}
	receiveWithin(t, ctx, fenced, "backend fence")
	receiveWithin(t, ctx, l.Done(), "backend Done")
	if err := l.Valid(); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("stolen backend lease still valid: %v", err)
	}
	l.Release(ctx)
	if cur, _, err := Load(ctx, s, l.Key()); err != nil || cur.Owner != "owner-b" {
		t.Fatalf("fenced release clobbered backend owner: %+v %v", cur, err)
	}
}

// The following backend tests exercise real conditional writes with roomy TTLs.
func TestReleaseIsImmediateHandover(t *testing.T) {
	ctx := context.Background()
	s := bucket.New(t)
	const ttl = time.Minute // long enough that only the release can explain it
	a, err := Acquire(ctx, s, "n/lease", "owner-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	a.Start(func() {})
	a.Release(context.Background())
	b, err := Acquire(ctx, s, "n/lease", "owner-b", ttl)
	if err != nil {
		t.Fatalf("acquire after release: %v, want the lease handed straight over", err)
	}
	b.Start(func() {})
	b.Release(context.Background())
}

func TestAcquireCannotReplaceLiveSameOwner(t *testing.T) {
	ctx := context.Background()
	s := bucket.New(t)
	a, err := Acquire(ctx, s, "same/lease", "process", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release(context.Background())
	b, err := Acquire(ctx, s, "same/lease", "process", time.Minute)
	if b != nil {
		defer b.Release(context.Background())
	}
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf("fresh acquisition replaced a live acquisition with the same owner ID: %v", err)
	}
	if err := a.Valid(); err != nil {
		t.Fatalf("first acquisition unexpectedly lost validity: %v", err)
	}
}

func TestRenewalCannotReacquireAfterPausedRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, fault := storetest.Faulty(t, newMemStore())
		// A handle with no renewer, so the test drives Take by hand.
		a := New(s, "gap/lease", "first", time.Minute)
		if err := a.Take(ctx); err != nil {
			t.Fatal(err)
		}
		defer a.Release(context.Background())
		fault.Set(storetest.Plan{Op: storetest.OpGet, Key: a.Key(), N: 1, Mode: storetest.Pause})
		defer fault.Resume()
		done := make(chan error, 1)
		go func() { done <- a.Take(ctx) }()
		synctest.Wait()
		if fault.Fired() != 1 {
			t.Fatal("renewal did not reach the paused read")
		}
		expireStored(t, a)
		b, err := Acquire(ctx, s, "gap/lease", "second", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		b.Release(context.Background())
		fault.Resume()
		if err := <-done; !errors.Is(err, ErrNotOwner) {
			t.Fatalf("paused renewal reacquired an abandoned handle: %v", err)
		}
		if err := a.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("abandoned handle may serve after another owner released: %v", err)
		}
	})
}

func TestLateRenewalDoesNotExtendAnElapsedInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, fault := storetest.Faulty(t, newMemStore())
		a := New(s, "late/lease", "first", time.Minute)
		if err := a.Take(ctx); err != nil {
			t.Fatal(err)
		}
		defer a.Release(context.Background())
		fault.Set(storetest.Plan{Op: storetest.OpPutIfMatch, Key: a.Key(), N: 1, Mode: storetest.Pause})
		defer fault.Resume()
		done := make(chan error, 1)
		go func() { done <- a.Take(ctx) }()
		synctest.Wait()
		if fault.Fired() != 1 {
			t.Fatal("renewal did not reach the paused CAS")
		}
		// The CAS can land successfully, but cannot retroactively cover the gap
		// between the old local deadline and the delayed response.
		a.Expire()
		fault.Resume()
		if err := <-done; !errors.Is(err, ErrNotOwner) {
			t.Fatalf("late CAS revived an expired acquisition: %v", err)
		}
		if err := a.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("late renewal made stale state serviceable: %v", err)
		}
	})
}

func TestReleaseCannotExpireNewAcquisitionWithSameOwner(t *testing.T) {
	ctx := context.Background()
	s := bucket.New(t)
	a, err := Acquire(ctx, s, "same/lease", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	a.Retire()
	expireStored(t, a)
	b, err := Acquire(ctx, s, "same/lease", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release(context.Background())
	a.Release(context.Background())
	if other, err := Acquire(ctx, s, "same/lease", "third-owner", time.Minute); !errors.Is(err, ErrNotOwner) {
		if other != nil {
			other.Release(context.Background())
		}
		t.Fatalf("old release handed away a newer acquisition: %v", err)
	}
}

// FuzzLoad: any bytes at the lease key load as a free lease with an ETag,
// never an error, so a fresh owner can always CAS over garbage.
func FuzzLoad(f *testing.F) {
	add := func(v any) {
		raw, err := json.Marshal(v)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	add(Body{Owner: "host/1/abcd", Nonce: "n", Expiry: time.Unix(1788000000, 0)})
	add(Body{})
	for _, s := range [][]byte{
		{}, []byte("{"), []byte("null"), []byte("[]"), []byte("0"),
		[]byte(`{"owner":"host/1/ab","nonce":"n","expiry":"not-a-time"}`),
		[]byte(`{"owner":"host/1/ab","nonce":"n","expiry":"9999-12-31T23:59:59Z"}`), // the far-future body
		[]byte(`{"owner":"host/1/ab","nonce":"n","expiry":"0001-01-01T00:00:00Z"}`), // the released body
		[]byte(`{"owner":null,"nonce":null,"expiry":null}`),
		[]byte(`{"owner":"` + strings.Repeat("x", 4096) + `"}`),
	} {
		f.Add(s)
	}
	ctx := context.Background()
	s := bucket.New(f)
	const key = "x/lease"
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := s.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		body, etag, err := Load(ctx, s, key)
		if err != nil {
			t.Fatalf("Load reported failure for bytes that merely are not a lease: %v", err)
		}
		if etag == "" {
			t.Fatal("Load dropped the ETag; garbage at the key could then only be PutIfAbsent'd, never CASed over")
		}
		// Take's whole decision, and the one thing that must hold: an
		// unparseable body is a FREE lease, so a fresh owner can write over
		// it. json leaves partial state on a decode error, so "zero" is the
		// property being asserted, not "unchanged".
		var probe Body
		if json.Unmarshal(data, &probe) != nil && body != (Body{}) {
			t.Fatalf("a body that does not decode came back as %+v; Take would compare a real owner against garbage", body)
		}
	})
}

// stubStore overrides only the calls a test needs to control.
type stubStore struct {
	Store
	get    func(context.Context, string) ([]byte, string, error)
	absent func(context.Context, string, []byte) (bool, error)
	match  func(context.Context, string, []byte, string) (bool, error)
}

func (s *stubStore) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if s.get != nil {
		return s.get(ctx, key)
	}
	return s.Store.GetWithETag(ctx, key)
}

func (s *stubStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if s.absent != nil {
		return s.absent(ctx, key, data)
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func (s *stubStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	if s.match != nil {
		return s.match(ctx, key, data, etag)
	}
	return s.Store.PutIfMatch(ctx, key, data, etag)
}

func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func receiveWithin[T any](t *testing.T, ctx context.Context, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("%s: %v", what, ctx.Err())
		panic("unreachable")
	}
}

func TestWritePreservesPutAndReadErrors(t *testing.T) {
	putErr, readErr := errors.New("PUT failed"), errors.New("read-back failed")
	for _, conditional := range []bool{false, true} {
		runUnit(t, fmt.Sprint(conditional), func(t *testing.T) {
			ctx := boundedContext(t)
			s := &stubStore{
				get:    func(context.Context, string) ([]byte, string, error) { return nil, "", readErr },
				absent: func(context.Context, string, []byte) (bool, error) { return false, putErr },
				match:  func(context.Context, string, []byte, string) (bool, error) { return false, putErr },
			}
			l := New(s, "errors/lease", "owner", time.Minute)
			etag := ""
			if conditional {
				etag = "etag"
			}
			err := l.write(ctx, etag)
			if !errors.Is(err, putErr) || !errors.Is(err, readErr) {
				t.Fatalf("write lost a failure: %v; PUT=%v read-back=%v", err, errors.Is(err, putErr), errors.Is(err, readErr))
			}
			if !strings.Contains(err.Error(), "PUT") || !strings.Contains(err.Error(), "read-back") {
				t.Fatalf("write failures lack operation labels: %v", err)
			}
		})
	}
}

func TestRenewalPreservesAcquisitionValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		base := newMemStore()
		keys := make(chan string, 8)
		s := &stubStore{Store: base, match: func(ctx context.Context, key string, data []byte, etag string) (bool, error) {
			keys <- objstore.KMSKey(ctx)
			return base.PutIfMatch(ctx, key, data, etag)
		}}
		acquireCtx, cancel := context.WithCancel(objstore.WithKMSKey(ctx, "caller-key"))
		l, err := Acquire(acquireCtx, s, "values/lease", "owner", 2*time.Second)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(l.Retire)
		l.Start(nil)
		cancel() // The renewer keeps values, not the request's cancellation.
		if got := receiveWithin(t, ctx, keys, "renewal PUT"); got != "caller-key" {
			t.Fatalf("renewal KMS key = %q, want caller-key", got)
		}
	})
}

func TestRetirementCancelsAcquire(t *testing.T) {
	for _, release := range []bool{false, true} {
		runUnit(t, fmt.Sprint(release), func(t *testing.T) {
			ctx := boundedContext(t)
			entered := make(chan struct{})
			s := &stubStore{
				get: func(context.Context, string) ([]byte, string, error) { return nil, "", objstore.ErrNotFound },
				absent: func(ctx context.Context, _ string, _ []byte) (bool, error) {
					close(entered)
					<-ctx.Done()
					return false, ctx.Err()
				},
			}
			l := New(s, "cancel/lease", "owner", 200*time.Millisecond)
			acquired := make(chan error, 1)
			go func() { acquired <- l.Acquire(ctx) }()
			receiveWithin(t, ctx, entered, "acquisition PUT")
			stopped := make(chan struct{})
			go func() {
				if release {
					l.Release(ctx)
				} else {
					l.Retire()
				}
				close(stopped)
			}()
			bounded, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			receiveWithin(t, bounded, stopped, "retirement during acquisition")
			if err := receiveWithin(t, ctx, acquired, "Acquire"); !errors.Is(err, ErrNotOwner) {
				t.Fatalf("retired Acquire: %v, want ErrNotOwner", err)
			}
			receiveWithin(t, ctx, l.Done(), "Done")
		})
	}
}

func TestReleaseBoundsWaitForAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		entered, unblock := make(chan struct{}), make(chan struct{})
		acquired := make(chan error, 1)
		t.Cleanup(func() {
			close(unblock)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			receiveWithin(t, cleanupCtx, acquired, "Acquire after backend unblocks")
		})
		s := &stubStore{get: func(ctx context.Context, _ string) ([]byte, string, error) {
			close(entered)
			// Model a backend slow to notice cancellation, but always unblock at cleanup.
			select {
			case <-unblock:
			case <-ctx.Done():
				<-unblock
			}
			return nil, "", context.Canceled
		}}
		l := New(s, "bound/lease", "owner", 100*time.Millisecond)
		go func() { acquired <- l.Acquire(ctx) }()
		receiveWithin(t, ctx, entered, "acquisition read")
		released := make(chan struct{})
		go func() { l.Release(ctx); close(released) }()
		bounded, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		receiveWithin(t, bounded, released, "TTL-bounded Release")
	})
}

func TestRetirementCancelsRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		base := newMemStore()
		entered := make(chan struct{})
		s := &stubStore{Store: base, match: func(ctx context.Context, _ string, _ []byte, _ string) (bool, error) {
			close(entered)
			<-ctx.Done()
			return false, ctx.Err()
		}}
		l, err := Acquire(ctx, s, "renew-cancel/lease", "owner", 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Retire)
		l.Start(nil)
		receiveWithin(t, ctx, entered, "renewal PUT")
		retired := make(chan struct{})
		go func() { l.Retire(); close(retired) }()
		bounded, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		receiveWithin(t, bounded, retired, "cancelled renewal")
	})
}

func TestPendingPrunesExpiredAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New(nil, "pending/lease", "owner", time.Minute)
		now := time.Now()
		for i := range 10000 {
			l.notePending(fmt.Sprint(i), now.Add(-2*l.ttl))
		}
		l.notePending("grace", now.Add(-l.ttl-l.ttl/4)) // Still within the takeover grace, but cannot be adopted.
		l.notePending("live", now)
		l.notePending("new", now)
		if len(l.pending) != 2 {
			t.Fatalf("retained %d pending nonces, want only 2 live attempts", len(l.pending))
		}
		if _, ok := l.pendingStart("live"); !ok {
			t.Fatal("forgot an attempt still eligible for adoption")
		}
	})
}

func TestValidRejectsRetiredLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		l := New(newMemStore(), "retired-valid/lease", "owner", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		l.Retire()
		receiveWithin(t, ctx, l.Done(), "Done after Retire")
		if err := l.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("retired lease valid after Done: %v", err)
		}
	})
}

func TestSharedRejectsRetiredLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		l := New(newMemStore(), "shared-retired/lease", "owner", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		var sh Shared
		r, err := sh.Join(func() (*Lease, error) { return l, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Release(ctx) })
		l.Retire()
		if _, err := sh.Join(func() (*Lease, error) { t.Fatal("mint called for retired lease"); return nil, nil }); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("joined retired lease: %v", err)
		}
	})
}

func TestReleasedRefIsNotValid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := boundedContext(t)
		l := New(newMemStore(), "ref-valid/lease", "owner", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		var sh Shared
		mint := func() (*Lease, error) { return l, nil }
		r1, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r2.Release(ctx) })
		r1.Release(ctx)
		if err := r1.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("released ref valid while another holds the lease: %v", err)
		}
		if err := r2.Valid(); err != nil {
			t.Fatalf("remaining ref lost its lease: %v", err)
		}
	})
}

func TestZeroRefIsNotValid(t *testing.T) {
	var r Ref
	if err := r.Valid(); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("zero Ref is valid: %v", err)
	}
}

// TestContinuousNeedsFloorProof: a handle that regained a broken lease is
// valid again but not continuous, until the holder proves its floor.
func TestContinuousNeedsFloorProof(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		l := New(newMemStore(), "floor/lease", "owner", time.Minute)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		if !l.Continuous() {
			t.Fatal("fresh acquisition is not continuous")
		}
		l.Fence()
		if l.Continuous() {
			t.Fatal("fenced lease is continuous")
		}
		if err := l.Steal(ctx); err != nil {
			t.Fatal(err)
		}
		if err := l.Valid(); err != nil {
			t.Fatal(err)
		}
		if l.Continuous() {
			t.Fatal("re-taken lease is continuous before FloorProven")
		}
		l.FloorProven()
		if !l.Continuous() {
			t.Fatal("FloorProven did not restore continuity")
		}
	})
}
