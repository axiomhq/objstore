package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestLease(t *testing.T) {
	ctx := context.Background()
	ttl := testTTL(300 * time.Millisecond)

	t.Run("AcquireRenewExpireTakeover", func(t *testing.T) {
		s := bucket.New(t)
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
	t.Run("AmbiguousTakeover", func(t *testing.T) {
		s, f := bucket.NewFaulty(t)
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

	t.Run("AmbiguousFirstAcquire", func(t *testing.T) {
		s, f := bucket.NewFaulty(t)
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
	t.Run("StolenFences", func(t *testing.T) {
		s := bucket.New(t)
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

	t.Run("RenewalAttributesALateLandingWrite", func(t *testing.T) {
		s := bucket.New(t)
		ttl := 2 * time.Second
		l := New(s, "a/lease", "owner-a", ttl)
		if err := l.Take(ctx); err != nil {
			t.Fatal(err)
		}
		// The lost attempt: its bytes reach the store, its start is noted as
		// pending, and nothing else about it is known to the process.
		start := time.Now()
		late := Body{Owner: "owner-a", Nonce: mint(), Expiry: start.Add(ttl)}
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
		plant(t, s, "a/lease", Body{Owner: "owner-a", Nonce: mint(), Expiry: time.Now().Add(ttl)})
		if err := l.Take(ctx); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("a nonce this process never wrote must not be adopted: %v", err)
		}
		l.Release(ctx)
	})

	// L3: the first acquisition landed but both its answer and the
	// read-back were lost. A retried Take must adopt our own record, not
	// wait 1.5 TTL for it as if somebody else held it.
	t.Run("RetriedTakeAfterUnresolvedAcquire", func(t *testing.T) {
		ls := &lossy{Store: bucket.New(t)}
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

	t.Run("TakeoverHonorsClockSkewMargin", func(t *testing.T) {
		s := bucket.New(t)
		const ttl = time.Minute
		a, err := Acquire(ctx, s, "skew/lease", "owner", ttl)
		if err != nil {
			t.Fatal(err)
		}
		a.Retire()
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

	// L1: a lease driven by Take has no renewer; Retire and Release must
	// still end it instead of waiting forever on done.
	t.Run("NewTakeRelease", func(t *testing.T) {
		s := bucket.New(t)
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

	t.Run("AcquireTwice", func(t *testing.T) {
		s := bucket.New(t)
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

	t.Run("ReleaseHonorsContext", func(t *testing.T) {
		s := bucket.New(t)
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
	ttl := testTTL(200 * time.Millisecond)

	t.Run("NotCalledOnInitialTake", func(t *testing.T) {
		s := bucket.New(t)
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

	t.Run("NotOwnerFences", func(t *testing.T) {
		s := bucket.New(t)
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

	t.Run("OtherErrorDoesNotFence", func(t *testing.T) {
		s := bucket.New(t)
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
	ctx := context.Background()
	s, f := bucket.NewFaulty(t)
	const ttl = 100 * time.Millisecond
	l, err := Acquire(ctx, s, "log/lease", "owner", ttl)
	if err != nil {
		t.Fatal(err)
	}
	// Every renewal fails, so the renewer logs on every tick until it fences.
	f.SetShape(storetest.Shape{ErrorRate: 1})
	var logs lockedBuffer
	for range 50 {
		l.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		time.Sleep(ttl / 20)
	}
	await(t, l.Done(), "Done after failing renewals")
	f.SetShape(storetest.Shape{})
	if !strings.Contains(logs.String(), "lease renewal failed") {
		t.Fatalf("no renewal failure logged:\n%s", logs.String())
	}
	l.Release(ctx)
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

// TestReleaseDuringRenewal: H1. A Release while a renewal PUT is in flight
// is a deliberate stop: no fence callback, no "lease fenced" log, and the
// handover still lands on the renewal's nonce, so the next holder takes
// over at once.
func TestReleaseDuringRenewal(t *testing.T) {
	ctx := context.Background()
	const ttl = 2 * time.Second // renew every 500ms, 1s per attempt
	s, f := bucket.NewFaulty(t)
	l, err := Acquire(ctx, s, "rel/lease", "owner-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	l.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	var fenced atomic.Bool
	l.Start(func() { fenced.Store(true) })
	f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Mode: storetest.Pause, Key: "rel/lease"})
	waitFor(t, func() bool { return f.Fired() == 1 })
	released := make(chan struct{})
	go func() { l.Release(ctx); close(released) }()
	waitFor(t, l.stopped)
	f.Resume()
	await(t, released, "Release")
	time.Sleep(10 * time.Millisecond) // a callback goroutine would have run by now
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
}

// TestReleaseDuringFirstAcquire: M1. A Release racing the first
// acquisition makes Acquire fail, not succeed on a retired handle, and
// still hands the landed write back.
func TestReleaseDuringFirstAcquire(t *testing.T) {
	ctx := context.Background()
	s, f := bucket.NewFaulty(t)
	l := New(s, "rel/first", "owner-a", time.Minute)
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Pause, Key: "rel/first"})
	acquired := make(chan error, 1)
	go func() { acquired <- l.Acquire(ctx) }()
	waitFor(t, func() bool { return f.Fired() == 1 })
	released := make(chan struct{})
	go func() { l.Release(ctx); close(released) }()
	waitFor(t, l.stopped)
	f.Resume()
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
}

func TestTakeAfterRetire(t *testing.T) {
	l := New(bucket.New(t), "retired/lease", "owner-a", time.Minute)
	l.Retire()
	if err := l.Take(context.Background()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Take on a retired lease: %v", err)
	}
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

// TestFenceCallbackDone: M1. Done waits for a running fence callback;
// Release does not.
func TestFenceCallbackDone(t *testing.T) {
	ctx := context.Background()
	const ttl = 200 * time.Millisecond
	s := bucket.New(t)
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
	select {
	case <-l.Done():
		t.Fatal("Done closed while the fence callback was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(unblock)
	await(t, l.Done(), "Done after the callback returned")
	l.Fence() // after Done: no callback, no panic
}

// TestReleaseAfterUnresolvedAcquire: L4. The first write landed but its
// answer and read-back were lost; Release still hands it back.
func TestReleaseAfterUnresolvedAcquire(t *testing.T) {
	ctx := context.Background()
	ls := &lossy{Store: bucket.New(t)}
	ls.loseNextPut()
	l := New(ls, "unres/lease", "owner-a", time.Minute)
	if err := l.Take(ctx); err == nil || errors.Is(err, ErrNotOwner) {
		t.Fatalf("first Take: %v, want an unresolved outcome", err)
	}
	l.Release(ctx)
	if cur, _, err := Load(ctx, ls, "unres/lease"); err != nil || !cur.Expiry.IsZero() {
		t.Fatalf("release did not hand back the unresolved write: %+v %v", cur, err)
	}
}

// TestReleasePrefersDone: L2. With the renewer already gone, a dead ctx
// is a failed handover, not "renewal still finishing".
func TestReleasePrefersDone(t *testing.T) {
	ctx := context.Background()
	l := New(bucket.New(t), "done/lease", "owner-a", time.Minute)
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
}

func TestStealAfterRetire(t *testing.T) {
	l := New(bucket.New(t), "steal/retired", "owner-a", time.Minute)
	l.Retire()
	if err := l.Steal(context.Background()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Steal on a retired lease: %v", err)
	}
}

// waitFor polls cond for up to 10s.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
	}
}

// testTTL is base on the file backend and 2 s on a real S3 endpoint: every
// wait in these tests is a multiple of the TTL, and a renewal gets TTL/2
// per attempt, which must clear the store's tail latency (a dev MinIO's
// p99 small PUT was 144 ms, its max 1.5 s).
func testTTL(base time.Duration) time.Duration {
	if os.Getenv("OBJSTORE_TEST_S3") != "" {
		return 2 * time.Second
	}
	return base
}
