package lease

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/axiomhq/objstore/storetest"
)

func TestLeaseAcquireRenewExpireTakeover(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	// Every wait below is a multiple of ttl, and a renewal gets ttl/2 per
	// attempt: on a real S3 endpoint that must clear the store's tail
	// latency (a dev MinIO's p99 small PUT was 144ms, its max 1.5s).
	ttl := 300 * time.Millisecond
	if os.Getenv("OBJSTORE_TEST_S3") != "" {
		ttl = 2 * time.Second
	}

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
	defer b.Release()
	// a stopped renewing at retire, so its own local deadline is long gone:
	// it would refuse to serve even before it learned about b.
	if err := a.Valid(); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("a still considers itself the owner: %v", err)
	}

	// Ambiguous CAS on an acquire: the PUT lands and the answer is lost. The
	// nonce read-back is what turns that into a held lease instead of a
	// spurious ErrNotOwner. c takes over from b through it.
	b.Retire()
	time.Sleep(2 * ttl)
	f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Mode: storetest.Ambiguous, Key: "a/lease"})
	c, err := Acquire(ctx, s, "a/lease", "owner-c", ttl)
	if f.Fired() != 1 {
		t.Fatalf("the ambiguous CAS never fired (fired=%d)", f.Fired())
	}
	f.Clear()
	if err != nil {
		t.Fatalf("acquire through an ambiguous CAS: %v — its own bytes at the key are a HELD lease", err)
	}
	c.Start(func() {})
	defer c.Release()

	// Ambiguous on the FIRST acquire too: PutIfAbsent lands, the answer is
	// lost. Same read-back, same verdict.
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Ambiguous, Key: "fresh/lease"})
	fr, err := Acquire(ctx, s, "fresh/lease", "owner-a", ttl)
	if f.Fired() != 1 {
		t.Fatalf("the ambiguous first acquire never fired (fired=%d)", f.Fired())
	}
	f.Clear()
	if err != nil {
		t.Fatalf("first acquire through an ambiguous PutIfAbsent: %v", err)
	}
	fr.Start(func() {})
	fr.Release()

	// The fence, from the renewal side: a lease stolen while its owner is
	// renewing must fence that owner on its next renewal. Stealing it
	// outright is the same end state a post-expiry takeover reaches, without
	// the wait.
	fenced := make(chan struct{})
	d, err := Acquire(ctx, s, "b/lease", "owner-d", ttl)
	if err != nil {
		t.Fatal(err)
	}
	d.Start(func() { close(fenced) })
	if err := Steal(context.Background(), s, "b/lease", "owner-e", ttl); err != nil {
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
	d.Release() // a no-op release: the object is owner-e's now
	cur, _, err := Load(ctx, s, "b/lease")
	if err != nil || cur.Owner != "owner-e" {
		t.Fatalf("release from a fenced lease clobbered the new owner: %+v (%v)", cur, err)
	}
}

func TestLeaseRenewalAttributesALateLandingWrite(t *testing.T) {
	s, _ := storetest.NewFaulty(t)
	ctx := context.Background()
	ttl := 2 * time.Second
	l := New(s, "a/lease", "owner-a", ttl)
	if err := l.Take(ctx); err != nil {
		t.Fatal(err)
	}
	// The lost attempt: its bytes reach the store, its start is noted as
	// pending, and nothing else about it is known to the process.
	start := time.Now()
	late := Body{Owner: "owner-a", Nonce: mint(), Expiry: start.Add(ttl)}
	data, _ := json.Marshal(late)
	_, etag, err := Load(ctx, s, "a/lease")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.PutIfMatch(ctx, "a/lease", data, etag); err != nil || !ok {
		t.Fatalf("planting the late write: ok=%v err=%v", ok, err)
	}
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
	foreign := Body{Owner: "owner-a", Nonce: mint(), Expiry: time.Now().Add(ttl)}
	data, _ = json.Marshal(foreign)
	_, etag, _ = Load(ctx, s, "a/lease")
	if ok, err := s.PutIfMatch(ctx, "a/lease", data, etag); err != nil || !ok {
		t.Fatalf("planting the unknown write: ok=%v err=%v", ok, err)
	}
	if err := l.Take(ctx); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("a nonce this process never wrote must not be adopted: %v", err)
	}
}

func TestLeaseTakeoverHonorsClockSkewMargin(t *testing.T) {
	ctx := context.Background()
	s, _ := storetest.NewFaulty(t)
	const ttl = time.Minute
	a, err := Acquire(ctx, s, "skew/lease", "owner", ttl)
	if err != nil {
		t.Fatal(err)
	}
	a.Retire()
	defer a.Release()
	// The persisted wall-clock expiry is behind the taker's clock, while the
	// holder's monotonic validity interval has not elapsed. Skew is < TTL/2.
	a.mu.Lock()
	a.deadline = time.Now().Add(ttl / 8)
	a.mu.Unlock()
	body, tag, err := Load(ctx, s, a.key)
	if err != nil {
		t.Fatal(err)
	}
	body.Expiry = time.Now().Add(-ttl / 8)
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.PutIfMatch(ctx, a.key, data, tag); err != nil || !ok {
		t.Fatalf("skew fixture: %v %v", ok, err)
	}
	b, err := Acquire(ctx, s, "skew/lease", "taker", ttl)
	if b != nil {
		defer b.Release()
	}
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf("takeover overlapped a still-valid holder: %v", err)
	}
	if err := a.Valid(); err != nil {
		t.Fatalf("holder fixture expired: %v", err)
	}
}
