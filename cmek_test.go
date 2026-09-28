package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore/kms"
)

type testKeyProvider struct{ revoked bool }

type meteredAWSProvider struct {
	wraps, unwraps int
}

func (p *meteredAWSProvider) LeaseCadenced(string) bool { return true }
func (p *meteredAWSProvider) Wrap(_ context.Context, _ string, dek []byte) ([]byte, string, error) {
	p.wraps++
	return append([]byte("wrapped"), dek...), "arn:aws:kms:test", nil
}
func (p *meteredAWSProvider) Unwrap(_ context.Context, _ string, wrapped []byte) ([]byte, error) {
	p.unwraps++
	if !bytes.HasPrefix(wrapped, []byte("wrapped")) {
		return nil, kms.ErrKeyUnavailable
	}
	return wrapped[7:], nil
}

func TestAWSKeyChecksFollowLeaseCadence(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	p := &meteredAWSProvider{}
	s.ConfigureCMEK(p)
	clock := fakeClock(s)
	s.SetCMEKRefreshInterval(150 * time.Millisecond)
	keyName := "aws:arn:aws:kms:test"
	wrapped, version, err := p.Wrap(ctx, keyName, bytes.Repeat([]byte{42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: keyName, KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := s.CheckNamespaceKey(ctx, "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RotateNamespaceKey(ctx, "test"); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "ns/test/page", []byte("encrypted")); err != nil {
			t.Fatal(err)
		}
	}
	if p.wraps != 2 || p.unwraps != 1 {
		t.Fatalf("within one renewal: wraps=%d unwraps=%d", p.wraps, p.unwraps)
	}
	clock.advance(170 * time.Millisecond)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.CheckNamespaceKey(ctx, "test"); err != nil {
				errs <- err
				return
			}
			if _, err := s.RotateNamespaceKey(ctx, "test"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if p.wraps != 3 || p.unwraps != 2 {
		t.Fatalf("after renewal: wraps=%d unwraps=%d", p.wraps, p.unwraps)
	}
}

func TestRetireNamespaceKeyClearsOtherProcessCache(t *testing.T) {
	ctx := context.Background()
	bucket := newMemBackend()
	p := &testKeyProvider{}
	open := func() *Store {
		s := Open(bucket, Config{})
		s.ConfigureCMEK(p)
		return s
	}
	first, second := open(), open()
	fail := &failDelete{Backend: first.b, key: "cmek/test"}
	first.b = fail
	wrapped, version, err := p.Wrap(ctx, "local:test", bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: "local:test", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	manifest := "ns/test/manifest"
	if err := first.Put(ctx, manifest, []byte(`{"state":"deleted","incarnation":"old"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Get(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	fail.armed = true
	if err := first.RetireNamespaceKey(ctx, "test", "old"); !errors.Is(err, errInjected) {
		t.Fatalf("expected interrupted key deletion: %v", err)
	}
	if got, err := second.Get(ctx, manifest); err != nil || !deletedHead(got, "old") {
		t.Fatalf("plaintext tombstone during interrupted deletion: %s %v", got, err)
	}
	fail.armed = false
	if err := first.RetireNamespaceKey(ctx, "test", "old"); err != nil {
		t.Fatal(err)
	}
	if _, err := fail.Backend.Get(ctx, "cmek/test"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("record still exists: %v", err)
	}
	if err := second.CheckNamespaceKey(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, manifest, []byte(`{"state":"live","incarnation":"new"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := second.Get(ctx, manifest)
	if err != nil || !bytes.Contains(got, []byte(`"incarnation":"new"`)) {
		t.Fatalf("new default head: %s %v", got, err)
	}
}

func (p *testKeyProvider) Wrap(_ context.Context, _ string, dek []byte) ([]byte, string, error) {
	if p.revoked {
		return nil, "", kms.ErrKeyUnavailable
	}
	return append([]byte("wrapped"), dek...), "1", nil
}
func (p *testKeyProvider) Unwrap(_ context.Context, _ string, wrapped []byte) ([]byte, error) {
	if p.revoked {
		return nil, kms.ErrKeyUnavailable
	}
	if !bytes.HasPrefix(wrapped, []byte("wrapped")) {
		return nil, kms.ErrKeyUnavailable
	}
	return wrapped[7:], nil
}

func TestEncryptedObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := newMemBackend()
	s := Open(bucket, Config{})
	provider := &testKeyProvider{}
	s.ConfigureCMEK(provider)
	clock := fakeClock(s)
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	wrapped, version, err := provider.Wrap(ctx, "test", dek)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: "test", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte("plaintext-data!"), 10_000)
	key := "ns/test/seg/pack.bin"
	if err := s.Put(ctx, key, plain); err != nil {
		t.Fatal(err)
	}
	raw, err := s.b.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("plaintext-data!")) {
		t.Fatal("raw object contains plaintext")
	}
	if _, err := decryptObject("ns/other/seg/pack.bin", raw, dek); err == nil {
		t.Fatal("wrong object key accepted")
	}
	got, err := s.Get(ctx, key)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("Get: %v", err)
	}
	reopened := Open(bucket, Config{})
	reopened.ConfigureCMEK(provider)
	if got, err := reopened.Get(ctx, key); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("reopen: %v", err)
	}
	for _, pair := range [][2]int64{{0, 1}, {1, 100}, {65535, 4}, {65536, 65536}, {int64(len(plain)) - 1, 1}} {
		got, err := s.GetRange(ctx, key, pair[0], pair[1])
		if err != nil || !bytes.Equal(got, plain[pair[0]:pair[0]+pair[1]]) {
			t.Fatalf("range %v: %v", pair, err)
		}
	}
	if _, err := s.GetRange(ctx, key, int64(len(plain)), 1); !errors.Is(err, ErrRange) {
		t.Fatalf("out of bounds: %v", err)
	}
	s.crypt().forget("test")
	provider.revoked = true
	if _, err := s.Get(ctx, key); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("revoked: %v", err)
	}
	provider.revoked = false
	if _, err := s.Get(ctx, key); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("inside the backoff: %v", err)
	}
	clock.advance(minKeyBackoff + time.Millisecond)
	if _, err := s.Get(ctx, key); err != nil {
		t.Fatalf("recovery: %v", err)
	}
}

func BenchmarkEncryptedObjectRoundTrip(b *testing.B) {
	key := bytes.Repeat([]byte{0x42}, 32)
	plain := bytes.Repeat([]byte{0x55}, 1<<20)
	const name = "ns/bench/seg/pack.bin"
	sealed, err := encryptObject(name, plain, key)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encrypt", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		for b.Loop() {
			if _, err := encryptObject(name, plain, key); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decrypt", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		for b.Loop() {
			if _, err := decryptObject(name, sealed, key); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("roundtrip", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		for b.Loop() {
			sealed, err := encryptObject(name, plain, key)
			if err != nil {
				b.Fatal(err)
			}
			opened, err := decryptObject(name, sealed, key)
			if err != nil || !bytes.Equal(opened, plain) {
				b.Fatalf("round trip: %v", err)
			}
		}
	})
}

var errInjected = errors.New("injected delete failure")

// failDelete fails every Delete or DeleteMany naming key while armed.
// storetest.Fault is the general injector; it imports this package, so an
// in-package test carries its own.
type failDelete struct {
	Backend
	key   string
	armed bool
}

func (f *failDelete) Delete(ctx context.Context, key string) error {
	if f.armed && key == f.key {
		return errInjected
	}
	return f.Backend.Delete(ctx, key)
}

func (f *failDelete) DeleteMany(ctx context.Context, keys ...string) error {
	if f.armed && slices.Contains(keys, f.key) {
		return errInjected
	}
	return f.Backend.DeleteMany(ctx, keys...)
}

// clock is a settable time source for cmekState.now.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// fakeClock makes s's key cache and backoff run on a clock the test moves.
func fakeClock(s *Store) *clock {
	c := &clock{t: time.Unix(1_000_000, 0)}
	s.crypt().now = c.now
	return c
}

// gateProvider is testKeyProvider with counted unwraps that, while gate is
// set, signal entered and wait for gate to close; err, when set, is
// returned instead.
type gateProvider struct {
	testKeyProvider
	unwraps atomic.Int32
	entered chan struct{}
	gate    chan struct{}
	err     error
}

func (p *gateProvider) Unwrap(ctx context.Context, name string, wrapped []byte) ([]byte, error) {
	p.unwraps.Add(1)
	if p.gate != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
		<-p.gate
	}
	if p.err != nil {
		return nil, p.err
	}
	return p.testKeyProvider.Unwrap(ctx, name, wrapped)
}

// countGets counts reads of key records, and fails the next one with
// failNext when set.
type countGets struct {
	Backend
	mu       sync.Mutex
	records  int
	failNext error
}

func (c *countGets) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, keyRecordPrefix) {
		c.mu.Lock()
		c.records++
		fail := c.failNext
		c.failNext = nil
		c.mu.Unlock()
		if fail != nil {
			return nil, OpErr("get", key, fail)
		}
	}
	return c.Backend.Get(ctx, key)
}

func (c *countGets) recordGets() int { c.mu.Lock(); defer c.mu.Unlock(); return c.records }

// encryptedStore is a Store over a counting backend with namespace "test"
// keyed through p.
func encryptedStore(t testing.TB, p kms.KeyProvider) (*Store, *countGets, *clock) {
	t.Helper()
	ctx := context.Background()
	b := &countGets{Backend: newMemBackend()}
	s := Open(b, Config{KeyProvider: p})
	clk := fakeClock(s)
	wrapped, version, err := (&testKeyProvider{}).Wrap(ctx, "test", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: "test", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	return s, b, clk
}

// TestKeyLookupSingleflight: concurrent cold callers share one record read
// and one unwrap.
func TestKeyLookupSingleflight(t *testing.T) {
	ctx := context.Background()
	p := &gateProvider{entered: make(chan struct{}, 1)}
	s, b, _ := encryptedStore(t, p)
	if err := s.Put(ctx, "ns/test/obj", []byte("v")); err != nil {
		t.Fatal(err)
	}
	s.crypt().forget("test")
	p.unwraps.Store(0)
	before := b.recordGets()
	p.gate = make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			if got, err := s.Get(ctx, "ns/test/obj"); err != nil || string(got) != "v" {
				errs <- fmt.Errorf("got %q, %v", got, err)
			}
		})
	}
	<-p.entered
	time.Sleep(20 * time.Millisecond) // let the rest join the flight
	close(p.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n, gets := p.unwraps.Load(), b.recordGets()-before; n != 1 || gets != 1 {
		t.Fatalf("16 cold callers: %d unwraps, %d record reads; want 1 and 1", n, gets)
	}
}

// TestPlaintextNamespaceNegativeCache: a namespace without a key record is
// looked up once per TTL for whole-object reads, which re-check what they
// read; writes and range reads, which cannot, re-read the record. A record
// another process installs inside the TTL never turns its ciphertext into
// "plaintext", nor lets this process write plaintext into it.
func TestPlaintextNamespaceNegativeCache(t *testing.T) {
	ctx := context.Background()
	b := &countGets{Backend: newMemBackend()}
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p})
	clk := fakeClock(s)
	for i := range 5 {
		if err := b.Put(ctx, fmt.Sprintf("ns/plain/%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		if got, err := s.Get(ctx, fmt.Sprintf("ns/plain/%d", i)); err != nil || string(got) != "v" {
			t.Fatalf("get: %q %v", got, err)
		}
	}
	if n := b.recordGets(); n != 1 {
		t.Fatalf("5 plaintext gets inside the TTL read the record %d times, want 1", n)
	}
	for i := range 2 {
		if err := s.Put(ctx, fmt.Sprintf("ns/plain/%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GetRange(ctx, "ns/plain/0", 0, 1); err != nil {
		t.Fatal(err)
	}
	if n := b.recordGets(); n != 4 {
		t.Fatalf("2 puts and a range read: %d record reads in all, want 4", n)
	}
	clk.advance(keyCacheTTL + time.Millisecond)
	if _, err := s.Get(ctx, "ns/plain/0"); err != nil {
		t.Fatal(err)
	}
	if n := b.recordGets(); n != 5 {
		t.Fatalf("after the TTL: %d record reads, want 5", n)
	}

	// Another process installs a key while this one still trusts "no
	// record" for the name (primed by a Get).
	other := Open(b.Backend, Config{KeyProvider: p})
	install := func(name string) {
		t.Helper()
		if _, err := s.Get(ctx, "ns/"+name+"/missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("priming %s: %v", name, err)
		}
		if k, ok := s.crypt().cached(name); !ok || k.key != nil {
			t.Fatalf("%s: negative cache not primed", name)
		}
		wrapped, version, _ := p.Wrap(ctx, "k", bytes.Repeat([]byte{9}, 32))
		if err := other.InstallNamespaceKey(ctx, name, Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
			t.Fatal(err)
		}
	}

	install("get")
	if err := other.Put(ctx, "ns/get/enc", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ns/get/enc"); err != nil || string(got) != "secret" {
		t.Fatalf("get under a stale negative cache: %q %v", got, err)
	}

	install("range")
	if err := other.Put(ctx, "ns/range/enc", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetRange(ctx, "ns/range/enc", 0, 6); err != nil || string(got) != "secret" {
		t.Fatalf("get-range under a stale negative cache: %q %v", got, err)
	}

	install("put")
	for i, put := range []func(key string) error{
		func(key string) error { return s.Put(ctx, key, []byte("secret")) },
		func(key string) error { _, err := s.PutIfAbsent(ctx, key, []byte("secret")); return err },
		func(key string) error {
			if err := b.Put(ctx, key, nil); err != nil {
				return err
			}
			_, tag, err := b.GetWithETag(ctx, key)
			if err != nil {
				return err
			}
			_, err = s.PutIfMatch(ctx, key, []byte("secret"), tag)
			return err
		},
	} {
		s.crypt().remember("put", nil, "", time.Hour) // a stale "no record"
		key := fmt.Sprintf("ns/put/%d", i)
		if err := put(key); err != nil {
			t.Fatal(err)
		}
		if raw, _ := b.Backend.Get(ctx, key); !looksEncrypted(raw) || bytes.Contains(raw, []byte("secret")) {
			t.Fatalf("write %d under a stale negative cache stored plaintext: %q", i, raw)
		}
		if got, err := other.Get(ctx, key); err != nil || string(got) != "secret" {
			t.Fatalf("write %d read back by the installer: %q %v", i, got, err)
		}
	}
}

// stallRecordGet reads the key record of name, then, while armed, signals
// read and holds the result until release closes: a lookup that saw the
// record's state and has not yet cached it.
type stallRecordGet struct {
	Backend
	name    string
	armed   atomic.Bool
	read    chan struct{}
	release chan struct{}
}

func (s *stallRecordGet) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.Backend.Get(ctx, key)
	if key == keyRecordPrefix+s.name && s.armed.CompareAndSwap(true, false) {
		close(s.read)
		<-s.release
	}
	return data, err
}

// TestInstallDuringLookup: a lookup that read "no record" just before this
// process installs a key neither caches that answer nor hands it to
// callers arriving after the install.
func TestInstallDuringLookup(t *testing.T) {
	ctx := context.Background()
	b := &stallRecordGet{Backend: newMemBackend(), name: "race", read: make(chan struct{}), release: make(chan struct{})}
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p})
	b.armed.Store(true)
	stale := make(chan error, 1)
	go func() { _, err := s.Get(ctx, "ns/race/obj"); stale <- err }()
	<-b.read

	wrapped, version, _ := p.Wrap(ctx, "k", bytes.Repeat([]byte{3}, 32))
	if err := s.InstallNamespaceKey(ctx, "race", Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	// A write after the install must not join the stalled lookup.
	var once sync.Once
	release := func() { once.Do(func() { close(b.release) }) }
	t.Cleanup(release)
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.Put(pctx, "ns/race/after", []byte("secret")); err != nil {
		t.Fatalf("write after install waited on the stalled lookup: %v", err)
	}
	if raw, _ := b.Backend.Get(ctx, "ns/race/after"); !looksEncrypted(raw) {
		t.Fatalf("write after install joined the stale lookup: %q", raw)
	}

	release()
	if err := <-stale; err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if k, ok := s.crypt().cached("race"); ok && k.key == nil {
		t.Fatal("stale \"no record\" cached after the install")
	}
	p.revoked = true
	s.crypt().forget("race")
	if err := s.CheckNamespaceKeyCached(ctx, "race"); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("revoked key after the install: %v", err)
	}
}

// TestKeyLookupCancelDoesNotPoisonNamespace: a caller that gives up
// mid-lookup returns its context error and leaves no backoff behind. A
// context error inside the detached lookup is the store's or the SDK's own
// timeout, and opens the backoff like any other failure.
func TestKeyLookupCancelDoesNotPoisonNamespace(t *testing.T) {
	ctx := context.Background()
	p := &gateProvider{entered: make(chan struct{}, 1)}
	s, b, clk := encryptedStore(t, p)
	if err := s.Put(ctx, "ns/test/obj", []byte("v")); err != nil {
		t.Fatal(err)
	}
	s.crypt().forget("test")

	p.gate = make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := s.Get(cctx, "ns/test/obj"); done <- err }()
	<-p.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("cancelled mid-lookup: %v", err)
	}
	close(p.gate)
	// The abandoned lookup still completes for everyone else: no backoff.
	if got, err := s.Get(ctx, "ns/test/obj"); err != nil || string(got) != "v" {
		t.Fatalf("next caller after a cancelled one: %q %v", got, err)
	}
	p.gate = nil
	s.crypt().forget("test")

	b.mu.Lock()
	b.failNext = context.DeadlineExceeded
	b.mu.Unlock()
	// The caller's own ctx is fine: the lookup's timeout must not read as
	// context.DeadlineExceeded to it.
	timedOut := func(err error) bool {
		return !errors.Is(err, context.DeadlineExceeded) && errors.Is(err, kms.ErrKeyUnavailable) &&
			strings.Contains(err.Error(), "key lookup timed out after") && strings.Contains(err.Error(), `namespace "test"`)
	}
	if _, err := s.Get(ctx, "ns/test/obj"); !timedOut(err) {
		t.Fatalf("record read timed out: %v", err)
	}
	if _, err := s.Get(ctx, "ns/test/obj"); !timedOut(err) || !strings.Contains(err.Error(), "backing off") {
		t.Fatalf("inside the backoff after a store timeout: %v", err)
	}
	clk.advance(minKeyBackoff + time.Millisecond)
	if got, err := s.Get(ctx, "ns/test/obj"); err != nil || string(got) != "v" {
		t.Fatalf("after the backoff: %q %v", got, err)
	}

	// A provider failure does open the backoff, and keeps its cause.
	cause := errors.New("throttled")
	s.crypt().forget("test")
	p.err = cause
	if _, err := s.Get(ctx, "ns/test/obj"); !errors.Is(err, kms.ErrKeyUnavailable) || !errors.Is(err, cause) || !strings.Contains(err.Error(), "get ns/test/obj") {
		t.Fatalf("provider failure: %v", err)
	}
	p.err = nil
	if _, err := s.Get(ctx, "ns/test/obj"); !errors.Is(err, kms.ErrKeyUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("inside the backoff: %v", err)
	}
}

// cadencedGate is gateProvider for a lease-cadenced (AWS) key.
type cadencedGate struct{ gateProvider }

func (*cadencedGate) LeaseCadenced(string) bool { return true }

// TestRotateHonoursContextBehindHungUnwrap: a lookup stuck in the provider
// holds the namespace's lease-cadence lock; RotateNamespaceKey waiting on
// it returns on its own context.
func TestRotateHonoursContextBehindHungUnwrap(t *testing.T) {
	ctx := context.Background()
	p := &cadencedGate{gateProvider{entered: make(chan struct{}, 1)}}
	s, _, _ := encryptedStore(t, p)
	s.SetCMEKRefreshInterval(time.Second)
	s.crypt().forget("test")
	p.gate = make(chan struct{})
	got := make(chan error, 1)
	go func() { _, err := s.Get(ctx, "ns/test/obj"); got <- err }()
	<-p.entered
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := s.RotateNamespaceKey(short, "test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rotate behind a hung unwrap: %v", err)
	}
	close(p.gate)
	if err := <-got; err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("get behind the hung unwrap: %v", err)
	}
}

// TestDeleteNeedsNoKey: deleting (crypto-shredding) an encrypted namespace
// works after the customer key is revoked.
func TestDeleteNeedsNoKey(t *testing.T) {
	ctx := context.Background()
	p := &gateProvider{}
	s, _, _ := encryptedStore(t, p)
	for _, k := range []string{"ns/test/a", "ns/test/b", "ns/test/c"} {
		if err := s.Put(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	s.crypt().forget("test")
	p.revoked = true
	if _, err := s.Get(ctx, "ns/test/a"); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("revoked get: %v", err)
	}
	if err := s.Delete(ctx, "ns/test/a"); err != nil {
		t.Fatalf("delete with a revoked key: %v", err)
	}
	if err := s.DeleteMany(ctx, "ns/test/b", "ns/test/c"); err != nil {
		t.Fatalf("delete-many with a revoked key: %v", err)
	}
	if keys, err := s.List(ctx, "ns/test/"); err != nil || len(keys) != 0 {
		t.Fatalf("left behind: %v %v", keys, err)
	}
}

// TestDecryptErrorsNameOpAndKey: a corrupt ciphertext fails with the
// operation and the key, like every backend error.
func TestDecryptErrorsNameOpAndKey(t *testing.T) {
	ctx := context.Background()
	s, b, _ := encryptedStore(t, &testKeyProvider{})
	if err := s.Put(ctx, "ns/test/obj", bytes.Repeat([]byte("x"), 100)); err != nil {
		t.Fatal(err)
	}
	raw, _ := b.Get(ctx, "ns/test/obj")
	raw[len(raw)-1] ^= 1
	if err := b.Put(ctx, "ns/test/obj", raw); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "ns/test/obj"); err == nil || !strings.Contains(err.Error(), "store: get ns/test/obj") {
		t.Fatalf("get: %v", err)
	}
	if _, err := s.GetRange(ctx, "ns/test/obj", 0, 10); err == nil || !strings.Contains(err.Error(), "store: get-range ns/test/obj") {
		t.Fatalf("get-range: %v", err)
	}
}

// TestConfigKeyProviderAndWithBackend: Config.KeyProvider encrypts from
// construction, and a WithBackend view shares the encryption.
func TestConfigKeyProviderAndWithBackend(t *testing.T) {
	ctx := context.Background()
	s, b, _ := encryptedStore(t, &testKeyProvider{})
	view := s.WithBackend(func(b Backend) Backend { return b })
	if err := view.Put(ctx, "ns/test/obj", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(ctx, "ns/test/obj"); bytes.Contains(raw, []byte("secret")) || !looksEncrypted(raw) {
		t.Fatalf("stored in the clear: %q", raw)
	}
	if got, err := s.Get(ctx, "ns/test/obj"); err != nil || string(got) != "secret" {
		t.Fatalf("get: %q %v", got, err)
	}
}

// TestAcceptPlaintextOverride: Config.AcceptPlaintext replaces the
// default exemptions, and decides only for bytes without the encrypted
// header: a key-only predicate never hands back ciphertext.
func TestAcceptPlaintextOverride(t *testing.T) {
	ctx := context.Background()
	b := newMemBackend()
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p, AcceptPlaintext: func(key string, _ []byte) bool { return strings.HasSuffix(key, "/marker") }})
	wrapped, version, _ := p.Wrap(ctx, "k", bytes.Repeat([]byte{1}, 32))
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	b.Put(ctx, "ns/test/marker", []byte("plain"))
	b.Put(ctx, "ns/test/lease", []byte(`{"holder":"x"}`))
	if got, err := s.Get(ctx, "ns/test/marker"); err != nil || string(got) != "plain" {
		t.Fatalf("exempt key: %q %v", got, err)
	}
	if _, err := s.Get(ctx, "ns/test/lease"); err == nil {
		t.Fatal("default exemption still applied under an override")
	}
	if err := s.Put(ctx, "ns/other/marker", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "ns/test/marker", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ns/test/marker"); err != nil || string(got) != "secret" {
		t.Fatalf("encrypted object under an accepting key: %q %v", got, err)
	}
}

// BenchmarkEncryptedGetRange: small ranges are copied out of the decrypted
// blocks, large ones returned in place.
func BenchmarkEncryptedGetRange(b *testing.B) {
	ctx := context.Background()
	s, _, _ := encryptedStore(b, &testKeyProvider{})
	s.crypt().ttl = time.Hour
	if err := s.Put(ctx, "ns/test/obj", bytes.Repeat([]byte{0x55}, 192<<10)); err != nil {
		b.Fatal(err)
	}
	for _, n := range []int64{1, 4 << 10, 16 << 10, 64 << 10} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(n)
			for b.Loop() {
				if _, err := s.GetRange(ctx, "ns/test/obj", 65000, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestKeyRefreshIntervalAndInstallErrors: Config.KeyRefreshInterval
// survives ConfigureCMEK, and InstallNamespaceKey reports its sentinels.
func TestKeyRefreshIntervalAndInstallErrors(t *testing.T) {
	ctx := context.Background()
	p := &testKeyProvider{}
	s := Open(newMemBackend(), Config{KeyProvider: p, KeyRefreshInterval: time.Minute})
	s.ConfigureCMEK(p)
	if got := s.crypt().refreshInterval(); got != time.Minute {
		t.Fatalf("refresh interval after ConfigureCMEK: %v", got)
	}
	// Set without a provider, then configured.
	noProvider := Open(newMemBackend(), Config{KeyRefreshInterval: time.Minute})
	noProvider.ConfigureCMEK(p)
	if got := noProvider.crypt().refreshInterval(); got != time.Minute {
		t.Fatalf("Config.KeyRefreshInterval without a provider: %v", got)
	}
	// Set before ConfigureCMEK, on an Open store and on a zero Store.
	for _, before := range []*Store{newMemStore(), {b: newMemBackend()}} {
		before.SetCMEKRefreshInterval(2 * time.Minute)
		before.ConfigureCMEK(p)
		if got := before.crypt().refreshInterval(); got != 2*time.Minute {
			t.Fatalf("SetCMEKRefreshInterval before ConfigureCMEK: %v", got)
		}
		// And after it.
		before.SetCMEKRefreshInterval(3 * time.Minute)
		if got := before.crypt().refreshInterval(); got != 3*time.Minute {
			t.Fatalf("SetCMEKRefreshInterval after ConfigureCMEK: %v", got)
		}
	}
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged}); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("invalid envelope: %v", err)
	}
	env := Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", DEKWrapped: []byte("wrapped-a")}
	if err := s.InstallNamespaceKey(ctx, "test", env); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallNamespaceKey(ctx, "test", env); err != nil {
		t.Fatalf("repeated install: %v", err)
	}
	env.DEKWrapped = []byte("wrapped-b")
	if err := s.InstallNamespaceKey(ctx, "test", env); !errors.Is(err, ErrKeyRecordExists) {
		t.Fatalf("different record: %v", err)
	}
}

// TestStaleLookupAfterRemoteInstall: open's header-triggered re-lookup
// never joins a lookup that read "no record" before another process
// installed the key, so a whole-object read returns plaintext, not
// ciphertext, and the stale answer is not cached.
func TestStaleLookupAfterRemoteInstall(t *testing.T) {
	ctx := context.Background()
	b := &stallRecordGet{Backend: newMemBackend(), name: "late", read: make(chan struct{}), release: make(chan struct{})}
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p})
	fakeClock(s)
	if _, err := s.Get(ctx, "ns/late/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("priming: %v", err)
	}
	// A range read does not trust the cached "no record": its lookup reads
	// the record (still absent) and stalls there.
	b.armed.Store(true)
	stale := make(chan error, 1)
	go func() { _, err := s.GetRange(ctx, "ns/late/other", 0, 1); stale <- err }()
	<-b.read

	other := Open(b.Backend, Config{KeyProvider: p})
	wrapped, version, _ := p.Wrap(ctx, "k", bytes.Repeat([]byte{5}, 32))
	if err := other.InstallNamespaceKey(ctx, "late", Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	if err := other.Put(ctx, "ns/late/obj", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	// Were the re-lookup to join the stalled one, it would see "no record"
	// once released.
	timer := time.AfterFunc(50*time.Millisecond, func() { close(b.release) })
	defer timer.Stop()
	if got, err := s.Get(ctx, "ns/late/obj"); err != nil || string(got) != "secret" {
		t.Fatalf("get under a stale in-flight lookup: %q %v", got, err)
	}
	if err := <-stale; err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if k, ok := s.crypt().cached("late"); !ok || k.key == nil {
		t.Fatalf("after the stale lookup finished: cached=%v key=%v", ok, k.key != nil)
	}
}

// TestEmptyObjectAuthenticated: an empty object carries a tag; a bare
// header, v1 or v2, does not read as an empty object.
func TestEmptyObjectAuthenticated(t *testing.T) {
	ctx := context.Background()
	s, b, _ := encryptedStore(t, &testKeyProvider{})
	const key = "ns/test/empty"
	if err := s.Put(ctx, key, nil); err != nil {
		t.Fatal(err)
	}
	raw, _ := b.Get(ctx, key)
	if len(raw) != encryptedHeaderSize+encryptedTagSize || raw[4] != encryptedV2 {
		t.Fatalf("empty object: %d bytes, version %d", len(raw), raw[4])
	}
	if got, err := s.Get(ctx, key); err != nil || len(got) != 0 {
		t.Fatalf("empty round trip: %q %v", got, err)
	}

	tampered := bytes.Clone(raw)
	tampered[len(tampered)-1] ^= 1
	b.Put(ctx, key, tampered)
	if _, err := s.Get(ctx, key); err == nil {
		t.Fatal("tampered empty-object tag accepted")
	}

	for _, version := range []byte{encryptedV1, encryptedV2} {
		forged := make([]byte, encryptedHeaderSize)
		copy(forged, encryptedMagic[:])
		forged[4] = version
		b.Put(ctx, key, forged)
		if got, err := s.Get(ctx, key); err == nil {
			t.Fatalf("forged v%d header read as %q", version, got)
		} else if version == encryptedV1 && !errors.Is(err, errLegacyEmpty) {
			t.Fatalf("forged v1 header: %v", err)
		}
	}
}

// TestLegacyV1ObjectDecrypts: a non-empty object written in format v1
// still reads, whole and by range.
func TestLegacyV1ObjectDecrypts(t *testing.T) {
	ctx := context.Background()
	s, b, _ := encryptedStore(t, &testKeyProvider{})
	const key = "ns/test/legacy"
	dek := bytes.Repeat([]byte{7}, 32) // encryptedStore's key
	plain := bytes.Repeat([]byte("legacy-bytes"), 10_000)
	if err := b.Put(ctx, key, encryptV1(t, key, plain, dek)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, key); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("v1 get: %v", err)
	}
	if got, err := s.GetRange(ctx, key, 65530, 20); err != nil || !bytes.Equal(got, plain[65530:65550]) {
		t.Fatalf("v1 range: %v", err)
	}
}

// encryptV1 seals plain the way format v1 did: version byte 1, and no
// block at all for an empty object.
func encryptV1(t *testing.T, object string, plain, key []byte) []byte {
	t.Helper()
	aead, err := aeadForKey(key)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, encryptedHeaderSize)
	copy(out, encryptedMagic[:])
	out[4] = encryptedV1
	binary.BigEndian.PutUint64(out[5:13], uint64(len(plain)))
	if _, err := rand.Read(out[13:25]); err != nil {
		t.Fatal(err)
	}
	for i := 0; i*encryptedBlockSize < len(plain); i++ {
		start := i * encryptedBlockSize
		end := min(start+encryptedBlockSize, len(plain))
		nonce := blockNonce(out[13:25], uint64(i))
		out = aead.Seal(out, nonce[:], plain[start:end], blockAAD(object, out[:encryptedHeaderSize], uint64(i)))
	}
	return out
}

// TestKeyBackoffs: a failed record read backs off minKeyBackoff flat; a
// failed unwrap doubles. A failure from a lookup that began before an
// Install opens no backoff, and Install closes one already open. The key
// error is wrapped once.
func TestKeyBackoffs(t *testing.T) {
	ctx := context.Background()
	p := &gateProvider{}
	s, b, clk := encryptedStore(t, p)
	c := s.crypt()
	for range 3 {
		c.forget("test")
		b.mu.Lock()
		b.failNext = errors.New("503")
		b.mu.Unlock()
		if _, err := s.Get(ctx, "ns/test/x"); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("record read failure: %v", err)
		}
		if f := c.failures["test"]; f.next.Sub(clk.now()) != minKeyBackoff {
			t.Fatalf("record read backoff: %v", f.next.Sub(clk.now()))
		}
	}

	c.forget("test")
	p.revoked = true
	for i, want := range []time.Duration{minKeyBackoff, 2 * minKeyBackoff, 4 * minKeyBackoff} {
		_, err := s.Get(ctx, "ns/test/x")
		if !errors.Is(err, kms.ErrKeyUnavailable) || strings.Count(err.Error(), "customer-managed encryption key unavailable") != 1 {
			t.Fatalf("unwrap failure %d: %v", i, err)
		}
		if got := c.failures["test"].next.Sub(clk.now()); got != want {
			t.Fatalf("unwrap backoff %d: %v, want %v", i, got, want)
		}
		clk.advance(want)
	}
	s.Get(ctx, "ns/test/x")
	if err := c.retryReady("test"); err == nil || strings.Count(err.Error(), "customer-managed encryption key unavailable") != 1 {
		t.Fatalf("backoff error: %v", err)
	}

	// Install (same record) closes the backoff.
	p.revoked = false
	wrapped, version, _ := p.Wrap(ctx, "test", bytes.Repeat([]byte{7}, 32))
	if err := s.InstallNamespaceKey(ctx, "test", Envelope{Mode: EncryptionCustomerManaged, KeyName: "test", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	if err := c.retryReady("test"); err != nil {
		t.Fatalf("backoff after install: %v", err)
	}

	// A failure from a lookup that began before an invalidation opens none.
	epoch := c.currentEpoch()
	c.forget("test")
	c.fail(epoch, "test", errors.New("stale"), true)
	if err := c.retryReady("test"); err != nil {
		t.Fatalf("stale failure opened a backoff: %v", err)
	}
}

// TestKeyCacheSweep: the key cache drops expired names once it passes
// keySweepAt, instead of growing with every name ever looked up.
func TestKeyCacheSweep(t *testing.T) {
	s := newMemStore()
	s.ConfigureCMEK(&testKeyProvider{})
	clk := fakeClock(s)
	c := s.crypt()
	for i := range keySweepAt {
		c.remember(fmt.Sprint("old", i), nil, "", time.Second)
	}
	clk.advance(2 * time.Second)
	c.remember("fresh", nil, "", time.Second)
	if n := len(c.keys); n != 1 {
		t.Fatalf("after the sweep: %d names cached, want 1", n)
	}
}
