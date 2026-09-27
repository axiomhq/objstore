package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
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
func encryptedStore(t *testing.T, p kms.KeyProvider) (*Store, *countGets, *clock) {
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
// looked up once per TTL, not once per operation, and a record installed
// by another process inside that window never turns its ciphertext into
// "plaintext".
func TestPlaintextNamespaceNegativeCache(t *testing.T) {
	ctx := context.Background()
	b := &countGets{Backend: newMemBackend()}
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p})
	clk := fakeClock(s)
	for i := range 5 {
		if err := s.Put(ctx, fmt.Sprintf("ns/plain/%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Get(ctx, fmt.Sprintf("ns/plain/%d", i)); err != nil || string(got) != "v" {
			t.Fatalf("get: %q %v", got, err)
		}
	}
	if n := b.recordGets(); n != 1 {
		t.Fatalf("10 plaintext ops inside the TTL read the record %d times, want 1", n)
	}
	clk.advance(keyCacheTTL + time.Millisecond)
	if _, err := s.Get(ctx, "ns/plain/0"); err != nil {
		t.Fatal(err)
	}
	if n := b.recordGets(); n != 2 {
		t.Fatalf("after the TTL: %d record reads, want 2", n)
	}

	// Another process installs a key and writes an encrypted object while
	// this one still trusts "no record".
	other := Open(b.Backend, Config{KeyProvider: p})
	wrapped, version, _ := p.Wrap(ctx, "k", bytes.Repeat([]byte{9}, 32))
	if err := other.InstallNamespaceKey(ctx, "plain", Envelope{Mode: EncryptionCustomerManaged, KeyName: "k", KeyVersion: version, DEKWrapped: wrapped}); err != nil {
		t.Fatal(err)
	}
	if err := other.Put(ctx, "ns/plain/enc", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ns/plain/enc"); err != nil || string(got) != "secret" {
		t.Fatalf("encrypted object under a stale negative cache: %q %v", got, err)
	}
}

// TestKeyLookupCancelDoesNotPoisonNamespace: a caller that gives up
// mid-lookup, and a lookup that fails with a context error, return the
// context error and leave no backoff behind for the next caller.
func TestKeyLookupCancelDoesNotPoisonNamespace(t *testing.T) {
	ctx := context.Background()
	p := &gateProvider{entered: make(chan struct{}, 1)}
	s, b, _ := encryptedStore(t, p)
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
	if _, err := s.Get(ctx, "ns/test/obj"); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("record read timed out: %v", err)
	}
	if got, err := s.Get(ctx, "ns/test/obj"); err != nil || string(got) != "v" {
		t.Fatalf("next caller after a context error: %q %v", got, err)
	}

	// A provider failure does open the backoff, and keeps its cause.
	cause := errors.New("throttled")
	s.crypt().forget("test")
	p.err = cause
	if _, err := s.Get(ctx, "ns/test/obj"); !errors.Is(err, kms.ErrKeyUnavailable) || !errors.Is(err, cause) || !strings.Contains(err.Error(), "get ns/test/obj") {
		t.Fatalf("provider failure: %v", err)
	}
	p.err = nil
	if _, err := s.Get(ctx, "ns/test/obj"); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("inside the backoff: %v", err)
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

// TestPlaintextKeysOverride: Config.PlaintextKeys replaces the default
// exemptions.
func TestPlaintextKeysOverride(t *testing.T) {
	ctx := context.Background()
	b := newMemBackend()
	p := &testKeyProvider{}
	s := Open(b, Config{KeyProvider: p, PlaintextKeys: func(key string, _ []byte) bool { return strings.HasSuffix(key, "/marker") }})
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
}
