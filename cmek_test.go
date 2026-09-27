package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"sync"
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
	s, err := New(ctx, "file://"+t.TempDir(), "cmek")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	p := &meteredAWSProvider{}
	s.ConfigureCMEK(p)
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
	time.Sleep(170 * time.Millisecond)
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
	root := t.TempDir()
	p := &testKeyProvider{}
	open := func() *Store {
		s, err := New(ctx, "file://"+root, "cmek")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.EnsureBucket(ctx); err != nil {
			t.Fatal(err)
		}
		s.ConfigureCMEK(p)
		return s
	}
	first, second := open(), open()
	fail := &failDelete{backend: first.b, key: "cmek/test"}
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
	if _, err := fail.backend.Get(ctx, "cmek/test"); !errors.Is(err, ErrNotFound) {
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
	root := t.TempDir()
	s, err := New(ctx, "file://"+root, "cmek")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	provider := &testKeyProvider{}
	s.ConfigureCMEK(provider)
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
	reopened, err := New(ctx, "file://"+root, "cmek")
	if err != nil {
		t.Fatal(err)
	}
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
	s.cmek.mu.Lock()
	delete(s.cmek.keys, "test")
	s.cmek.mu.Unlock()
	provider.revoked = true
	if _, err := s.Get(ctx, key); !errors.Is(err, kms.ErrKeyUnavailable) {
		t.Fatalf("revoked: %v", err)
	}
	provider.revoked = false
	time.Sleep(120 * time.Millisecond)
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
	backend
	key   string
	armed bool
}

func (f *failDelete) Delete(ctx context.Context, key string) error {
	if f.armed && key == f.key {
		return errInjected
	}
	return f.backend.Delete(ctx, key)
}

func (f *failDelete) DeleteMany(ctx context.Context, keys ...string) error {
	if f.armed && slices.Contains(keys, f.key) {
		return errInjected
	}
	return f.backend.DeleteMany(ctx, keys...)
}
