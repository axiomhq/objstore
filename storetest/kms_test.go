package storetest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestKMSRecordsAndRevokes(t *testing.T) {
	ctx := context.Background()
	s, k := storetest.NewKMS(bucket.New(t))
	s = s.WithKMSKeys(func(_ context.Context, key string) (string, error) {
		if key == "secret" {
			return "key-a", nil
		}
		return "", nil
	})
	if !s.KMS() {
		t.Fatal("KMS store must report per-object keys")
	}
	for _, key := range []string{"secret", "plain"} {
		if err := s.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if k.KeyOf("secret") != "key-a" || k.KeyOf("plain") != "" {
		t.Fatalf("keys: secret=%q plain=%q", k.KeyOf("secret"), k.KeyOf("plain"))
	}
	k.Revoke("key-a")
	if _, err := s.Get(ctx, "secret"); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatalf("revoked read: %v", err)
	}
	if _, err := s.GetRange(ctx, "secret", 0, 1); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatalf("revoked range read: %v", err)
	}
	if err := s.Put(ctx, "secret", []byte("x")); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatalf("revoked write: %v", err)
	}
	if b, err := s.Get(ctx, "plain"); err != nil || string(b) != "plain" {
		t.Fatalf("unkeyed read: %q %v", b, err)
	}
	k.Restore("key-a")
	if b, err := s.Get(ctx, "secret"); err != nil || string(b) != "secret" {
		t.Fatalf("restored read: %q %v", b, err)
	}
}
