package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
)

type removedBackend struct {
	objstore.Backend
	after func()
}

func (b removedBackend) Delete(ctx context.Context, key string) error {
	err := b.Backend.Delete(ctx, key)
	if err == nil {
		b.after()
	}
	return err
}

func (b removedBackend) DeleteMany(ctx context.Context, keys ...string) error {
	err := b.Backend.DeleteMany(ctx, keys...)
	if err == nil {
		b.after()
	}
	return err
}

func TestKMSDeletePreservesReplacementKey(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "Delete", true: "DeleteMany"}[batch], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			raw := fs.Open(t.TempDir(), "b", objstore.Config{})
			if err := raw.EnsureBucket(ctx); errors.Is(err, errors.ErrUnsupported) {
				t.Skip(err)
			} else if err != nil {
				t.Fatal(err)
			}
			_, k := NewKMS(raw)
			if err := k.Put(objstore.WithKMSKey(ctx, "old"), "k", []byte("old")); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			wait := func() {
				t.Helper()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			completed := false
			k.Backend = removedBackend{k.Backend, func() {
				// Reads must not wait for a mutation paused in the backend.
				go func() {
					_, err := k.Get(ctx, "k")
					if errors.Is(err, objstore.ErrNotFound) {
						err = nil
					}
					done <- err
				}()
				wait()
				// Force the bad interleaving if mutations are not serialized.
				unlocked := k.mutation.TryLock()
				if unlocked {
					k.mutation.Unlock()
				}
				go func() { done <- k.Put(objstore.WithKMSKey(ctx, "new"), "k", []byte("new")) }()
				if unlocked {
					wait()
					completed = true
				}
			}}
			var err error
			if batch {
				err = k.DeleteMany(ctx, "k")
			} else {
				err = k.Delete(ctx, "k")
			}
			if err != nil {
				t.Fatal(err)
			}
			// A serialized replacement can complete only after Delete returns.
			if !completed {
				wait()
			}
			k.Revoke("new")
			if _, err := k.Get(ctx, "k"); !errors.Is(err, objstore.ErrAccessDenied) {
				t.Fatalf("replacement read after revoke: %v, key=%q, want access denied", err, k.KeyOf("k"))
			}
		})
	}
}
