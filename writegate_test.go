package objstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
)

// TestWriteGateBoundsBulkWritesButNotUrgent pins the write path decoupling:
// with one write slot held by a paused bulk put, a second bulk put waits
// (and honours its context), while an Urgent put — the shape of a WAL
// commit, manifest swap, or lease heartbeat — goes straight through.
func TestWriteGateBoundsBulkWritesButNotUrgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	base := openFS(t, objstore.Config{MaxInflightWrites: 1})
	s, first, resume := pauseWrite(t, ctx, base, "bulk-1")

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := s.Put(short, "bulk-2", []byte("b")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second bulk put with the slot held: err=%v, want deadline exceeded", err)
	}
	if err := s.Put(objstore.Urgent(ctx), "urgent", []byte("u")); err != nil {
		t.Fatalf("urgent put behind a held slot: %v", err)
	}
	if _, err := s.Get(ctx, "bulk-2"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("blocked bulk put must not have landed: err=%v", err)
	}

	resume()
	if err := receiveWrite(t, ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "bulk-2", []byte("b")); err != nil {
		t.Fatalf("bulk put after the slot freed: %v", err)
	}
	if got, err := s.Get(ctx, "bulk-2"); err != nil || string(got) != "b" {
		t.Fatalf("bulk-2 after release: %q, %v", got, err)
	}
}

// TestNegativeMaxInflightWritesIsUnbounded: a negative bound means no
// bound, not a gate nobody can pass.
func TestNegativeMaxInflightWritesIsUnbounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	base := openFS(t, objstore.Config{MaxInflightWrites: -1})
	s, first, resume := pauseWrite(t, ctx, base, "held")
	for i := range 4 {
		if err := s.Put(ctx, fmt.Sprintf("k%d", i), []byte("b")); err != nil {
			t.Fatalf("put %d beside a held write: %v", i, err)
		}
	}
	resume()
	if err := receiveWrite(t, ctx, first); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteManyEmptyBypassesWriteGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	base := openFS(t, objstore.Config{MaxInflightWrites: 1})
	s, first, resume := pauseWrite(t, ctx, base, "held")
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := s.DeleteMany(short); err != nil {
		t.Fatalf("empty DeleteMany waited for a write slot: %v", err)
	}
	resume()
	if err := receiveWrite(t, ctx, first); err != nil {
		t.Fatal(err)
	}
}

func TestWriteGateCleanupOnEarlyReturn(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	base := openFS(t, objstore.Config{MaxInflightWrites: 1})
	var first <-chan error
	t.Run("PausedWriter", func(child *testing.T) {
		_, first, _ = pauseWrite(child, ctx, base, "held")
		// Return without Resume, as a failed assertion would. Cleanup must
		// drain the writer before the test's bucket can be removed.
	})
	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded.Done():
		t.Fatalf("paused writer outlived its test: %v", bounded.Err())
	}
}

func pauseWrite(t *testing.T, ctx context.Context, base *objstore.Store, key string) (*objstore.Store, <-chan error, func()) {
	t.Helper()
	s, f := storetest.Faulty(t, base)
	f.Set(storetest.Plan{Op: storetest.OpPut, N: 1, Mode: storetest.Pause, Key: key})
	first := make(chan error, 1)
	go func() { first <- s.Put(ctx, key, []byte("a")) }()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for f.Fired() == 0 {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("first put never reached the store: %v", ctx.Err())
		}
	}
	return s, first, f.Resume
}

func receiveWrite(t *testing.T, ctx context.Context, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatalf("put did not finish: %v", ctx.Err())
		return ctx.Err()
	}
}

// openFS returns a Store on a fresh file bucket, skipping t where the file
// backend is unsupported.
func openFS(t *testing.T, cfg objstore.Config) *objstore.Store {
	t.Helper()
	s := fs.Open(t.TempDir(), "b", cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.EnsureBucket(ctx); errors.Is(err, errors.ErrUnsupported) {
		t.Skip(err)
	} else if err != nil {
		t.Fatal(err)
	}
	return s
}
