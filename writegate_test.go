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
	ctx := context.Background()
	base := openFS(t, objstore.Config{MaxInflightWrites: 1})
	s, f := storetest.NewFault(base)
	f.Set(storetest.Plan{Op: storetest.OpPut, N: 1, Mode: storetest.Pause, Key: "bulk-1"})

	first := make(chan error, 1)
	go func() { first <- s.Put(ctx, "bulk-1", []byte("a")) }()
	deadline := time.Now().Add(5 * time.Second)
	for f.Fired() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first put never reached the store")
		}
		time.Sleep(time.Millisecond)
	}

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

	f.Resume()
	if err := <-first; err != nil {
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
	ctx := context.Background()
	base := openFS(t, objstore.Config{MaxInflightWrites: -1})
	s, f := storetest.NewFault(base)
	f.Set(storetest.Plan{Op: storetest.OpPut, N: 1, Mode: storetest.Pause, Key: "held"})
	first := make(chan error, 1)
	go func() { first <- s.Put(ctx, "held", []byte("a")) }()
	for f.Fired() == 0 {
		time.Sleep(time.Millisecond)
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for i := range 4 {
		if err := s.Put(bounded, fmt.Sprintf("k%d", i), []byte("b")); err != nil {
			t.Fatalf("put %d beside a held write: %v", i, err)
		}
	}
	f.Resume()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

// openFS returns a Store on a fresh file bucket, skipping t where the file
// backend is unsupported.
func openFS(t *testing.T, cfg objstore.Config) *objstore.Store {
	t.Helper()
	s := fs.Open(t.TempDir(), "b", cfg)
	if err := s.EnsureBucket(context.Background()); errors.Is(err, errors.ErrUnsupported) {
		t.Skip(err)
	} else if err != nil {
		t.Fatal(err)
	}
	return s
}
