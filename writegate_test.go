package objstore_test

import (
	"context"
	"errors"
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
	base := fs.Open(t.TempDir(), "b", objstore.Config{MaxInflightWrites: 1})
	if err := base.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
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
