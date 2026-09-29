package rangeread

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func newReader(t *testing.T, s *objstore.Store, objects *cache.Cache, cfg Config) *Reader {
	t.Helper()
	r, err := New(s, objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReaderFetch(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	if err := s.Put(t.Context(), "ns/f/obj", []byte("body")); err != nil {
		t.Fatal(err)
	}
	fault.ResetOps()
	for range 3 {
		got, err := r.Fetch(t.Context(), "ns/f/obj")
		if err != nil || string(got) != "body" {
			t.Fatalf("Fetch = %q, %v", got, err)
		}
	}
	if n := fault.Ops()[storetest.OpGet]; n != 1 {
		t.Fatalf("3 Fetches made %d GETs, want 1", n)
	}
	if _, err := r.Fetch(t.Context(), "ns/f/missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestReaderPrefetch(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	var keys []string
	for i := range 3 * cache.GateWidth {
		k := fmt.Sprintf("ns/p/%03d", i)
		if err := s.Put(t.Context(), k, []byte(k)); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}

	t.Run("Warms", func(t *testing.T) {
		fault.ResetOps()
		r.Prefetch(t.Context(), append(keys, "ns/p/missing")...) // synchronous; a miss is dropped
		if n := fault.Ops()[storetest.OpGet]; n != len(keys)+1 {
			t.Fatalf("Prefetch made %d GETs, want %d", n, len(keys)+1)
		}
		for _, k := range keys {
			if _, ok := objects.ByteCacheFor(k).Peek(k); !ok {
				t.Fatalf("%s not warm after Prefetch returned", k)
			}
		}
	})

	t.Run("SkipsWarmKeys", func(t *testing.T) {
		fault.ResetOps()
		r.Prefetch(t.Context(), keys...)
		if n := fault.Ops()[storetest.OpGet]; n != 0 {
			t.Fatalf("Prefetch of warm keys made %d GETs", n)
		}
	})

	t.Run("CancelledShedsEverything", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fault.ResetOps()
		r.Prefetch(ctx, "ns/p/cold-1", "ns/p/cold-2")
		if n := fault.Ops()[storetest.OpGet]; n != 0 {
			t.Fatalf("cancelled Prefetch made %d GETs", n)
		}
	})
}

// A follower on a shared fetch waits under its own context: a caller that
// gives up leaves promptly while the leader's GET runs on under the
// leader's context.
func TestFetchFollowerContextCanCancel(t *testing.T) {
	s, f := bucket.NewFaulty(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	const key = "x/seg/stall"
	f.Set(storetest.Plan{Op: storetest.OpGet, N: 1, Mode: storetest.Hang, Key: key})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := r.Fetch(leaderCtx, key)
		leaderDone <- err
	}()
	for deadline := time.Now().Add(5 * time.Second); f.Fired() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("leader never reached the store")
		}
	}
	misses := objects.Memory.Misses()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	followerDone := make(chan error, 1)
	go func() {
		_, err := r.Fetch(ctx, key)
		followerDone <- err
	}()
	// The follower's own miss precedes its join; it is parked behind the
	// leader from here on.
	for deadline := time.Now().Add(5 * time.Second); objects.Memory.Misses() == misses; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("follower never reached the flight")
		}
	}
	cancel()
	select {
	case err := <-followerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("follower error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled follower remained parked behind the leader's GET")
	}
	select {
	case err := <-leaderDone:
		t.Fatalf("leader finished with the follower: %v", err)
	default:
	}
	cancelLeader()
	select {
	case err := <-leaderDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not finish")
	}
}

// A loader panic reaches every waiter as an error and never the process:
// singleflight's DoChan re-raises it on a goroutine nothing can recover.
// A nil store panics inside the loader's GET.
func TestFetchLoaderPanicIsAnError(t *testing.T) {
	objects := cache.New(nil, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, nil, objects, Config{})
	const workers = 8
	errs := make(chan error, workers)
	for range workers {
		go func() {
			_, err := r.Fetch(context.Background(), "x/seg/boom")
			errs <- err
		}()
	}
	for range workers {
		select {
		case err := <-errs:
			if err == nil || !strings.Contains(err.Error(), "panic:") {
				t.Fatalf("waiter error = %v, want the loader's panic", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter never returned")
		}
	}
	if _, err := r.Fetch(context.Background(), "x/seg/boom"); err == nil || !strings.Contains(err.Error(), "panic:") {
		t.Fatalf("fetch after a panicking load = %v, want a fresh loader's panic, not a stuck flight", err)
	}
}
