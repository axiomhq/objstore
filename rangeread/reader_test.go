package rangeread

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

// waitingContext signals when Fetch's select has registered its flight.
type waitingContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

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

func TestFetchFollowersShareDeterministicError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"access-denied", objstore.ErrAccessDenied},
		{"not-found", objstore.ErrNotFound},
		{"range", objstore.ErrRange},
		{"corrupt", ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const followers = 8
			entered, release := make(chan struct{}), make(chan struct{})
			var gets atomic.Int32
			s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
				return &readBackend{Backend: b, get: func(ctx context.Context, _ string) ([]byte, error) {
					if gets.Add(1) == 1 {
						close(entered)
					}
					select {
					case <-release:
						return nil, fmt.Errorf("read: %w", tc.err)
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}}
			})
			objects := cache.New(s, 1<<20, nil, cache.Keys{})
			t.Cleanup(objects.Close)
			r := newReader(t, s, objects, Config{})
			done := make(chan error, followers+1)
			go func() { _, err := r.Fetch(ctx, "k"); done <- err }()
			await(t, entered, "leader GET")
			for range followers {
				fctx := &waitingContext{Context: ctx, waiting: make(chan struct{})}
				go func() { _, err := r.Fetch(fctx, "k"); done <- err }()
				await(t, fctx.waiting, "follower join")
			}
			close(release)
			for range followers + 1 {
				if err := await(t, done, "read"); !errors.Is(err, tc.err) {
					t.Errorf("read: %v, want %v", err, tc.err)
				}
			}
			if got := gets.Load(); got != 1 {
				t.Fatalf("%d followers made %d GETs, want 1", followers, got)
			}
		})
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
// gives up leaves promptly while the GET runs on for the remaining waiter.
func TestFetchFollowerContextCanCancel(t *testing.T) {
	ctx, deadline := context.WithTimeout(t.Context(), 5*time.Second)
	defer deadline()
	entered, stopped := make(chan struct{}), make(chan struct{})
	s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
		return &readBackend{Backend: b, get: func(ctx context.Context, _ string) ([]byte, error) {
			close(entered)
			defer close(stopped)
			<-ctx.Done()
			return nil, ctx.Err()
		}}
	})
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	const key = "x/seg/stall"
	leaderCtx, cancelLeader := context.WithCancel(ctx)
	defer cancelLeader()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := r.Fetch(leaderCtx, key)
		leaderDone <- err
	}()
	await(t, entered, "leader GET")
	followerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fctx := &waitingContext{Context: followerCtx, waiting: make(chan struct{})}
	followerDone := make(chan error, 1)
	go func() {
		_, err := r.Fetch(fctx, key)
		followerDone <- err
	}()
	await(t, fctx.waiting, "follower registration")
	cancel()
	if err := await(t, followerDone, "follower"); !errors.Is(err, context.Canceled) {
		t.Fatalf("follower error = %v, want context.Canceled", err)
	}
	select {
	case err := <-leaderDone:
		t.Fatalf("leader finished with the follower: %v", err)
	default:
	}
	cancelLeader()
	if err := await(t, leaderDone, "leader"); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	await(t, stopped, "last waiter cancelled the GET")
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
