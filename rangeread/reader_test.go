package rangeread

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"github.com/axiomhq/objstore/storetest"
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
	s, fault := storetest.NewFault(storetest.New(t))
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
	s, fault := storetest.NewFault(storetest.New(t))
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
