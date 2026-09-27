package rangeread

import (
	"context"
	"sync"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Reader reads immutable objects by byte range on top of the object cache:
// it plans and coalesces the ranged GETs of one read stage (FetchRanges)
// and counts the physical range work. The cache remembers what it reads and
// does not know it exists. Build one per process.
type Reader struct {
	Store   *objstore.Store
	Objects *cache.Cache
	Cfg     Config
	Memory  *semaphore.Weighted // cfg.MaxInFlightBytes across the process
	IO      Counters
	// parents shares one GET among concurrent identical coalesced parent
	// ranges (FetchRanges), without caching the parent.
	parents singleflight.Group
}

func New(s *objstore.Store, objects *cache.Cache, cfg Config) *Reader {
	return &Reader{Store: s, Objects: objects, Cfg: cfg, Memory: semaphore.NewWeighted(cfg.MaxInFlightBytes)}
}

// Fetch returns an immutable object via the cache. Cached bytes must never
// be mutated: every decoder copies out. Concurrent misses for the same key
// share one GET under the leader's ctx, but each caller waits under its
// own: a background prefetch unwinds at shutdown behind a request-led GET,
// and a cancelled request does not hold its admission slot until the
// leader's GET ends. The shared call runs on and
// is forgotten when it completes.
func (r *Reader) Fetch(ctx context.Context, key string) ([]byte, error) {
	return r.Objects.FetchWith(ctx, key, func(ctx context.Context) ([]byte, error) {
		return r.Objects.Gated(ctx, func(ctx context.Context) ([]byte, error) { return r.Store.Get(ctx, key) })
	})
}

// Prefetch warms the cache for keys via at most gate-width workers; flight
// coalesces with any racing consumer. Cancellation sheds pending keys.
// Errors are dropped on purpose: the serial consumer re-fetches and
// surfaces the real error with its own context.
func (r *Reader) Prefetch(ctx context.Context, keys ...string) {
	var remaining []string
	for _, k := range keys {
		if _, ok := r.Objects.Memory.Peek(k); ok { // Peek: the consumer's fetch owns the hit/miss accounting
			continue
		}
		remaining = append(remaining, k)
	}
	if len(remaining) == 0 {
		return
	}
	work := make(chan string, len(remaining))
	for _, k := range remaining {
		work <- k
	}
	close(work)
	workers := min(cache.GateWidth, len(remaining))
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				if ctx.Err() != nil {
					return
				}
				r.Fetch(ctx, k) //nolint:errcheck // warm-only; consumer surfaces errors
			}
		}()
	}
	wg.Wait()
}
