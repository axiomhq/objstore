package rangeread

import (
	"context"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Reader reads immutable objects by byte range on top of the object cache:
// it plans and coalesces the ranged GETs of one read stage (FetchRanges)
// and counts the physical range work. The cache remembers what it reads and
// does not know it exists. Use New: on the zero Reader, Fetch and Prefetch
// panic and FetchRanges skips every stage.
type Reader struct {
	IO      Counters
	store   *objstore.Store
	objects *cache.Cache
	config  Config              // normalized by New
	memory  *semaphore.Weighted // config.MaxInFlightBytes across the process
	// parents shares one GET among concurrent identical coalesced parent
	// ranges (FetchRanges), without caching the parent.
	parents singleflight.Group
	joined  func() // test hook: called once a parent read has joined or led its flight
}

// New returns a Reader on s through objects. Zero fields of cfg take their
// defaults; an invalid cfg is an error (see Config.Normalized).
func New(s *objstore.Store, objects *cache.Cache, cfg Config) (*Reader, error) {
	cfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	return &Reader{store: s, objects: objects, config: cfg, memory: semaphore.NewWeighted(cfg.MaxInFlightBytes)}, nil
}

// Config is the normalized configuration the Reader was built with.
func (r *Reader) Config() Config { return r.config }

// Fetch returns an immutable object via the cache. Cached bytes must never
// be mutated: every decoder copies out. Concurrent misses for the same key
// share one GET under the leader's ctx, but each caller waits under its
// own: a background prefetch unwinds at shutdown behind a request-led GET,
// and a cancelled request does not hold its admission slot until the
// leader's GET ends. The shared call runs on and is forgotten when it
// completes.
func (r *Reader) Fetch(ctx context.Context, key string) ([]byte, error) {
	return r.objects.FetchWith(ctx, key, func(ctx context.Context) ([]byte, error) {
		return r.objects.Gated(ctx, func(ctx context.Context) ([]byte, error) { return r.store.Get(ctx, key) })
	})
}

// Prefetch warms the cache for keys and returns when every key is loaded,
// failed or shed: it is synchronous; run it in a goroutine to warm in the
// background. Keys already in their memory tier (ByteCacheFor) are skipped
// without counting a hit. At most cache.GateWidth fetches run at once, and
// each coalesces with any racing consumer. Cancellation sheds pending
// keys. Errors are dropped on purpose: the consumer re-fetches and surfaces
// the real error under its own context.
func (r *Reader) Prefetch(ctx context.Context, keys ...string) {
	var g errgroup.Group
	g.SetLimit(cache.GateWidth)
	for _, k := range keys {
		if _, ok := r.objects.ByteCacheFor(k).Peek(k); ok { // Peek: the consumer's fetch owns the hit/miss accounting
			continue
		}
		if ctx.Err() != nil {
			break
		}
		g.Go(func() error {
			if ctx.Err() == nil {
				r.Fetch(ctx, k) //nolint:errcheck // warm-only; consumer surfaces errors
			}
			return nil
		})
	}
	g.Wait() //nolint:errcheck // every worker returns nil
}
