package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/axiomhq/objstore"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Cache is the process's path to immutable objects: an in-memory byte cache,
// a disposable local disk cache, one gate in front of the store, one
// singleflight across concurrent misses of a key. Everything immutable is
// read through FetchWith, and the tiers decide who answers.
//
// Keys are logical. The cache interprets them only through Keys, and a key
// under ns/<name>/ belongs to namespace <name> (InvalidateNamespace, pins,
// ExpireInactive), the same convention objstore's encryption uses.
type Cache struct {
	Store  *objstore.Store
	Memory *ByteCache
	WAL    *ByteCache
	// Resident holds decoded log pages outside the memory budget. New
	// leaves it empty; replace it with NewResident to give it a budget.
	Resident     *Resident
	Disk         *Disk
	keys         Keys
	flight       singleflight.Group  // coalesces concurrent loads of one key
	Gate         *semaphore.Weighted // GateWidth store GET slots; occupancy in GateInUse
	GateInUse    atomic.Int64
	invalidateMu sync.Mutex
	classes      classCounters
}

// Keys tells the cache what a key holds. A nil func matches no key.
type Keys struct {
	// WAL keys are sequentially scanned log pages. They get their own
	// share of the memory budget (Cache.WAL), so one cold scan cannot
	// evict everything else.
	WAL func(key string) bool
	// Low keys share the memory budget at low priority (ByteCache.SetLow):
	// bulk bytes whose useful part lives on under another key.
	Low func(key string) bool
	// Ranged keys are counted as ClassBlock in ClassCounts, the rest as
	// ClassObject.
	Ranged func(key string) bool
}

func (k Keys) wal(key string) bool    { return k.WAL != nil && k.WAL(key) }
func (k Keys) ranged(key string) bool { return k.Ranged != nil && k.Ranged(key) }

// New wires the tiers. disk may be nil (no disk cache).
func New(s *objstore.Store, memoryBytes int, disk *Disk, keys Keys) *Cache {
	// WAL pages are large and sequentially scanned. A separate bounded share
	// keeps one cold log scan from retaining most of the budget, and keeps
	// index objects out of that churn.
	walBytes := 0
	if keys.WAL != nil && memoryBytes >= 128<<20 {
		walBytes = min(64<<20, memoryBytes/4)
	}
	memory := NewByteCache(memoryBytes - walBytes)
	if keys.Low != nil {
		// Low entries fill what the rest leaves free, down to a floor of an
		// eighth of the budget: a floor near half let bulk reads evict the
		// views just decoded from them.
		memory.SetLow(keys.Low, min(memoryBytes/8, 128<<20))
	}
	return &Cache{Store: s, Memory: memory, WAL: NewByteCache(walBytes), Resident: NewResident(0),
		Disk: disk, keys: keys, Gate: semaphore.NewWeighted(GateWidth)}
}

// ByteCacheFor picks the budget a key is charged to: WAL pages their own,
// everything else the memory budget.
func (c *Cache) ByteCacheFor(key string) *ByteCache {
	if c.keys.wal(key) {
		return c.WAL
	}
	return c.Memory
}

func (c *Cache) InvalidateNamespace(name string) {
	c.invalidateMu.Lock()
	defer c.invalidateMu.Unlock()
	// Retire in-flight publications in all budgets before sweeping any.
	c.Memory.Generation.Add(1)
	c.WAL.Generation.Add(1)
	c.Memory.sweepNamespace(name)
	c.WAL.sweepNamespace(name)
	c.Resident.InvalidateNamespace(name)
}

func (c *Cache) Close() { c.Disk.Close() }

// Put writes an immutable object and caches it on the way past, so the
// process that published it never re-reads it.
func (c *Cache) Put(ctx context.Context, key string, data []byte) error {
	if err := c.Store.Put(ctx, key, data); err != nil {
		return err
	}
	memory := c.ByteCacheFor(key)
	memory.Put(key, data, memory.Generation.Load())
	return nil
}

// GateWidth bounds concurrent store GETs (Cache.Gate) so a wide query
// cannot stampede the bucket. semaphore.Weighted keeps its occupancy private, so
// AcquireGet/ReleaseGet count it for Stats.
const GateWidth = 32

// AcquireGet takes one store GET slot or returns ctx's error, never both: a
// caller who is already gone never starts new store work (Acquire refuses a
// done context even when a slot is free, and hands back one it won as the
// context ended).
func (c *Cache) AcquireGet(ctx context.Context) error {
	if err := c.Gate.Acquire(ctx, 1); err != nil {
		return err
	}
	c.GateInUse.Add(1)
	return nil
}

func (c *Cache) ReleaseGet() {
	c.GateInUse.Add(-1)
	c.Gate.Release(1)
}

// FetchWith gives immutable ranged blocks the same memory/disk/singleflight
// path as whole objects. The cache key is logical; load performs the exact
// store operation only on a miss.
func (c *Cache) FetchWith(ctx context.Context, key string, load func(context.Context) ([]byte, error)) ([]byte, error) {
	return c.FetchCached(ctx, key, load, true)
}

// Physical coalesced ranges are internal parents of logical cache entries.
// Their lookup must not charge a second hit/miss for the same logical read.
// Store-tier transfers and Range* counters still account for physical I/O.
func (c *Cache) FetchCached(ctx context.Context, key string, load func(context.Context) ([]byte, error), logical bool) ([]byte, error) {
	return c.fetchCached(ctx, key, load, logical, 0)
}

// FetchCachedRange caches a decoded single-child range while charging the
// store tier for its physical length instead of the decoded child's length.
func (c *Cache) FetchCachedRange(ctx context.Context, key string, storedBytes int, load func(context.Context) ([]byte, error)) ([]byte, error) {
	return c.fetchCached(ctx, key, load, false, storedBytes)
}

func (c *Cache) fetchCached(ctx context.Context, key string, load func(context.Context) ([]byte, error), logical bool, storedBytes int) ([]byte, error) {
	memory := c.ByteCacheFor(key)
	if b, ok := Scoped(ctx, key); ok {
		return b, nil
	}
	lookup := memory.Peek
	if logical {
		lookup = memory.Get
	}
	if b, ok := lookup(key); ok {
		if logical {
			c.RecordRequestHit(ctx, key, true)
		}
		return b, nil
	}
	if logical {
		c.classes.misses[c.keys.class(key)].Add(1) // the request's is charged below, once the flight says
	}
	generation := memory.Generation.Load()
	flightKey := DiskKey(key, generation)
	flightGroupKey := flightKey
	if storedBytes > 0 {
		// A whole-object fetch can call FetchCachedRange for its own child
		// key. Give the inner range a distinct flight so it cannot wait on
		// itself.
		flightGroupKey += "\x00range-child"
	}
	type fetched struct {
		data   []byte
		hit    bool
		disk   bool // the disk tier answered
		memory bool // a concurrent fill had put it in memory
	}
	ch := c.flight.DoChan(flightGroupKey, func() (v any, err error) {
		// DoChan re-raises a loader panic on singleflight's own goroutine,
		// where nothing can recover it: convert it into every waiter's error.
		defer func() {
			if p := recover(); p != nil {
				v, err = nil, fmt.Errorf("cache: fetch %s: panic: %v\n%s", key, p, debug.Stack())
			}
		}()
		if b, ok := memory.Peek(key); ok {
			return fetched{data: b, hit: true, memory: true}, nil
		}
		if b, ok := c.Disk.Get(flightKey); ok {
			if logical {
				c.noteDiskHit(key)
			}
			memory.Put(key, b, generation)
			return fetched{data: b, hit: true, disk: true}, nil
		}
		parent, _ := ctx.Value(loadSourceKey{}).(*loadSource)
		source := &loadSource{parent: parent}
		b, err := load(context.WithValue(ctx, loadSourceKey{}, source))
		if err != nil {
			return nil, err
		}
		fromCache := source.cached.Load() && !source.stored.Load()
		if !fromCache { // a slice of an object a tier holds is on disk already
			c.Disk.Put(flightKey, b)
		}
		memory.Put(key, b, generation)
		return fetched{data: b, hit: fromCache}, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		got := r.Val.(fetched)
		if logical {
			if got.hit {
				c.RecordRequestHit(ctx, key, got.memory)
			} else {
				RecordRequestMiss(ctx, key)
			}
			if !got.memory {
				recordRequestMiss(ctx, c.keys.class(key))
			}
			if got.disk {
				recordRequestDiskHit(ctx, c.keys.class(key))
			}
		}
		if !got.hit {
			NoteStoreLoad(ctx, key)
		}
		return got.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// FromDisk answers a logical key the memory tier missed from the disk tier,
// counts it as a disk hit and promotes it into memory, so the next lookup
// is a memory hit and the disk is read once per eviction, not per query.
func (c *Cache) FromDisk(ctx context.Context, key string) ([]byte, bool) {
	memory := c.ByteCacheFor(key)
	generation := memory.Generation.Load()
	b, ok := c.Disk.Get(DiskKey(key, generation))
	if ok {
		c.NoteDiskServed(ctx, key)
		memory.Put(key, b, generation)
	}
	return b, ok
}

// CachedRange returns bytes [off, off+n) of an immutable object only when a
// cache tier already holds the object. It never reads object storage.
// Memory answers with a slice of its cached bytes, which callers must not
// mutate or retain; the disk tier reads and verifies only the covering
// blocks (fromDisk) and never promotes the object into memory: an object
// may be tens of megabytes, and a request wants a few kilobytes of it.
// Peek, because this is an opportunistic physical-parent lookup for a
// logical request that owns the hit and miss accounting.
func (c *Cache) CachedRange(key string, off, n int64) (b []byte, fromDisk, ok bool) {
	if off < 0 || n < 0 {
		return nil, false, false
	}
	memory := c.ByteCacheFor(key)
	if whole, ok := memory.Peek(key); ok {
		if off > int64(len(whole)) || n > int64(len(whole))-off {
			return nil, false, false
		}
		return whole[off : off+n : off+n], false, true
	}
	if b, ok := c.Disk.GetRange(DiskKey(key, memory.Generation.Load()), off, n); ok {
		return b, true, true
	}
	return nil, false, false
}

// loadSource records, for every enclosing fetchCached load, whether its
// work was served from a whole object already in a cache tier and whether
// it reached object storage. A load that only cut its bytes from a cached
// object (an extent of an object a tier holds, e.g. after a warm) is a
// request hit; any store read, or a load that reports neither, is a miss.
type loadSource struct {
	cached, stored atomic.Bool
	parent         *loadSource
}

type loadSourceKey struct{}

// MarkServedFromCache tells the enclosing loads their bytes came from
// object, which a cache tier held. An object this same request read from
// the store is no cache hit for it: its slices stay misses.
func MarkServedFromCache(ctx context.Context, object string) {
	if loadedByRequest(ctx, object) {
		markStoreRead(ctx)
		return
	}
	for f, _ := ctx.Value(loadSourceKey{}).(*loadSource); f != nil; f = f.parent {
		f.cached.Store(true)
	}
}

func markStoreRead(ctx context.Context) {
	for f, _ := ctx.Value(loadSourceKey{}).(*loadSource); f != nil; f = f.parent {
		f.stored.Store(true)
	}
}

// Gated runs one store read under the gate.
func (c *Cache) Gated(ctx context.Context, read func(context.Context) ([]byte, error)) ([]byte, error) {
	if b, ok := ctx.Value(budgetKey{}).(*Budget); ok && b.Spend() {
		return nil, ErrBudget
	}
	markStoreRead(ctx)
	if err := c.AcquireGet(ctx); err != nil {
		return nil, err // no slot taken; do not release
	}
	defer c.ReleaseGet()
	return read(ctx)
}

type resultsKey struct{}

// WithResults publishes one query stage's exact children on ctx. FetchCached
// prefers them over the LRU so a tiny cache cannot drop a working set the
// stage already paid for.
func WithResults(ctx context.Context, results map[string][]byte) context.Context {
	return context.WithValue(ctx, resultsKey{}, results)
}

// Scoped looks up a logical key in the stage results published by WithResults.
func Scoped(ctx context.Context, key string) ([]byte, bool) {
	results, _ := ctx.Value(resultsKey{}).(map[string][]byte)
	b, ok := results[key]
	return b, ok
}

type budgetKey struct{}

// Budget caps the store requests one BACKGROUND job may issue. Nothing on a
// request path carries one.
type Budget struct {
	max   int64
	spent atomic.Int64
}

// ErrBudget is the background-read budget's exhaustion.
var ErrBudget = errors.New("cache: background read budget exhausted")

// WithBudget bounds the store requests reads under ctx may make.
func WithBudget(ctx context.Context, max int) (context.Context, *Budget) {
	b := &Budget{max: int64(max)}
	return context.WithValue(ctx, budgetKey{}, b), b
}

func (b *Budget) Spend() bool    { return b.spent.Add(1) > b.max }
func (b *Budget) Exceeded() bool { return b.spent.Load() > b.max }
