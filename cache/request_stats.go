package cache

import (
	"context"
	"sync"
	"sync/atomic"
)

type requestStatsKey struct{}

// RequestStats follows a query context through cache and range fetches.
// Atomics allow a query's parallel range workers to charge the same request.
type RequestStats struct {
	// classes are this request's lookups by class and outcome
	// (ClassCounts); the Cache's counters are the process-wide sums.
	classes classCounters
	// loaded names the objects this request read from object storage, so
	// a slice it later cuts from one of them is still charged as a load.
	loaded sync.Map
	// missed names the keys this request missed in memory, marked at the
	// miss: its parallel workers look a block up several times, and
	// whether a sibling's fill lands before a lookup is scheduling, so
	// every lookup but the one that missed first is charged as a load. A
	// request is only as warm as the store reads it did not need.
	missed sync.Map
	// built names the views this request decoded: it counted the lookups
	// of their inputs, and its own reads of them count nothing.
	built sync.Map
}

// NoteBuilt records that this request decoded the view under key.
func NoteBuilt(ctx context.Context, key string) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.built.Store(key, true)
	}
}

// NoteStoreLoad records that this request read object (or a logical key)
// from the store.
func NoteStoreLoad(ctx context.Context, object string) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.loaded.Store(object, true)
	}
}

func loadedByRequest(ctx context.Context, object string) bool {
	s, _ := ctx.Value(requestStatsKey{}).(*RequestStats)
	if s == nil {
		return false
	}
	_, ok := s.loaded.Load(object)
	return ok
}

func WithRequestStats(ctx context.Context) (context.Context, *RequestStats) {
	s := &RequestStats{}
	return context.WithValue(ctx, requestStatsKey{}, s), s
}

// AddRequestLookups charges lookups made elsewhere on this request's behalf:
// the cache lookups of a shard leg another query node ran.
func AddRequestLookups(ctx context.Context, classes ClassCounts) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.classes.add(classes)
	}
}

// Classes is this request's lookups by class and outcome.
func (s *RequestStats) Classes() ClassCounts { return s.classes.snapshot() }

// Class is what a cache key holds, for the per-class lookup counters.
type Class int

const (
	ClassBlock  Class = iota // a ranged read (Keys.Ranged)
	ClassObject              // a whole object
	NumClasses
)

func (k Keys) class(key string) Class {
	if k.ranged(key) {
		return ClassBlock
	}
	return ClassObject
}

// Outcome is who answered one logical cache lookup.
type Outcome int

const (
	// MemoryHit: the memory tier held the bytes or a decoded view of them,
	// or a load in flight for another caller delivered them (that load is
	// counted once, by the caller that ran it).
	MemoryHit Outcome = iota
	// DiskHit: the memory tier missed and the disk tier answered, from the
	// object itself or from a whole object it holds that the bytes lie in.
	DiskHit
	// Load: neither tier answered; the bytes came from object storage (or
	// from an object this same request had read from it).
	Load
)

// ClassCounts are logical cache lookups by class, as three disjoint counts
// that sum to the lookups: the memory tier's hits, the disk tier's hits,
// and the loads from object storage.
type ClassCounts struct {
	MemoryHits [NumClasses]int64 `json:"memory_hits"`
	DiskHits   [NumClasses]int64 `json:"disk_hits"`
	Loads      [NumClasses]int64 `json:"loads"`
}

// HitRatio is the share of lookups a cache tier answered, 1 with none.
func (c ClassCounts) HitRatio() float64 {
	var hits, total int64
	for i := range NumClasses {
		hits += c.MemoryHits[i] + c.DiskHits[i]
		total += c.MemoryHits[i] + c.DiskHits[i] + c.Loads[i]
	}
	if total == 0 {
		return 1
	}
	return float64(hits) / float64(total)
}

type classCounters struct {
	n [3][NumClasses]atomic.Int64 // by Outcome
}

func (cc *classCounters) add(c ClassCounts) {
	for i := range NumClasses {
		cc.n[MemoryHit][i].Add(c.MemoryHits[i])
		cc.n[DiskHit][i].Add(c.DiskHits[i])
		cc.n[Load][i].Add(c.Loads[i])
	}
}

// Note counts one logical lookup of key with outcome o, for the process
// and for ctx's request. For the request, a hit on a key it missed before
// is a load (MarkMissed), and a hit on a view it decoded is not a lookup
// (NoteBuilt).
func (c *Cache) Note(ctx context.Context, key string, o Outcome) { c.NoteAs(ctx, key, o, false) }

// NoteAs is Note for the lookup that marked key missed (owner, from
// MarkMissed): its own outcome stands.
func (c *Cache) NoteAs(ctx context.Context, key string, o Outcome, owner bool) {
	class := c.keys.class(key)
	c.classes.n[o][class].Add(1)
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		if _, ok := s.built.Load(key); ok {
			return
		}
		if o != Load && !owner {
			if _, ok := s.missed.Load(key); ok {
				o = Load
			}
		}
		s.classes.n[o][class].Add(1)
	}
}

// MarkMissed records that ctx's request missed key in memory, and reports
// whether this lookup is the first of the request's to miss it (the owner,
// whose outcome NoteAs keeps). Without request stats every lookup owns.
func MarkMissed(ctx context.Context, key string) (owner bool) {
	s, _ := ctx.Value(requestStatsKey{}).(*RequestStats)
	if s == nil {
		return true
	}
	_, seen := s.missed.LoadOrStore(key, true)
	return !seen
}

// ClassCounts snapshots the process-wide counters.
func (c *Cache) ClassCounts() ClassCounts { return c.classes.snapshot() }

func (cc *classCounters) snapshot() ClassCounts {
	var out ClassCounts
	for i := range NumClasses {
		out.MemoryHits[i] = cc.n[MemoryHit][i].Load()
		out.DiskHits[i] = cc.n[DiskHit][i].Load()
		out.Loads[i] = cc.n[Load][i].Load()
	}
	return out
}
