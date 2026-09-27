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
	hits   atomic.Int64
	misses atomic.Int64
	// classes are this request's memory misses and disk hits by class
	// (ClassCounts); the Cache's counters are the process-wide sums.
	classes classCounters
	// loaded names the objects this request read from object storage, so a
	// slice it later cuts from one of them is still charged as a miss.
	loaded sync.Map
	// missed names the keys this request missed, so a later lookup of one
	// that a sibling worker's fill answers is still charged as a miss.
	missed sync.Map
}

// NoteStoreLoad records that this request read object from the store.
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

func RecordRequestLookup(ctx context.Context, hit bool) {
	s, _ := ctx.Value(requestStatsKey{}).(*RequestStats)
	if s == nil {
		return
	}
	if hit {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
}

// RecordRequestMiss records a lookup of key no cache tier answered.
func RecordRequestMiss(ctx context.Context, key string) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.missed.Store(key, true)
		s.misses.Add(1)
	}
}

// RecordRequestHit records a lookup of key a cache tier answered. A key this
// same request missed is no hit for it: its parallel workers look a block up
// several times, and whether a sibling's fill lands before a lookup is
// scheduling, so every lookup after the first miss is charged as the miss it
// is when they overlap. memory: the memory tier answered, and the charged
// miss is also this request's memory miss.
func (c *Cache) RecordRequestHit(ctx context.Context, key string, memory bool) {
	s, _ := ctx.Value(requestStatsKey{}).(*RequestStats)
	if s == nil {
		return
	}
	if _, ok := s.missed.Load(key); !ok {
		s.hits.Add(1)
		return
	}
	s.misses.Add(1)
	if memory {
		s.classes.misses[c.keys.class(key)].Add(1)
	}
}

// AddRequestLookups charges lookups made elsewhere on this request's behalf:
// the cache hits and misses of a shard leg another query node ran.
func AddRequestLookups(ctx context.Context, hits, misses int64, classes ClassCounts) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.hits.Add(hits)
		s.misses.Add(misses)
		for i := range NumClasses {
			s.classes.misses[i].Add(classes.Misses[i])
			s.classes.diskHits[i].Add(classes.DiskHits[i])
		}
	}
}

func (s *RequestStats) Counts() (hits, misses int64) {
	return s.hits.Load(), s.misses.Load()
}

// Classes is this request's per-class memory misses and disk hits.
func (s *RequestStats) Classes() ClassCounts { return s.classes.snapshot() }

// Class is what a cache key holds, for the per-class miss counters.
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

// ClassCounts are memory-tier misses of logical keys and the disk tier's
// hits among them, by class. A miss the disk does not answer is a load
// (a store read, or a slice of a whole object some tier holds).
type ClassCounts struct {
	Misses   [NumClasses]int64 `json:"misses"`
	DiskHits [NumClasses]int64 `json:"disk_hits"`
}

type classCounters struct {
	misses, diskHits [NumClasses]atomic.Int64
}

// NoteMiss records a logical lookup the memory tier missed, for the
// process and for ctx's request.
func (c *Cache) NoteMiss(ctx context.Context, key string) {
	c.classes.misses[c.keys.class(key)].Add(1)
	recordRequestMiss(ctx, c.keys.class(key))
}

func recordRequestMiss(ctx context.Context, class Class) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.classes.misses[class].Add(1)
	}
}

func (c *Cache) noteDiskHit(key string) { c.classes.diskHits[c.keys.class(key)].Add(1) }

// recordRequestDiskHit charges ctx's request with a disk hit; the process
// counter is noteDiskHit's, charged once however many requests shared it.
func recordRequestDiskHit(ctx context.Context, class Class) {
	if s, _ := ctx.Value(requestStatsKey{}).(*RequestStats); s != nil {
		s.classes.diskHits[class].Add(1)
	}
}

// NoteDiskServed records a logical lookup the memory tier missed and the
// disk tier answered, for a caller that read the disk itself.
func (c *Cache) NoteDiskServed(ctx context.Context, key string) {
	c.NoteMiss(ctx, key)
	c.noteDiskHit(key)
	recordRequestDiskHit(ctx, c.keys.class(key))
}

// ClassCounts snapshots the per-class counters.
func (c *Cache) ClassCounts() ClassCounts { return c.classes.snapshot() }

func (cc *classCounters) snapshot() ClassCounts {
	var out ClassCounts
	for i := range NumClasses {
		out.Misses[i] = cc.misses[i].Load()
		out.DiskHits[i] = cc.diskHits[i].Load()
	}
	return out
}
