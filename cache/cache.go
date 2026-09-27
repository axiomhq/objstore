package cache

import (
	"hash/maphash"
	"strings"
	"sync"
	"sync/atomic"
)

// Stripes is the lock-striping width: a power of two so the key hash masks
// to a stripe. Every fetch funnels through the cache, so one mutex around
// the whole map serialised them.
const Stripes = 16

// ByteCache holds immutable object bytes and decoded values under one budget.
// Local namespace deletion/recreation invalidates raw and derived entries.
//
// The budget is split evenly across stripes and LRU is per stripe, so
// eviction is approximate globally: the victim is the least recently used
// entry of the stripe the incoming key hashes to. An entry larger than a
// stripe's share is refused rather than evicting its whole stripe.
//
// Keys SetLow classifies share the budget at a lower priority: they fill
// whatever the other entries leave free and are evicted first, down to a
// floor below which the other entries give way instead. A working set whose
// regular and low entries both fit keeps both; one that does not loses low
// entries, never a regular one to a low one.
type ByteCache struct {
	Cap        int // total; each stripe holds cap/len(stripes)
	seed       maphash.Seed
	stripes    []cacheStripe
	low        func(key string) bool // nil: every entry is regular
	Generation atomic.Uint64         // invalidation also retires in-flight publications

	hits, misses       atomic.Int64
	lowHits, lowMisses atomic.Int64
}

type cacheStripe struct {
	mu       sync.Mutex
	cap      int
	size     int
	lowFloor int    // low entries below this charge are not evicted for regular ones
	lists    [2]lru // [0] regular, [1] low priority
	items    map[string]*entry
}

// lru is one priority's recency list and its charge.
type lru struct {
	head, tail *entry // head = most recently used
	size       int
}

type entry struct {
	key        string
	val        []byte
	low        bool
	decoded    sizedDecoded // optional typed decode of val; evicted with the bytes
	dsize      int          // charged retained size of decoded (see PutDecoded)
	prev, next *entry
}

type sizedDecoded interface {
	retainedBytes() int
	decodedValue() any
}

type concreteDecoded struct {
	value any
	bytes int
}

func (v concreteDecoded) retainedBytes() int { return v.bytes }
func (v concreteDecoded) decodedValue() any  { return v.value }

// Sizer is a decoded value that knows its retained charge: PutDecoded with
// a zero size charges CacheBytes.
type Sizer interface {
	CacheBytes() int
}

var _ sizedDecoded = concreteDecoded{}

func NewByteCache(capBytes int) *ByteCache { return NewByteCacheStripes(capBytes, Stripes) }

// NewByteCacheStripes is NewByteCache with an explicit stripe count. Tests that
// assert exact LRU order use one stripe.
func NewByteCacheStripes(capBytes, stripes int) *ByteCache {
	c := &ByteCache{Cap: capBytes, seed: maphash.MakeSeed(), stripes: make([]cacheStripe, stripes)}
	for i := range c.stripes {
		c.stripes[i] = cacheStripe{cap: capBytes / stripes, items: map[string]*entry{}}
	}
	return c
}

// SetLow marks the keys low reports as low priority and protects floor
// bytes of them from regular entries. Call before first use.
func (c *ByteCache) SetLow(low func(key string) bool, floor int) {
	c.low = low
	for i := range c.stripes {
		c.stripes[i].lowFloor = floor / len(c.stripes)
	}
}

func (c *ByteCache) isLow(key string) bool { return c.low != nil && c.low(key) }

func (c *ByteCache) stripe(key string) *cacheStripe {
	return &c.stripes[maphash.String(c.seed, key)&uint64(len(c.stripes)-1)]
}

// Callers must never mutate the returned slice — it is the shared cached value.
func (c *ByteCache) Get(key string) ([]byte, bool) {
	return c.get(key, true)
}

// Peek is Get without touching hit/miss counters. Used by the in-flight
// reload so a single logical miss is not counted twice.
func (c *ByteCache) Peek(key string) ([]byte, bool) {
	return c.get(key, false)
}

func (c *ByteCache) get(key string, stat bool) ([]byte, bool) {
	s := c.stripe(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if ok {
		s.touch(e)
	}
	s.mu.Unlock()
	if !stat {
		if ok {
			return e.val, true
		}
		return nil, false
	}
	if !ok {
		c.Missed(key)
		return nil, false
	}
	c.hits.Add(1)
	if e.low {
		c.lowHits.Add(1)
	}
	return e.val, true
}

// val must not be mutated after Put — the cache aliases it.
func (c *ByteCache) Put(key string, val []byte, generation uint64) {
	s := c.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != c.Generation.Load() {
		return
	}
	if _, ok := s.items[key]; ok {
		return // immutable: same key always means same bytes; first write wins
	}
	if len(val) > s.cap {
		return // would evict the entire stripe for one value; not worth it
	}
	e := &entry{key: key, val: val, low: c.isLow(key)}
	if e.low && len(val) > s.lowFloor && s.lists[0].size+len(val) > s.cap {
		// Past the floor and past what regular entries leave free, it would
		// evict every other low entry of the stripe and then itself.
		return
	}
	s.items[key] = e
	s.pushFront(e)
	s.charge(e, len(val))
	s.evictLocked()
}

// evictLocked drops least-recently-used entries until the stripe is back
// under cap: low-priority ones while they hold more than their floor (or
// nothing else is left), regular ones otherwise. size counts BOTH the
// bytes and whatever decode rides on them, so a decode that dwarfs its
// object (decompressed vectors can be 4x the bytes they came from) is
// inside the budget rather than beside it. Stops at one entry: an
// entry that alone exceeds cap has to survive — Put and PutDecoded both
// refuse to admit one that big, so this is a belt.
func (s *cacheStripe) evictLocked() {
	for s.size > s.cap && len(s.items) > 1 {
		victim := s.lists[0].tail
		if low := s.lists[1].tail; low != nil && (s.lists[1].size > s.lowFloor || victim == nil) {
			victim = low
		}
		s.remove(victim)
	}
}

// charge adds n bytes to e's stripe and priority list.
func (s *cacheStripe) charge(e *entry, n int) {
	s.size += n
	s.list(e).size += n
}

func (s *cacheStripe) list(e *entry) *lru {
	if e.low {
		return &s.lists[1]
	}
	return &s.lists[0]
}

func (s *cacheStripe) remove(e *entry) {
	s.unlink(e)
	delete(s.items, e.key)
	s.charge(e, -(len(e.val) + e.dsize))
}

func (s *cacheStripe) unlink(e *entry) {
	l := s.list(e)
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (s *cacheStripe) pushFront(e *entry) {
	l := s.list(e)
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	} else {
		l.tail = e
	}
	l.head = e
}

func (s *cacheStripe) touch(e *entry) {
	if s.list(e).head != e {
		s.unlink(e)
		s.pushFront(e)
	}
}

// PutValue caches a typed value under key with no bytes of its own: a
// derived layout (a decoded view over several cached blocks) whose backing
// bytes are other entries. size is its retained charge. First
// write wins, as for Put; a value larger than the stripe is not admitted.
func (c *ByteCache) PutValue(key string, v any, size int, generation uint64) {
	if size <= 0 || v == nil {
		return
	}
	s := c.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != c.Generation.Load() {
		return
	}
	if _, ok := s.items[key]; ok {
		return
	}
	if size > s.cap {
		return
	}
	e := &entry{key: key, low: c.isLow(key), decoded: concreteDecoded{value: v, bytes: size}, dsize: size}
	s.items[key] = e
	s.pushFront(e)
	s.charge(e, size)
	s.evictLocked()
}

// PutDecoded attaches a typed decode of an already-cached key. No-op if the
// key is not in the cache — decoded values must not outlive their bytes.
//
// A zero size charges v.CacheBytes() when v is a Sizer; any other value
// with a zero size, and any oversized decode, is not retained. Positive
// sizes are explicit charges, for derived layouts that share
// already-accounted backing bytes.
func (c *ByteCache) PutDecoded(key string, v any, size int, generation uint64) {
	if generation != c.Generation.Load() {
		return
	}
	if size < 0 {
		return
	}
	s := c.stripe(key)
	if size == 0 {
		sizer, ok := v.(Sizer)
		if !ok || sizer == nil {
			return
		}
		if size = sizer.CacheBytes(); size <= 0 || size > s.cap {
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != c.Generation.Load() {
		return
	}
	e, ok := s.items[key]
	if !ok {
		return
	}
	if size > s.cap-len(e.val) {
		return
	}
	s.charge(e, size-e.dsize) // a re-decode replaces the charge, never doubles it
	e.decoded, e.dsize = concreteDecoded{value: v, bytes: size}, size
	s.evictLocked()
}

// Recharge sets the charge of key's decode to size when that decode is v
// itself (v is a pointer: identity, not equality), and reports whether it
// did. A holder of a value that lost a publication race, was evicted or
// predates the generation is refused, so it cannot re-charge another
// value's entry. A charge that no longer fits the stripe drops the entry.
func (c *ByteCache) Recharge(key string, v any, size int, generation uint64) bool {
	if size <= 0 || v == nil {
		return false
	}
	s := c.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != c.Generation.Load() {
		return false
	}
	e, ok := s.items[key]
	if !ok || e.decoded == nil || e.decoded.decodedValue() != v {
		return false
	}
	if size > s.cap-len(e.val) {
		s.remove(e)
		return false
	}
	s.charge(e, size-e.dsize)
	e.decoded, e.dsize = concreteDecoded{value: v, bytes: size}, size
	s.evictLocked()
	return true
}

func (c *ByteCache) InvalidateNamespace(name string) {
	// Bump first: a Put racing this sweep either lands before it and is
	// swept, or sees the new generation and is refused.
	c.Generation.Add(1)
	c.sweepNamespace(name)
}

func (c *ByteCache) sweepNamespace(name string) {
	raw := "ns/" + name + "/"
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		for key, e := range s.items {
			if strings.HasPrefix(key, raw) {
				s.remove(e)
			}
		}
		s.mu.Unlock()
	}
}

// Decoded returns a previously PutDecoded value of type T for key.
func Decoded[T any](c *ByteCache, key string) (T, bool) {
	s := c.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	var zero T
	e, ok := s.items[key]
	if !ok {
		return zero, false
	}
	v := e.decoded
	if v == nil {
		return zero, false
	}
	t, ok := v.decodedValue().(T)
	if ok {
		s.touch(e)
	}
	return t, ok
}

// Missed records a logical miss of key, e.g. a value the range reader loads.
func (c *ByteCache) Missed(key string) {
	c.misses.Add(1)
	if c.isLow(key) {
		c.lowMisses.Add(1)
	}
}

// Hit records a logical hit of key after a non-counting Peek.
func (c *ByteCache) Hit(key string) {
	c.hits.Add(1)
	if c.isLow(key) {
		c.lowHits.Add(1)
	}
}

// Stats returns cumulative hits and misses (observability + test hook).
func (c *ByteCache) Stats() (hits, misses int) {
	return int(c.hits.Load()), int(c.misses.Load())
}

// LowStats is Stats for the low-priority keys alone.
func (c *ByteCache) LowStats() (hits, misses int) {
	return int(c.lowHits.Load()), int(c.lowMisses.Load())
}

// Charge is the retained size summed over every stripe.
func (c *ByteCache) Charge() int {
	total := 0
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		total += s.size
		s.mu.Unlock()
	}
	return total
}

// LowCharge is the retained size of the low-priority entries.
func (c *ByteCache) LowCharge() int {
	total := 0
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		total += s.lists[1].size
		s.mu.Unlock()
	}
	return total
}

// Misses is the cumulative miss counter. Tests poll it to observe a follower
// joining a singleflight.
func (c *ByteCache) Misses() int64 { return c.misses.Load() }

// DecodedSize is the charged retained size of key's decode, if any.
func (c *ByteCache) DecodedSize(key string) (int, bool) {
	s := c.stripe(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[key]
	if !ok {
		return 0, false
	}
	return e.dsize, true
}
