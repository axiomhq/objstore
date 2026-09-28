package cache

import (
	"strings"
	"sync"
)

// Resident holds decoded log pages under a byte budget without recency
// eviction: a page stays until the log is truncated past it (DropThrough)
// or its namespace is invalidated, and a page that does not fit is
// refused. It serves a log suffix every read scans in sequence order,
// where under LRU a suffix one page larger than the budget misses on every
// page. Refusing instead keeps a stable resident prefix; the rest streams
// per read.
//
// Entries are grouped by log prefix and carry their page's sequence.
//
// First come, first kept across namespaces; there are no per-namespace
// shares.
type Resident struct {
	Cap  int
	gens generations // per namespace; invalidation retires in-flight publications

	mu     sync.Mutex
	size   int
	items  map[string]residentEntry
	floors map[string]uint64 // per prefix: pages at or below are truncated
}

type residentEntry struct {
	value  any
	prefix string
	seq    uint64
	size   int
}

// NewResident returns a budget of capBytes; zero or less holds nothing.
func NewResident(capBytes int) *Resident {
	return &Resident{Cap: max(capBytes, 0), items: make(map[string]residentEntry), floors: make(map[string]uint64)}
}

// Get returns the value stored under key.
func (r *Resident) Get(key string) (any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[key]
	return e.value, ok
}

// GenerationOf is key's namespace generation. Read it before loading key
// and pass it to Put: an InvalidateNamespace of key's namespace in between
// makes the publication a no-op. Other namespaces' invalidations do not.
func (r *Resident) GenerationOf(key string) uint64 { return r.gens.of(key) }

// Put stores the page seq of prefix under key unless the budget is full,
// the page is already truncated, or key's namespace was invalidated since
// generation (GenerationOf) was read. It reports whether value is now
// resident.
func (r *Resident) Put(prefix string, seq uint64, key string, value any, size int, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if generation != r.gens.of(key) || seq <= r.floors[prefix] {
		return false
	}
	if _, ok := r.items[key]; ok {
		return true
	}
	if size < 0 || size > r.Cap-r.size {
		return false
	}
	r.items[key] = residentEntry{value: value, prefix: prefix, seq: seq, size: size}
	r.size += size
	return true
}

// DropThrough releases the pages of prefix at or below seq and refuses
// them from now on.
func (r *Resident) DropThrough(prefix string, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if seq <= r.floors[prefix] {
		return
	}
	r.floors[prefix] = seq
	for key, e := range r.items {
		if e.prefix == prefix && e.seq <= seq {
			delete(r.items, key)
			r.size -= e.size
		}
	}
}

// InvalidateNamespace drops the namespace's pages and truncation floors.
func (r *Resident) InvalidateNamespace(name string) {
	r.gens.bump(name)
	ns := "ns/" + name + "/"
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, e := range r.items {
		if strings.HasPrefix(key, ns) {
			delete(r.items, key)
			r.size -= e.size
		}
	}
	for prefix := range r.floors {
		if strings.HasPrefix(prefix, ns) {
			delete(r.floors, prefix)
		}
	}
}

// Room is the bytes still free.
func (r *Resident) Room() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Cap - r.size
}

// Charge is the bytes resident.
func (r *Resident) Charge() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}
