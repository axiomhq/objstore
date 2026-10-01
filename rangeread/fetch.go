package rangeread

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/axiomhq/objstore/cache"
	"golang.org/x/sync/errgroup"
)

// Load associates one physical extent with a logical decoded cache entry.
type Load struct {
	Extent
	// Key is the logical cache key of the decoded child, never empty. It
	// must identify the extent and its decoding: two loads with one Key
	// must agree on Extent, DecodedBytes and Transient (else
	// ErrInvalidExtent), only the first is read and decoded (its Decode
	// wins; functions cannot be compared), and a cached entry under Key is
	// served without reading at all.
	Key string
	// Decode turns the stored bytes into the child; nil keeps them as
	// stored. It must validate any stored checksum, since its result is
	// cached. It runs for every child read from the store, transient or
	// not, and never for one served from a cache tier or the scope.
	Decode       func([]byte) ([]byte, error)
	DecodedBytes int64 // maximum decoded child bytes, in addition to stored assembly
	// Transient reads are served from the returned scope only and never
	// cached: row-sized slices of large blocks, which by the
	// thousand a query would cost the cache more in entries than in bytes.
	Transient bool
}

// FetchRanges executes one dependency stage: it reads every load not
// already cached, coalesced by Plan, and returns ctx carrying all of the
// stage's children (cache.WithResults; read them with cache.Scoped). It
// keeps exact children in owned buffers, so a tiny cached child cannot
// retain a large merged range. The returned scope supplies this query's
// results even if the LRU is smaller than its selected working set; it
// is replaced only when a later FetchRanges derives directly from the
// returned ctx. Wrapping that ctx retains the old scope's buffers;
// per-stage wrappers should derive from the unscoped base context instead.
//
// A stage whose children would retain more than Config.MaxInFlightBytes is
// skipped: FetchRanges returns ctx unchanged and nil, and the consumer
// falls back to its own per-object reads. An invalid load, an empty Key, or
// one Key given two different loads is ErrInvalidExtent, whatever the
// budget; a range the store returns at the wrong length is ErrCorrupt.
func (r *Reader) FetchRanges(ctx context.Context, loads []Load) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	if len(loads) == 0 {
		return ctx, nil
	}
	// A prefetch scope is optional. Never materialize a corpus-sized set of
	// children just because physical reads can be scheduled in small chunks.
	// Count stored assembly + decoded children + key/map overhead, including
	// cache hits that the scope would keep alive after an LRU eviction. The
	// consumer falls back to its normal bounded per-object reads.
	//
	// Validate and dedupe every load before the budget pass, so an invalid
	// stage is an error whatever its size or order.
	seen := make(map[string]int, len(loads)) // Key -> index in unique
	var unique []Load                        // loads, first of each Key
	for _, load := range loads {
		if err := load.valid(); err != nil {
			return ctx, err
		}
		if load.DecodedBytes < 0 {
			return ctx, fmt.Errorf("%w: %s: negative DecodedBytes %d", ErrInvalidExtent, load.Key, load.DecodedBytes)
		}
		if load.Key == "" {
			return ctx, fmt.Errorf("%w: %s: empty Key", ErrInvalidExtent, load.Object)
		}
		if i, ok := seen[load.Key]; ok {
			if u := unique[i]; u.Extent != load.Extent || u.DecodedBytes != load.DecodedBytes || u.Transient != load.Transient {
				return ctx, fmt.Errorf("%w: Key %s names two different loads", ErrInvalidExtent, load.Key)
			}
			continue
		}
		seen[load.Key] = len(unique)
		unique = append(unique, load)
	}
	var retained int64
	for _, load := range unique {
		for _, n := range []int64{load.Length, load.DecodedBytes, int64(len(load.Key)+len(load.Object)) + 128} {
			if n > r.config.MaxInFlightBytes-retained {
				return ctx, nil
			}
			retained += n
		}
	}
	var gens []uint64                // gens[i]: pending[i]'s namespace generation, read before its I/O
	var parentGens map[string]uint64 // physical objects' generations, read before planning
	results := make(map[string][]byte, len(unique))
	var pending []Load
	var owners []bool // owners[i]: this lookup owns the request's miss of pending[i] (cache.MarkMissed)
	var extents []Extent
	for _, load := range unique {
		if b, ok := cache.Scoped(ctx, load.Key); ok {
			results[load.Key] = b
			continue
		}
		memory := r.objects.ByteCacheFor(load.Key)
		if b, ok := memory.Peek(load.Key); ok {
			r.objects.Note(ctx, load.Key, cache.MemoryHit)
			results[load.Key] = b
			continue
		}
		if b, ok := r.objects.FromDisk(ctx, load.Key); ok {
			results[load.Key] = b
			continue
		}
		pending = append(pending, load)
		gens = append(gens, memory.GenerationOf(load.Key))
		if parentGens == nil {
			parentGens = make(map[string]uint64)
		}
		if _, ok := parentGens[load.Object]; !ok {
			parentGens[load.Object] = r.objects.ByteCacheFor(load.Object).GenerationOf(load.Object)
		}
		owners = append(owners, cache.MarkMissed(ctx, load.Key))
		extents = append(extents, load.Extent)
	}
	plans, err := Plan(extents, r.config)
	if err != nil {
		return ctx, err
	}
	// children[i]: the pending loads plan i overlaps. Plans come back
	// sorted and disjoint; walk them against the loads in the same order,
	// so matching is linear in plans plus overlaps.
	order := make([]int, len(pending))
	for j := range order {
		order[j] = j
	}
	slices.SortFunc(order, func(a, b int) int { return compareExtents(pending[a].Extent, pending[b].Extent) })
	children := make([][]int, len(plans))
	p := 0
	for _, j := range order {
		load := pending[j]
		for p < len(plans) && (plans[p].Object < load.Object || plans[p].Object == load.Object && plans[p].Offset+plans[p].Length <= load.Offset) {
			p++
		}
		for q := p; q < len(plans) && plans[q].Object == load.Object && plans[q].Offset < load.Offset+load.Length; q++ {
			children[q] = append(children[q], j)
		}
	}
	// A plan that is exactly one cacheable child is read through the cache
	// under the child's key (FetchCachedRange decodes and publishes it
	// there). Everything else, a merged plan or a transient child, is
	// assembled into owned buffers and decoded below.
	direct := make([]bool, len(pending))
	planChild := make([]int, len(plans))
	for i, plan := range plans {
		planChild[i] = -1
		if len(children[i]) != 1 {
			continue
		}
		j := children[i][0]
		if load := pending[j]; !load.Transient && plan.Extra == 0 && plan.Offset == load.Offset && plan.Length == load.Length {
			planChild[i], direct[j] = j, true
		}
	}
	// Every child not read directly gets an exact-size buffer up front,
	// counted by the retention check above (not by the memory semaphore,
	// which covers physical range buffers only). Each worker copies its
	// child bytes out while it still holds its range's reservation, so no
	// gap bytes outlive the read.
	assembled := make([][]byte, len(pending))
	for i, load := range pending {
		if !direct[i] {
			assembled[i] = make([]byte, load.Length)
		}
	}
	// outcome[j] is who answered child j: a direct child's
	// FetchCachedRange says, otherwise the worst of the ranges it was cut
	// from, Load if any came from the store, a memory hit if it joined
	// another query's read of the parent. stored[j]: some range it
	// was cut from came from the store; a child cut only from ranges a
	// cache tier held is already on disk inside its object, and is not
	// written there again.
	stored := make([]atomic.Bool, len(pending))
	outcome := make([]atomic.Int32, len(pending))
	for j := range outcome {
		outcome[j].Store(-1)
	}
	charge := func(j int, o cache.Outcome) {
		for {
			old := outcome[j].Load()
			if old >= int32(o) || outcome[j].CompareAndSwap(old, int32(o)) {
				return
			}
		}
	}
	var started atomic.Bool
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.config.Concurrency)
	for i, plan := range plans {
		child := planChild[i]
		g.Go(func() error {
			// read reports who answered: the store (Load), or the memory or
			// disk tier holding the object its bytes were cut from.
			read := func(ctx context.Context) ([]byte, cache.Outcome, error) {
				if b, fromDisk, ok := r.objects.CachedRange(plan.Object, plan.Offset, plan.Length); ok {
					cache.MarkServedFromCache(ctx, plan.Object, fromDisk)
					if fromDisk {
						return bytes.Clone(b), cache.DiskHit, nil
					}
					return bytes.Clone(b), cache.MemoryHit, nil
				}
				data, err := r.objects.Gated(ctx, func(ctx context.Context) ([]byte, error) {
					if started.CompareAndSwap(false, true) {
						r.IO.Waves.Add(1)
					}
					r.IO.Gets.Add(1)
					cache.NoteStoreLoad(ctx, plan.Object)
					data, err := r.store.GetRange(ctx, plan.Object, plan.Offset, plan.Length)
					r.IO.Bytes.Add(int64(len(data)))
					if err == nil && int64(len(data)) != plan.Length {
						err = fmt.Errorf("%w: %s: range at %d returned %d bytes, want %d", ErrCorrupt, plan.Object, plan.Offset, len(data), plan.Length)
					}
					if err == nil {
						r.IO.ExtraBytes.Add(plan.Extra)
					}
					return data, err
				})
				return data, cache.Load, err
			}
			var data []byte
			var src, counted cache.Outcome
			var release func()
			var err error
			if child >= 0 {
				load := pending[child]
				data, src, err = r.objects.FetchCachedRange(gctx, load.Key, int(plan.Length), func(ctx context.Context) ([]byte, error) {
					if err := r.memory.Acquire(ctx, plan.Length); err != nil {
						return nil, err
					}
					defer r.memory.Release(plan.Length)
					stored, _, err := read(ctx)
					if err != nil {
						return nil, err
					}
					// Copy under the producer's reservation. The cache and
					// scope own the child, never the physical read buffer.
					data := make([]byte, len(stored))
					copy(data, stored)
					if load.Decode == nil {
						return data, nil
					}
					return load.Decode(data)
				})
				if err == nil {
					charge(child, src)
				}
			} else {
				// A coalesced parent is not cached: its children are, and
				// retaining it beside them halves the room the children
				// have. Concurrent cold reads planning the
				// same parent still share its one GET.
				data, src, counted, release, err = r.sharedParent(gctx, plan.Extent, parentGens[plan.Object], read)
			}
			if err != nil {
				return err
			}
			if child >= 0 {
				assembled[child] = data
				return nil
			}
			defer release()
			for _, j := range children[i] {
				load := pending[j]
				lo, hi := max(plan.Offset, load.Offset), min(plan.Offset+plan.Length, load.Offset+load.Length)
				if lo < hi {
					copy(assembled[j][lo-load.Offset:hi-load.Offset], data[lo-plan.Offset:hi-plan.Offset])
					charge(j, counted)
					if src == cache.Load {
						stored[j].Store(true)
					}
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return ctx, err
	}
	// Decode serially: I/O has completed and the cache itself serializes
	// insertion. Copies prevent retention of unrelated coalesced bytes.
	// Every assembled child is decoded here, transient ones included; a
	// direct child was decoded by FetchCachedRange, which cached it.
	for i, load := range pending {
		if err := ctx.Err(); err != nil {
			return ctx, err
		}
		data := assembled[i]
		memory := r.objects.ByteCacheFor(load.Key)
		if load.Decode != nil && !direct[i] {
			data, err = load.Decode(data)
			if err != nil {
				return ctx, err
			}
		}
		if err := ctx.Err(); err != nil {
			return ctx, err // a decoder may have cancelled the stage
		}
		memory.Missed(load.Key)
		if o := outcome[i].Load(); o >= 0 {
			r.objects.NoteAs(ctx, load.Key, cache.Outcome(o), owners[i])
		}
		if !direct[i] && !load.Transient {
			if stored[i].Load() {
				r.objects.Disk.Put(cache.DiskKey(load.Key, gens[i]), data)
			}
			memory.Put(load.Key, data, gens[i])
		}
		results[load.Key] = data
	}
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	return cache.WithResults(ctx, results), nil
}

type rangeFlight struct {
	done     chan struct{}
	data     []byte
	src      cache.Outcome
	err      error
	length   int64
	refs     int // producer and consumers; guarded by flightMu
	reserved bool
}

// sharedParent runs read once for concurrent identical coalesced parents
// and hands every waiter the same bytes, which callers only copy out of,
// with who answered the read and the outcome this caller counts: its own
// read's, or a memory hit for a waiter on another's.
// Call release after copying: the producer and every consumer own the
// reservation together. The read runs under the leader's context, so its
// error may be the leader's own (a cancellation, a per-request budget):
// a follower that gets one retries once, as a fresh
// shared flight under its own context, rather than inherit it.
func (r *Reader) sharedParent(ctx context.Context, x Extent, generation uint64, read func(context.Context) ([]byte, cache.Outcome, error)) ([]byte, cache.Outcome, cache.Outcome, func(), error) {
	return r.sharedParentOnce(ctx, x, generation, read, false)
}

func (r *Reader) releaseFlightLocked(f *rangeFlight) {
	if f.refs--; f.refs == 0 {
		f.data = nil
		if f.reserved {
			r.memory.Release(f.length)
			f.reserved = false
		}
	}
}

func (r *Reader) sharedParentOnce(ctx context.Context, x Extent, generation uint64, read func(context.Context) ([]byte, cache.Outcome, error), retried bool) ([]byte, cache.Outcome, cache.Outcome, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, cache.Load, cache.Load, nil, err
	}
	key := strconv.FormatUint(generation, 10) + "\x00" + x.Object + "\x00" + strconv.FormatInt(x.Offset, 10) + "+" + strconv.FormatInt(x.Length, 10)
	r.flightMu.Lock()
	if r.flights == nil {
		r.flights = make(map[string]*rangeFlight)
	}
	f := r.flights[key]
	led := f == nil
	if led {
		f = &rangeFlight{done: make(chan struct{}), length: x.Length, refs: 1}
		r.flights[key] = f
	}
	f.refs++
	r.flightMu.Unlock()
	leave := func() {
		r.flightMu.Lock()
		defer r.flightMu.Unlock()
		r.releaseFlightLocked(f)
	}
	if led {
		go r.produceRange(ctx, key, f, read)
	}
	if r.joined != nil {
		r.joined() // the flight is registered: a test's rendezvous
	}
	select {
	case <-f.done:
		if f.err != nil {
			err := f.err
			leave() // release the failed flight before a retry can reserve bytes
			if !led && !retried && ctx.Err() == nil {
				return r.sharedParentOnce(ctx, x, generation, read, true)
			}
			return nil, cache.Load, cache.Load, nil, err
		}
		counted := f.src
		if !led {
			counted = cache.MemoryHit
		}
		return f.data, f.src, counted, leave, nil
	case <-ctx.Done():
		leave()
		return nil, cache.Load, cache.Load, nil, ctx.Err()
	}
}

func (r *Reader) produceRange(ctx context.Context, key string, f *rangeFlight, read func(context.Context) ([]byte, cache.Outcome, error)) {
	var data []byte
	var src cache.Outcome
	var err error
	defer func() {
		r.flightMu.Lock()
		defer r.flightMu.Unlock()
		f.data, f.src, f.err = data, src, err
		if r.flights[key] == f {
			delete(r.flights, key)
		}
		close(f.done)
		r.releaseFlightLocked(f)
	}()
	func() {
		defer func() {
			if p := recover(); p != nil {
				data, err = nil, fmt.Errorf("rangeread: parent %s: panic: %v\n%s", key, p, debug.Stack())
			}
		}()
		if err = r.memory.Acquire(ctx, f.length); err != nil {
			return
		}
		r.flightMu.Lock()
		f.reserved = true
		r.flightMu.Unlock()
		data, src, err = read(ctx)
	}()
}
