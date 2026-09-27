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
	// Key is the logical cache key of the decoded child. It must identify
	// the extent and its decoding: two loads with one Key are the same
	// child, only the first is read, and a cached entry under Key is
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
// dies with the query stage, and a later FetchRanges on the returned ctx
// replaces it rather than adding to it.
//
// A stage whose children would retain more than Config.MaxInFlightBytes is
// skipped: FetchRanges returns ctx unchanged and nil, and the consumer
// falls back to its own per-object reads. An invalid load is
// ErrInvalidExtent; a short read from the store is ErrCorrupt.
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
	var retained int64
	seen := make(map[string]bool)
	for _, load := range loads {
		if err := load.valid(); err != nil {
			return ctx, err
		}
		if load.DecodedBytes < 0 {
			return ctx, fmt.Errorf("%w: %s: negative DecodedBytes %d", ErrInvalidExtent, load.Key, load.DecodedBytes)
		}
		if seen[load.Key] {
			continue
		}
		seen[load.Key] = true
		for _, n := range []int64{load.Length, load.DecodedBytes, int64(len(load.Key)+len(load.Object)) + 128} {
			if n > r.Config.MaxInFlightBytes-retained {
				return ctx, nil
			}
			retained += n
		}
	}
	var gens []uint64 // gens[i]: pending[i]'s namespace generation, read before its I/O
	results := make(map[string][]byte, len(loads))
	unique := make(map[string]bool, len(loads))
	var pending []Load
	var owners []bool // owners[i]: this lookup owns the request's miss of pending[i] (cache.MarkMissed)
	var extents []Extent
	for _, load := range loads {
		if unique[load.Key] {
			continue
		}
		unique[load.Key] = true
		if b, ok := cache.Scoped(ctx, load.Key); ok {
			results[load.Key] = b
			continue
		}
		memory := r.Objects.ByteCacheFor(load.Key)
		if b, ok := memory.Peek(load.Key); ok {
			r.Objects.Note(ctx, load.Key, cache.MemoryHit)
			results[load.Key] = b
			continue
		}
		if b, ok := r.Objects.FromDisk(ctx, load.Key); ok {
			results[load.Key] = b
			continue
		}
		pending = append(pending, load)
		gens = append(gens, memory.GenerationOf(load.Key))
		owners = append(owners, cache.MarkMissed(ctx, load.Key))
		extents = append(extents, load.Extent)
	}
	plans, err := Plan(extents, r.Config)
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
	// Assemble exact children while each physical buffer holds its memory
	// reservation. Gaps never remain in an unbounded per-wave buffer list.
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
	g.SetLimit(r.Config.Concurrency)
	for i, plan := range plans {
		child := planChild[i]
		g.Go(func() error {
			if err := r.Memory.Acquire(gctx, plan.Length); err != nil {
				return err
			}
			defer r.Memory.Release(plan.Length)
			// read reports who answered: the store (Load), or the memory or
			// disk tier holding the object its bytes were cut from.
			read := func(ctx context.Context) ([]byte, cache.Outcome, error) {
				if b, fromDisk, ok := r.Objects.CachedRange(plan.Object, plan.Offset, plan.Length); ok {
					cache.MarkServedFromCache(ctx, plan.Object, fromDisk)
					if fromDisk {
						return bytes.Clone(b), cache.DiskHit, nil
					}
					return bytes.Clone(b), cache.MemoryHit, nil
				}
				data, err := r.Objects.Gated(ctx, func(ctx context.Context) ([]byte, error) {
					if started.CompareAndSwap(false, true) {
						r.IO.Waves.Add(1)
					}
					r.IO.Gets.Add(1)
					cache.NoteStoreLoad(ctx, plan.Object)
					data, err := r.Store.GetRange(ctx, plan.Object, plan.Offset, plan.Length)
					r.IO.Bytes.Add(int64(len(data)))
					if err == nil && int64(len(data)) != plan.Length {
						err = fmt.Errorf("%w: short range: got %d, want %d", ErrCorrupt, len(data), plan.Length)
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
			var err error
			if child >= 0 {
				load := pending[child]
				data, src, err = r.Objects.FetchCachedRange(gctx, load.Key, int(plan.Length), func(ctx context.Context) ([]byte, error) {
					stored, _, err := read(ctx)
					if err != nil || load.Decode == nil {
						return stored, err
					}
					return load.Decode(stored)
				})
				if err == nil {
					charge(child, src)
				}
			} else {
				// A coalesced parent is not cached: its children are, and
				// retaining it beside them halves the room the children
				// have. Concurrent cold reads planning the
				// same parent still share its one GET.
				data, src, counted, err = r.sharedParent(gctx, plan.Extent, read)
			}
			if err != nil {
				return err
			}
			if child >= 0 {
				assembled[child] = data
				return nil
			}
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
		data := assembled[i]
		memory := r.Objects.ByteCacheFor(load.Key)
		if load.Decode != nil && !direct[i] {
			data, err = load.Decode(data)
			if err != nil {
				return ctx, err
			}
		}
		memory.Missed(load.Key)
		if o := outcome[i].Load(); o >= 0 {
			r.Objects.NoteAs(ctx, load.Key, cache.Outcome(o), owners[i])
		}
		if !direct[i] && !load.Transient {
			if stored[i].Load() {
				r.Objects.Disk.Put(cache.DiskKey(load.Key, gens[i]), data)
			}
			memory.Put(load.Key, data, gens[i])
		}
		results[load.Key] = data
	}
	return cache.WithResults(ctx, results), nil
}

type parentRead struct {
	data []byte
	src  cache.Outcome
}

// sharedParent runs read once for concurrent identical coalesced parents
// and hands every waiter the same bytes, which callers only copy out of,
// with who answered the read and the outcome this caller counts: its own
// read's, or a memory hit for a waiter on another's.
// Nothing is retained after the last waiter. The read runs under the
// leader's context, so its error may be the leader's own (a cancellation, a
// per-request budget): a follower that gets one retries once, as a fresh
// shared flight under its own context, rather than inherit it.
func (r *Reader) sharedParent(ctx context.Context, x Extent, read func(context.Context) ([]byte, cache.Outcome, error)) (data []byte, src, counted cache.Outcome, err error) {
	return r.sharedParentOnce(ctx, x, read, false)
}

func (r *Reader) sharedParentOnce(ctx context.Context, x Extent, read func(context.Context) ([]byte, cache.Outcome, error), retried bool) (data []byte, src, counted cache.Outcome, err error) {
	key := x.Object + "\x00" + strconv.FormatInt(x.Offset, 10) + "+" + strconv.FormatInt(x.Length, 10)
	var led atomic.Bool
	ch := r.parents.DoChan(key, func() (v any, err error) {
		led.Store(true)
		// DoChan re-raises a panic on its own goroutine, where nothing can
		// recover it: every waiter gets it as an error instead.
		defer func() {
			if p := recover(); p != nil {
				v, err = nil, fmt.Errorf("rangeread: parent %s: panic: %v\n%s", key, p, debug.Stack())
			}
		}()
		data, src, err := read(ctx)
		return parentRead{data, src}, err
	})
	if r.joined != nil {
		r.joined() // the flight is registered: a test's rendezvous
	}
	select {
	case res := <-ch:
		if res.Err != nil {
			if !led.Load() && !retried && ctx.Err() == nil {
				return r.sharedParentOnce(ctx, x, read, true)
			}
			return nil, cache.Load, cache.Load, res.Err
		}
		got := res.Val.(parentRead)
		if !led.Load() {
			return got.data, got.src, cache.MemoryHit, nil
		}
		return got.data, got.src, got.src, nil
	case <-ctx.Done():
		return nil, cache.Load, cache.Load, ctx.Err()
	}
}
