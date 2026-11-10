package rangeread

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/axiomhq/objstore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Read returns the bytes of each extent, in order, read with the ranged
// GETs Plan chooses for them. Each result is its own buffer, so keeping
// one does not keep a coalesced range alive. A range the store returns at
// the wrong length is ErrCorrupt; an invalid extent is ErrInvalidExtent.
func Read(ctx context.Context, s *objstore.Store, extents []Extent, cfg Config) ([][]byte, error) {
	cfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	plan, err := Plan(extents, cfg)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(extents))
	for i, x := range extents {
		out[i] = make([]byte, x.Length)
	}
	idx := newExtentIndex(extents)
	inFlight := semaphore.NewWeighted(cfg.MaxInFlightBytes)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)
	for _, p := range plan {
		g.Go(func() error {
			if err := inFlight.Acquire(gctx, p.Length); err != nil {
				return err
			}
			defer inFlight.Release(p.Length)
			return readPlanned(gctx, s, p, extents, out, idx.overlapping(p))
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadPlanned reads one planned span with a single GetRange and copies the
// part of it each extent covers into out[i], which must be extents[i].Length
// bytes long; extents p does not overlap are left alone. An extent
// longer than one span is filled by the ReadPlanned calls of each span it
// overlaps; spans of one Plan are disjoint, so those calls may run at once.
// A range the store returns at the wrong length is ErrCorrupt.
func ReadPlanned(ctx context.Context, s *objstore.Store, p Planned, extents []Extent, out [][]byte) error {
	all := make([]int, len(extents))
	for i := range all {
		all[i] = i
	}
	return readPlanned(ctx, s, p, extents, out, all)
}

// readPlanned is ReadPlanned over only the extents named by cand.
func readPlanned(ctx context.Context, s *objstore.Store, p Planned, extents []Extent, out [][]byte, cand []int) error {
	data, err := s.GetRange(ctx, p.Object, p.Offset, p.Length)
	if err != nil {
		return err
	}
	if int64(len(data)) != p.Length {
		return fmt.Errorf("%w: %s: range at %d returned %d bytes, want %d", ErrCorrupt, p.Object, p.Offset, len(data), p.Length)
	}
	for _, i := range cand {
		x := extents[i]
		lo, hi := max(p.Offset, x.Offset), min(p.Offset+p.Length, x.Offset+x.Length)
		if x.Object == p.Object && lo < hi {
			copy(out[i][lo-x.Offset:hi-x.Offset], data[lo-p.Offset:hi-p.Offset])
		}
	}
	return nil
}

// extentIndex finds the extents a span can overlap without scanning all of
// them: order sorts extent indices by object then offset, and reach[k] is
// the furthest end among order[:k+1] of the same object, so it never
// decreases within an object.
type extentIndex struct {
	extents []Extent
	order   []int
	reach   []int64
}

func newExtentIndex(extents []Extent) *extentIndex {
	order := make([]int, len(extents))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return compareExtents(extents[a], extents[b]) })
	reach := make([]int64, len(order))
	for k, i := range order {
		x := extents[i]
		reach[k] = x.Offset + x.Length
		if k > 0 && extents[order[k-1]].Object == x.Object {
			reach[k] = max(reach[k], reach[k-1])
		}
	}
	return &extentIndex{extents, order, reach}
}

// overlapping returns the extent indices that may overlap p: a superset
// of the overlapping ones, which readPlanned filters exactly.
func (x *extentIndex) overlapping(p Planned) []int {
	n := len(x.order)
	first := sort.Search(n, func(k int) bool { return x.extents[x.order[k]].Object >= p.Object })
	end := sort.Search(n, func(k int) bool { return x.extents[x.order[k]].Object > p.Object })
	lo := first + sort.Search(end-first, func(k int) bool { return x.reach[first+k] > p.Offset })
	hi := first + sort.Search(end-first, func(k int) bool { return x.extents[x.order[first+k]].Offset >= p.Offset+p.Length })
	if lo >= hi {
		return nil
	}
	return x.order[lo:hi]
}
