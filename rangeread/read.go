package rangeread

import (
	"context"
	"fmt"

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
	inFlight := semaphore.NewWeighted(cfg.MaxInFlightBytes)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)
	for _, p := range plan {
		g.Go(func() error {
			if err := inFlight.Acquire(gctx, p.Length); err != nil {
				return err
			}
			defer inFlight.Release(p.Length)
			return ReadPlanned(gctx, s, p, extents, out)
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
	data, err := s.GetRange(ctx, p.Object, p.Offset, p.Length)
	if err != nil {
		return err
	}
	if int64(len(data)) != p.Length {
		return fmt.Errorf("%w: %s: range at %d returned %d bytes, want %d", ErrCorrupt, p.Object, p.Offset, len(data), p.Length)
	}
	for i, x := range extents {
		lo, hi := max(p.Offset, x.Offset), min(p.Offset+p.Length, x.Offset+x.Length)
		if x.Object == p.Object && lo < hi {
			copy(out[i][lo-x.Offset:hi-x.Offset], data[lo-p.Offset:hi-p.Offset])
		}
	}
	return nil
}
