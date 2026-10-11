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
	byObject := make(map[string][]int) // object → indexes into extents
	for i, x := range extents {
		out[i] = make([]byte, x.Length)
		byObject[x.Object] = append(byObject[x.Object], i)
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
			data, err := s.GetRange(gctx, p.Object, p.Offset, p.Length)
			if err != nil {
				return err
			}
			if int64(len(data)) != p.Length {
				return fmt.Errorf("%w: %s: range at %d returned %d bytes, want %d", ErrCorrupt, p.Object, p.Offset, len(data), p.Length)
			}
			// Planned ranges are disjoint, so concurrent copies into one
			// extent never touch the same bytes.
			for _, i := range byObject[p.Object] {
				x := extents[i]
				lo, hi := max(p.Offset, x.Offset), min(p.Offset+p.Length, x.Offset+x.Length)
				if lo < hi {
					copy(out[i][lo-x.Offset:hi-x.Offset], data[lo-p.Offset:hi-p.Offset])
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
