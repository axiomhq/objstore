package rangeread

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
)

// Extent is Length bytes of Object starting at byte Offset. Object must
// not contain '#': logical cache keys derived from an object name use it
// as their separator (object#...), so such a name could collide with one.
type Extent struct {
	Object         string
	Offset, Length int64
}

// valid reports ErrInvalidExtent for an extent no store could serve.
func (x Extent) valid() error {
	if x.Object == "" || strings.Contains(x.Object, "#") || x.Offset < 0 || x.Length <= 0 || x.Offset > math.MaxInt64-x.Length {
		return fmt.Errorf("%w: %q at %d+%d", ErrInvalidExtent, x.Object, x.Offset, x.Length)
	}
	return nil
}

// compareExtents orders extents by object, then offset.
func compareExtents(a, b Extent) int {
	return cmp.Or(strings.Compare(a.Object, b.Object), cmp.Compare(a.Offset, b.Offset))
}

// Planned is one ranged GET of a plan: the extents it covers, merged, with
// Extra the bytes in gaps between them that no input extent asked for.
type Planned struct {
	Extent
	Extra int64 // bytes in gaps, not requested by any input extent
}

// Plan returns the ranged GETs that cover extents, sorted by object and
// offset. It unions overlaps before coalescing gaps. Cheaper gaps get first
// use of the wave-wide extra-byte budget, with object/offset order breaking
// ties. Keep the original greedy plan if range limits make it use fewer reads.
// Large required extents are split, never rejected or truncated to a budget.
// An invalid extent is ErrInvalidExtent.
func Plan(extents []Extent, cfg Config) ([]Planned, error) {
	cfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	spans := slices.Clone(extents)
	for _, r := range spans {
		if err := r.valid(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(spans, compareExtents)
	union := spans[:0]
	for _, r := range spans {
		if len(union) > 0 {
			last := &union[len(union)-1]
			if r.Object == last.Object && r.Offset <= last.Offset+last.Length {
				last.Length = max(last.Offset+last.Length, r.Offset+r.Length) - last.Offset
				continue
			}
		}
		union = append(union, r)
	}
	var out []Planned
	var pieces []Planned
	var extra int64
	for _, r := range union {
		for r.Length > 0 {
			piece := Extent{Object: r.Object, Offset: r.Offset, Length: min(r.Length, cfg.MaxRangeBytes)}
			pieces = append(pieces, Planned{Extent: piece})
			merged := false
			if len(out) > 0 {
				last := &out[len(out)-1]
				gap := piece.Offset - (last.Offset + last.Length)
				// Subtract from the limit instead of summing lengths: the
				// object may legitimately use offsets near MaxInt64.
				if piece.Object == last.Object && gap >= 0 && gap <= cfg.MaxGapBytes && gap <= cfg.MaxExtraBytes-extra &&
					gap <= cfg.MaxRangeBytes-last.Length && piece.Length <= cfg.MaxRangeBytes-last.Length-gap {
					last.Length += gap + piece.Length
					last.Extra += gap
					extra += gap
					merged = true
				}
			}
			if !merged {
				out = append(out, Planned{Extent: piece})
			}
			r.Offset += piece.Length
			r.Length -= piece.Length
		}
	}
	if len(out) < 2 {
		return out, nil
	}
	// Each boundary can save one request. Boundaries keep their original
	// gap cost even when either neighboring group absorbs other pieces.
	type gapBoundary struct {
		left int
		cost int64
	}
	var gaps []gapBoundary
	first, last := make([]int, len(pieces)), make([]int, len(pieces))
	for i, piece := range pieces {
		first[i], last[i] = i, i
		if i+1 < len(pieces) && piece.Object == pieces[i+1].Object {
			gap := pieces[i+1].Offset - (piece.Offset + piece.Length)
			if gap <= cfg.MaxGapBytes {
				gaps = append(gaps, gapBoundary{i, gap})
			}
		}
	}
	slices.SortStableFunc(gaps, func(a, b gapBoundary) int { return cmp.Compare(a.cost, b.cost) })
	extra = 0
	for _, gap := range gaps {
		if gap.cost > cfg.MaxExtraBytes-extra {
			break
		}
		// An unprocessed boundary separates two groups. Only their outside
		// endpoints need updating; no searches or per-merge allocations.
		left, right := first[gap.left], gap.left+1
		a, b := &pieces[left], pieces[right]
		if gap.cost > cfg.MaxRangeBytes-a.Length || b.Length > cfg.MaxRangeBytes-a.Length-gap.cost {
			continue
		}
		a.Length += gap.cost + b.Length
		a.Extra += gap.cost + b.Extra
		extra += gap.cost
		last[left] = last[right]
		first[last[right]] = left
	}
	cheap := pieces[:0]
	for i := 0; i < len(pieces); i = last[i] + 1 {
		cheap = append(cheap, pieces[i])
	}
	if len(cheap) >= len(out) {
		return out, nil
	}
	return cheap, nil
}
