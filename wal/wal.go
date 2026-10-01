package wal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/axiomhq/objstore"
)

// seqDigits is the width of a key's sequence: uint64 max has 20 digits.
const seqDigits = 20

// A Writer reserves a header per record within maxUnackedBytes. Four
// times that covers page bytes and decoded record descriptors alike.
const maxBatchBytes int64 = 4 * maxUnackedBytes

// splitBatch fills pages greedily: each adjacent pair's framed records
// exceed maxEntryBytes-entryHeaderReserve, even with an indivisible record.
var maxBatchPages = uint64(2*maxUnackedBytes/(maxEntryBytes-entryHeaderReserve) + 1)

// Key is the object key of page seq under prefix. The sequence is
// zero-padded to 20 digits (uint64 max), so lexical order is numeric order.
func Key(prefix string, seq uint64) string {
	var d [seqDigits]byte
	digits := strconv.AppendUint(d[:0], seq, 10)
	var b strings.Builder
	b.Grow(len(prefix) + seqDigits)
	b.WriteString(prefix)
	b.WriteString("00000000000000000000"[len(digits):])
	b.Write(digits)
	return b.String()
}

// SeqFromKey parses the sequence out of a key Key made, whatever its prefix:
// the key's last 20 characters.
func SeqFromKey(key string) (uint64, error) {
	if len(key) < seqDigits {
		return 0, fmt.Errorf("wal: %q is not a WAL key", key)
	}
	digits := key[len(key)-seqDigits:]
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, fmt.Errorf("wal: %q is not a WAL key", key)
		}
	}
	return strconv.ParseUint(digits, 10, 64)
}

// Entry is one committed entry as a walk sees it: a single page, or every
// page of a multi-page batch, coalesced.
type Entry[T any] struct {
	// Header is the entry's last page's. A marker (Pages nil) has its batch
	// fields zeroed.
	Header
	// Key is the last page's object key; its sequence is Header.Seq.
	Key string
	// Incomplete marks the physical end of a batch whose tail pages are
	// missing from an unbounded walk. A replacement writer starts after it,
	// but readers must not advance past it: the original writer may still
	// publish the rest.
	Incomplete bool
	// Pages holds each page's decode, in page order: one for a single-page
	// entry, BatchPages for a batch. Nil for a marker: a batch abandoned at
	// the end of the log (Incomplete), or cut by a bounded walk's end.
	Pages []T
}

// Walk visits committed entries with Seq in (after, through] in ascending
// order, reading and decoding ONE page at a time. through == 0 follows the
// live log to its first missing page; a bounded walk that meets a missing
// page inside (after, through] fails with ErrCorrupt. A batch beyond the
// Writer's page-count or decoded byte budget also fails with ErrCorrupt.
//
// No listing: the sequence numbers are the catalog. A miss at N proves the
// log ends there, because a writer claims sequentially and advances only on
// a proven outcome, so a crashed, fenced or unresolved writer leaves the
// log short, never holey.
func Walk[T any](ctx context.Context, s *objstore.Store, prefix string, after, through uint64, decode func([]byte) (Header, T, error), visit func(Entry[T]) error) error {
	return WalkWithGet(ctx, s.Get, prefix, after, through, decode, visit)
}

// WalkWithGet is Walk with the page fetch supplied by the caller, say a
// read through a cache. get must return an error wrapping
// objstore.ErrNotFound for a missing page.
func WalkWithGet[T any](ctx context.Context, get func(context.Context, string) ([]byte, error), prefix string, after, through uint64, decode func([]byte) (Header, T, error), visit func(Entry[T]) error) error {
	c := coalescer[T]{visit: visit}
	for seq := after + 1; seq > after && (through == 0 || seq <= through); seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		k := Key(prefix, seq)
		data, err := get(ctx, k)
		if errors.Is(err, objstore.ErrNotFound) {
			if through != 0 {
				return fmt.Errorf("%w: missing WAL entry %q: %w", ErrCorrupt, k, err)
			}
			return c.finish(true)
		}
		if err != nil {
			return fmt.Errorf("read wal entry %q: %w", k, err)
		}
		memory := pageMemoryBytes(data)
		h, body, err := decode(data)
		if err != nil {
			return fmt.Errorf("corrupt wal entry %q: %w", k, err)
		}
		if h.Seq != seq {
			return fmt.Errorf("%w: wal entry %q: sequence %d disagrees with key %d", ErrCorrupt, k, h.Seq, seq)
		}
		if err := c.add(h, k, body, memory); err != nil {
			return err
		}
	}
	return c.finish(false)
}

// coalescer is shared by the serial and parallel walkers. Only the caller
// owns it, so pages may decode out of order without changing the order in
// which entries and markers are visited.
type coalescer[T any] struct {
	batch []Entry[T] // the open batch's pages, one Pages element each
	bytes int64      // decoded budget held outside the parallel permit pool
	visit func(Entry[T]) error
}

func (c *coalescer[T]) add(h Header, key string, body T, bytes int64) error {
	if len(c.batch) != 0 && h.BatchIndex == 0 {
		// A new entry after a batch that never finished: a terminal failure
		// after some pages landed finished that writer (or it crashed), and
		// a replacement writer moved on past the partial batch.
		if err := c.visit(marker(c.batch[len(c.batch)-1], false)); err != nil {
			return err
		}
		c.batch, c.bytes = nil, 0
	}
	if h.BatchPages <= 1 { // 0 (Encode) and 1 (Writer) both mean a single page
		return c.visit(Entry[T]{Header: h, Key: key, Pages: []T{body}})
	}
	if h.BatchPages > maxBatchPages || bytes < 0 || bytes > maxBatchBytes-c.bytes {
		return fmt.Errorf("%w: wal entry %q: batch exceeds writer limits", ErrCorrupt, key)
	}
	if h.BatchIndex != uint64(len(c.batch)) || (len(c.batch) > 0 && (h.Nonce != c.batch[0].Nonce || h.BatchPages != c.batch[0].BatchPages)) {
		return fmt.Errorf("%w: wal entry %q: invalid batch header", ErrCorrupt, key)
	}
	c.bytes += bytes
	c.batch = append(c.batch, Entry[T]{Header: h, Key: key, Pages: []T{body}})
	if uint64(len(c.batch)) != h.BatchPages {
		return nil
	}
	pages := make([]T, len(c.batch))
	for i := range c.batch {
		pages[i] = c.batch[i].Pages[0]
	}
	last := c.batch[len(c.batch)-1]
	last.Pages = pages
	c.batch, c.bytes = nil, 0
	return c.visit(last)
}

func (c *coalescer[T]) finish(incomplete bool) error {
	if len(c.batch) == 0 {
		return nil
	}
	// Only a missing page in an unbounded walk marks a partial batch
	// Incomplete. A bounded range excludes it with a plain marker.
	return c.visit(marker(c.batch[len(c.batch)-1], incomplete))
}

func marker[T any](e Entry[T], incomplete bool) Entry[T] {
	e.BatchPages, e.BatchIndex, e.Pages = 0, 0, nil
	e.Incomplete = incomplete
	return e
}
