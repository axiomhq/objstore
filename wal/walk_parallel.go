package wal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/axiomhq/objstore"
	"golang.org/x/sync/semaphore"
)

const maxInFlightBytes int64 = 64 << 20

type parallelPage[T any] struct {
	seq    uint64
	key    string
	header Header
	body   T
	err    error
	eof    bool
	bytes  int64
	memory int64 // decoded budget, before capping the pool permit
	// gate closes when the page `workers` positions earlier holds its
	// byte permit (or has none to take): only then may this page's GET
	// start, so at most workers fetched pages wait for a permit.
	gate <-chan struct{}
	// turn closes when the previous page holds its byte permit (or has
	// none to take): permits are taken in sequence order.
	turn      <-chan struct{}
	permitted chan struct{}
	ready     chan struct{} // closed once the page is fetched and decoded
}

// WalkParallel is Walk with pages fetched, decoded and prepared on bounded
// workers, then coalesced and visited in sequence order. prep, if not nil,
// runs once per decoded page, before any batch coalescing, with the page's
// header and key. It must only mutate the supplied body and synchronize any
// state shared with visit.
//
// Page storage is bounded in three parts. A page of W wire bytes and N
// records is charged D = max(4W, 2W + N×S), where S is a []byte descriptor
// (24 bytes on 64-bit). This covers Decode's wire bytes, nonce copy and
// record descriptors. Custom decode and prep must keep their combined
// storage, including raw bytes and temporary allocations, within D; a
// non-WAL header gets only the 4W budget.
//
// Each fetched page takes its permit in sequence order from a 64 MiB pool.
// A page charged more than the pool takes all of it and decodes alone, so
// the pool holds at most max(64 MiB, largest D). On coalescing, the charge
// transfers to a separate 512 MiB batch budget before the permit is freed:
// even a batch larger than the pool can finish. A Writer's 128 MiB unacked
// bound and greedy page cuts bound a batch to that budget and at most nine
// pages; both walks reject larger batches with ErrCorrupt.
//
// A GET starts once the page `workers` positions earlier holds its permit,
// so at most workers fetched pages hold raw bytes outside the pool. With
// pages at most maxPageBytes (65 MiB) on the wire, retained page storage is
// at most max(64 MiB, largest D) + 512 MiB + workers × 65 MiB. This excludes
// bookkeeping, GET internals and storage a caller keeps after visit. A slow
// consumer holds the walk there. Up to workers GETs are in flight.
//
// An unbounded walk issues at most workers not-found GETs past the end of
// the log: a page skips its GET when the page `workers` positions earlier
// found the end.
func WalkParallel[T any](ctx context.Context, s *objstore.Store, prefix string, after, through uint64, workers int,
	decode func([]byte) (Header, T, error), prep func(h Header, key string, body *T) error, visit func(Entry[T]) error) error {
	return WalkParallelWithGet(ctx, s.Get, prefix, after, through, workers, decode, prep, visit)
}

// WalkParallelWithGet is WalkParallel with the page fetch supplied by the
// caller, as WalkWithGet is Walk's. get must be safe for concurrent use and
// return an error wrapping objstore.ErrNotFound for a missing page.
func WalkParallelWithGet[T any](ctx context.Context, get func(context.Context, string) ([]byte, error), prefix string, after, through uint64, workers int,
	decode func([]byte) (Header, T, error), prep func(h Header, key string, body *T) error, visit func(Entry[T]) error) error {
	parentCtx := ctx
	workers = min(max(workers, 1), 16)
	slots := 2 * workers
	free := make(chan *parallelPage[T], slots)
	for range slots {
		free <- &parallelPage[T]{}
	}
	work := make(chan *parallelPage[T], slots)
	order := make(chan *parallelPage[T], slots)
	bytes := semaphore.NewWeighted(maxInFlightBytes)
	// end is the lowest sequence known to end the walk (not found, or
	// failed); pages past it skip their GET. Set before the page's permitted
	// closes, so the page `workers` positions later, which waits on that, sees it.
	var end atomic.Uint64
	end.Store(math.MaxUint64)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	wg.Add(1 + workers)
	go func() {
		defer wg.Done()
		defer close(work)
		defer close(order)
		open := make(chan struct{})
		close(open)
		var turn <-chan struct{} = open
		gates := make([]<-chan struct{}, workers) // permitted of the last workers pages, by seq % workers
		for i := range gates {
			gates[i] = open
		}
		for seq := after + 1; seq > after && (through == 0 || seq <= through); seq++ {
			var p *parallelPage[T]
			select {
			case p = <-free:
			case <-ctx.Done():
				return
			}
			slot := (seq - after - 1) % uint64(workers)
			*p = parallelPage[T]{seq: seq, key: Key(prefix, seq), gate: gates[slot], turn: turn,
				permitted: make(chan struct{}), ready: make(chan struct{})}
			turn, gates[slot] = p.permitted, p.permitted
			// Both are buffered to slots, and at most slots pages circulate.
			order <- p
			work <- p
		}
	}()
	for range workers {
		go func() {
			defer wg.Done()
			for p := range work {
				fetchPage(ctx, get, bytes, &end, through, decode, prep, p)
				close(p.ready)
			}
		}()
	}

	batches := coalescer[T]{visit: visit}
	for {
		var p *parallelPage[T]
		select {
		case next, ok := <-order:
			if !ok {
				return batches.finish(false)
			}
			p = next
		case <-parentCtx.Done():
			return parentCtx.Err()
		}
		select {
		case <-p.ready:
		case <-parentCtx.Done():
			return parentCtx.Err()
		}
		// One check per page: a select with both ready picks at random.
		if err := parentCtx.Err(); err != nil {
			return err
		}
		if p.err != nil {
			return p.err
		}
		if p.eof {
			return batches.finish(true)
		}
		if err := batches.add(p.header, p.key, p.body, p.memory); err != nil {
			return err
		}
		bytes.Release(p.bytes)
		*p = parallelPage[T]{}
		free <- p // never blocks: slots pages circulate through a slots-deep channel
	}
}

// fetchPage waits for p's gate, then reads, permits and decodes p. Every
// path closes p.permitted once p's turn has come, so the next page never
// waits on a failed one.
func fetchPage[T any](ctx context.Context, get func(context.Context, string) ([]byte, error), bytes *semaphore.Weighted, end *atomic.Uint64, through uint64,
	decode func([]byte) (Header, T, error), prep func(h Header, key string, body *T) error, p *parallelPage[T]) {
	var data []byte
	var err error
	select {
	case <-p.gate:
		if p.seq > end.Load() {
			// The walk ends before p: it is never visited, so skip the GET.
			err = objstore.ErrNotFound
		} else {
			data, err = get(ctx, p.key)
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		lowerTo(end, p.seq)
	}
	select {
	case <-p.turn:
	case <-ctx.Done():
		p.err = ctx.Err()
		close(p.permitted)
		return
	}
	switch {
	case errors.Is(err, objstore.ErrNotFound):
		if through == 0 {
			p.eof = true
		} else {
			p.err = fmt.Errorf("%w: missing WAL entry %q: %w", ErrCorrupt, p.key, err)
		}
	case err != nil:
		p.err = fmt.Errorf("read wal entry %q: %w", p.key, err)
	default:
		// Count record descriptors before decode can allocate them.
		p.memory = pageMemoryBytes(data)
		n := min(p.memory, maxInFlightBytes)
		if err := bytes.Acquire(ctx, n); err != nil {
			p.err = err
		} else {
			p.bytes = n
		}
	}
	close(p.permitted)
	if p.err != nil || p.eof {
		return
	}
	h, body, err := decode(data)
	switch {
	case err != nil:
		p.err = fmt.Errorf("corrupt wal entry %q: %w", p.key, err)
	case h.Seq != p.seq:
		p.err = fmt.Errorf("%w: wal entry %q: sequence %d disagrees with key %d", ErrCorrupt, p.key, h.Seq, p.seq)
	default:
		if prep != nil {
			p.err = prep(h, p.key, &body)
		}
		p.header, p.body = h, body
	}
}

// lowerTo sets v to seq if seq is lower.
func lowerTo(v *atomic.Uint64, seq uint64) {
	for {
		cur := v.Load()
		if seq >= cur || v.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// headerReadBytes is one ranged read that holds a page header whose nonce
// is at most 32 bytes, what a Writer mints: magic, seven varints, nonce.
const headerReadBytes = 128

// WalkHeaders is WalkParallel over page headers only: each page costs one
// ranged GET of its first headerReadBytes (a whole GET when the page is
// shorter, or its header longer), so a walk's cost is its page count, not
// its bytes. Each visited Entry's Pages hold its pages' headers, Records
// and Weight included, for a caller that accounts a log without reading its
// records. A header is not checked against its page's CRC (DecodeHeader).
func WalkHeaders(ctx context.Context, s *objstore.Store, prefix string, after, through uint64, workers int, visit func(Entry[Header]) error) error {
	get := func(ctx context.Context, key string) ([]byte, error) {
		b, err := s.GetRange(ctx, key, 0, headerReadBytes)
		if errors.Is(err, objstore.ErrRange) {
			return s.Get(ctx, key)
		}
		if err == nil {
			if _, herr := DecodeHeader(b); herr != nil {
				return s.Get(ctx, key)
			}
		}
		return b, err
	}
	decode := func(b []byte) (Header, Header, error) {
		h, err := DecodeHeader(b)
		return h, h, err
	}
	return WalkParallelWithGet(ctx, get, prefix, after, through, workers, decode, nil, visit)
}
