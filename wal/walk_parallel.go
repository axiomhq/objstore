package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
// Up to workers GETs are in flight, reading ahead of the visitor by at
// most 2*workers pages; an unbounded walk therefore spends up to that many
// not-found GETs past the end of the log. Each fetched page then waits, in
// sequence order, for a byte permit of four times its size (its decode's
// budget) from a 64 MiB pool, and keeps it until visited, so a slow
// consumer bounds retained pages by both slots and bytes. Pages waiting for
// a permit hold their raw bytes meanwhile; a page larger than the pool
// takes all of it and proceeds alone.
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
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	wg.Add(1 + workers)
	go func() {
		defer wg.Done()
		defer close(work)
		defer close(order)
		first := make(chan struct{})
		close(first)
		var turn <-chan struct{} = first
		for seq := after + 1; seq > after && (through == 0 || seq <= through); seq++ {
			var p *parallelPage[T]
			select {
			case p = <-free:
			case <-ctx.Done():
				return
			}
			*p = parallelPage[T]{seq: seq, key: Key(prefix, seq), turn: turn,
				permitted: make(chan struct{}), ready: make(chan struct{})}
			turn = p.permitted
			// Both are buffered to slots, and at most slots pages circulate.
			order <- p
			work <- p
		}
	}()
	for range workers {
		go func() {
			defer wg.Done()
			for p := range work {
				fetchPage(ctx, get, bytes, through, decode, prep, p)
				close(p.ready)
			}
		}()
	}

	batches := coalescer[T]{visit: visit}
	var result error
	for {
		if err := parentCtx.Err(); err != nil {
			return err
		}
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
		if err := parentCtx.Err(); err != nil {
			return err
		}
		if p.err != nil {
			return p.err
		}
		if p.eof {
			return batches.finish(true)
		}
		if result = batches.add(p.header, p.key, p.body); result != nil {
			return result
		}
		bytes.Release(p.bytes)
		*p = parallelPage[T]{}
		free <- p // never blocks: slots pages circulate through a slots-deep channel
	}
}

// fetchPage reads, permits and decodes p. Every path closes p.permitted
// once p's turn has come, so the next page never waits on a failed one.
func fetchPage[T any](ctx context.Context, get func(context.Context, string) ([]byte, error), bytes *semaphore.Weighted, through uint64,
	decode func([]byte) (Header, T, error), prep func(h Header, key string, body *T) error, p *parallelPage[T]) {
	data, err := get(ctx, p.key)
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
		// The permit is for the decoded page, which may be several times
		// its wire bytes.
		n := min(4*int64(len(data)), maxInFlightBytes)
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
