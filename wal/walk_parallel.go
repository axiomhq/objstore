package wal

import (
	"context"
	"errors"
	"fmt"

	"github.com/axiomhq/objstore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const maxInFlightBytes int64 = 64 << 20

type parallelPage[T any] struct {
	seq    uint64
	key    string
	data   []byte
	header Header
	body   T
	err    error
	eof    bool
	bytes  int64
	ready  chan struct{}
}

// WalkParallel is Walk with the pages fetched in order, decoded and
// prepared on bounded workers, then coalesced and visited in sequence
// order. prep, if not nil, runs once per decoded page, before any batch
// coalescing, with the page's header and key. It must only mutate the
// supplied body and synchronize any state shared with visit.
//
// Exactly 2*workers page slots circulate through the pipeline. Decoded pages
// retain a byte permit until visited, so a slow consumer bounds retained
// pages by both slots and bytes. One fetch can transiently hold another page
// while waiting for its permit; a page larger than the byte limit takes the
// whole permit and proceeds alone.
func WalkParallel[T any](ctx context.Context, s *objstore.Store, prefix string, after, through uint64, workers int,
	decode func([]byte) (Header, T, error), prep func(h Header, key string, body *T) error, visit func(Entry[T]) error) error {
	parentCtx := ctx
	workers = min(max(workers, 1), 16)
	inflight := 2 * workers
	free := make(chan *parallelPage[T], inflight)
	for range inflight {
		free <- &parallelPage[T]{ready: make(chan struct{}, 1)}
	}
	work := make(chan *parallelPage[T], inflight)
	order := make(chan *parallelPage[T], inflight)
	bytes := semaphore.NewWeighted(maxInFlightBytes)
	g, gctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(gctx)
	defer cancel()

	g.Go(func() error {
		defer close(work)
		defer close(order)
		for seq := after + 1; seq > after && (through == 0 || seq <= through); seq++ {
			var p *parallelPage[T]
			select {
			case p = <-free:
			case <-ctx.Done():
				return ctx.Err()
			}
			p.seq, p.key = seq, Key(prefix, seq)
			var zero T
			p.header, p.body, p.err, p.eof = Header{}, zero, nil, false
			data, err := s.Get(ctx, p.key)
			if errors.Is(err, objstore.ErrNotFound) {
				if through == 0 {
					p.eof = true
				} else {
					p.err = fmt.Errorf("%w: missing WAL entry %q: %w", ErrCorrupt, p.key, err)
				}
			} else if err != nil {
				p.err = fmt.Errorf("read wal entry %q: %w", p.key, err)
			} else {
				// The permit is for the decoded page, which may be several
				// times its wire bytes.
				p.bytes = min(4*int64(len(data)), maxInFlightBytes)
				if err := bytes.Acquire(ctx, p.bytes); err != nil {
					return err
				}
				p.data = data
			}
			select {
			case order <- p:
			case <-ctx.Done():
				if p.bytes != 0 {
					bytes.Release(p.bytes)
				}
				return ctx.Err()
			}
			if p.err != nil || p.eof {
				close(p.ready)
				return nil
			}
			select {
			case work <- p:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	for range workers {
		g.Go(func() error {
			for {
				select {
				case p, ok := <-work:
					if !ok {
						return nil
					}
					h, body, err := decode(p.data)
					p.data = nil
					if err != nil {
						p.err = fmt.Errorf("corrupt wal entry %q: %w", p.key, err)
					} else if h.Seq != p.seq {
						p.err = fmt.Errorf("%w: wal entry %q: sequence %d disagrees with key %d", ErrCorrupt, p.key, h.Seq, p.seq)
					} else {
						if prep != nil {
							p.err = prep(h, p.key, &body)
						}
						p.header, p.body = h, body
					}
					close(p.ready)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	batches := coalescer[T]{visit: visit}
	var result error
loop:
	for {
		if err := parentCtx.Err(); err != nil {
			result = err
			break
		}
		var p *parallelPage[T]
		select {
		case next, ok := <-order:
			if !ok {
				result = batches.finish(false)
				break loop
			}
			p = next
		case <-ctx.Done():
			result = ctx.Err()
			break loop
		}
		select {
		case <-p.ready:
		case <-ctx.Done():
			result = ctx.Err()
			break loop
		}
		if err := parentCtx.Err(); err != nil {
			result = err
			break
		}
		if p.err != nil {
			result = p.err
			break
		}
		if p.eof {
			result = batches.finish(true)
			break
		}
		result = batches.add(p.header, p.key, p.body)
		if result != nil {
			break
		}
		bytes.Release(p.bytes)
		var zero T
		p.data, p.header, p.body, p.bytes = nil, Header{}, zero, 0
		p.ready = make(chan struct{}, 1)
		select {
		case free <- p:
		case <-ctx.Done():
			result = ctx.Err()
			break loop
		}
	}
	if result != nil {
		cancel()
	}
	groupErr := g.Wait()
	if err := parentCtx.Err(); err != nil {
		return err
	}
	if result != nil {
		return result
	}
	if groupErr != nil {
		return groupErr
	}
	return nil
}
