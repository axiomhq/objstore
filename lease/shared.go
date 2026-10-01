package lease

import (
	"context"
	"fmt"
	"sync"
)

// Shared is one lease shared by reference count among the holders inside
// one process: several concurrent operations on the same key take one lease
// between them, and the last Release hands it back. The zero value holds
// nothing.
//
// No store I/O runs under Shared's mutex: a Join that has to mint, and the
// last Ref.Release, mark s busy instead, and other Joins wait for them.
// Methods are callable from a fence callback installed after Join returns.
// Call Start only after Join returns, not inside mint: an early fence runs
// the callback on Start's goroutine, so a Join there waits on its own mint.
type Shared struct {
	mu   sync.Mutex
	busy chan struct{} // closed when a mint or the last release finishes; nil when idle
	held *Lease
	refs int
}

// Held reports whether a lease is currently held through s.
func (s *Shared) Held() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held != nil
}

// wait blocks until idle or ctx ends. Called and returns with s.mu held.
func (s *Shared) wait(ctx context.Context) error {
	for s.busy != nil {
		busy := s.busy
		s.mu.Unlock()
		select {
		case <-busy:
		case <-ctx.Done():
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// idle clears busy and wakes waiters. Called with s.mu held.
func (s *Shared) idle() {
	close(s.busy)
	s.busy = nil
}

// Join returns a reference to the lease s already holds, or calls mint to
// acquire one and holds that. A lease that is no longer valid (fenced,
// retired, lapsed) is not joined: ErrNotOwner. An invalid held lease does
// not call mint; an invalid minted lease is released. Concurrent Joins mint
// once: the others wait for that mint, and try their own if it failed.
func (s *Shared) Join(mint func() (*Lease, error)) (*Ref, error) {
	return s.JoinContext(context.Background(), mint)
}

// JoinContext is Join with a cancellable wait for another mint or the last
// release. It returns ctx.Err() if ctx ends while waiting, without calling
// mint or changing s. It does not cancel mint itself; mint must bound its
// own I/O. As with Join, call Start only after JoinContext returns.
func (s *Shared) JoinContext(ctx context.Context, mint func() (*Lease, error)) (*Ref, error) {
	s.mu.Lock()
	if err := s.wait(ctx); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if l := s.held; l != nil {
		defer s.mu.Unlock()
		if err := l.Valid(); err != nil {
			return nil, fmt.Errorf("lease: cannot join %s, it is no longer valid: %w", l.key, err)
		}
		s.refs++
		return &Ref{s: s, l: l}, nil
	}
	s.busy = make(chan struct{})
	s.mu.Unlock()
	// Deferred, so a panicking mint does not leave s busy forever.
	defer func() {
		s.mu.Lock()
		s.idle()
		s.mu.Unlock()
	}()

	l, err := mint()
	if err != nil {
		return nil, err
	}
	if err := l.Valid(); err != nil {
		// No Ref owns a rejected mint, so hand it back here.
		l.Release(ctx)
		return nil, err
	}
	s.mu.Lock()
	s.held, s.refs = l, 1
	s.mu.Unlock()
	return &Ref{s: s, l: l}, nil
}

// Ref is one holder's reference to a Shared lease.
type Ref struct {
	s        *Shared
	l        *Lease
	released bool
}

// Valid is the lease's Valid unless this reference was released. A nil Ref
// holds nothing.
func (r *Ref) Valid() error {
	if r == nil || r.s == nil {
		return fmt.Errorf("%w: no lease is held", ErrNotOwner)
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.released {
		return fmt.Errorf("%w: this reference was released", ErrNotOwner)
	}
	return r.l.Valid()
}

// Lease is the shared lease, nil for a nil Ref.
func (r *Ref) Lease() *Lease {
	if r == nil {
		return nil
	}
	return r.l
}

// Release drops this reference; the last one releases the lease (see
// Lease.Release; ctx bounds the handover). Calling it twice, or on a nil
// Ref, is a no-op.
func (r *Ref) Release(ctx context.Context) {
	if r == nil {
		return
	}
	s := r.s
	s.mu.Lock()
	if r.released || s.held != r.l {
		r.released = true
		s.mu.Unlock()
		return
	}
	r.released = true
	s.refs--
	if s.refs > 0 {
		s.mu.Unlock()
		return
	}
	// Last reference. A Join arriving now waits for the handover, or its
	// fresh acquisition would find this lease still live.
	s.held, s.busy = nil, make(chan struct{})
	s.mu.Unlock()
	// Deferred, as in Join, so a panicking Release does not leave s busy.
	defer func() {
		s.mu.Lock()
		s.idle()
		s.mu.Unlock()
	}()
	r.l.Release(ctx)
}
