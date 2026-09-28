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
// Every method is callable from a fence callback, which runs on its own
// goroutine.
type Shared struct {
	mu   sync.Mutex
	cond *sync.Cond // signals busy -> false; lazily bound to mu
	busy bool       // a mint or the last release is in flight
	held *Lease
	refs int
}

// Held reports whether a lease is currently held through s.
func (s *Shared) Held() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held != nil
}

// wait blocks until no mint or release is in flight. Called with s.mu held.
func (s *Shared) wait() {
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	for s.busy {
		s.cond.Wait()
	}
}

// idle clears busy and wakes waiters. Called with s.mu held.
func (s *Shared) idle() {
	s.busy = false
	if s.cond != nil {
		s.cond.Broadcast()
	}
}

// Join returns a reference to the lease s already holds, or calls mint to
// acquire one and holds that. A held lease that is no longer valid (fenced,
// lapsed) is not joined: ErrNotOwner, and mint is not called. Concurrent Joins mint once:
// the others wait for that mint, and try their own if it failed.
func (s *Shared) Join(mint func() (*Lease, error)) (*Ref, error) {
	s.mu.Lock()
	s.wait()
	if l := s.held; l != nil {
		defer s.mu.Unlock()
		if err := l.Valid(); err != nil {
			return nil, fmt.Errorf("lease: cannot join %s, it is no longer valid: %w", l.key, err)
		}
		s.refs++
		return &Ref{s: s, l: l}, nil
	}
	s.busy = true
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

// Valid is the lease's Valid. A nil Ref holds nothing.
func (r *Ref) Valid() error {
	if r == nil {
		return fmt.Errorf("%w: no lease is held", ErrNotOwner)
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
	s.held, s.busy = nil, true
	s.mu.Unlock()
	// Deferred, as in Join, so a panicking Release does not leave s busy.
	defer func() {
		s.mu.Lock()
		s.idle()
		s.mu.Unlock()
	}()
	r.l.Release(ctx)
}
