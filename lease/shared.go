package lease

import (
	"fmt"
	"sync"
)

// Shared is one lease shared by reference count among the holders inside
// one process: several concurrent operations on the same key take one lease
// between them, and the last Release hands it back. The zero value holds
// nothing.
type Shared struct {
	mu   sync.Mutex
	held *Lease
	refs int
}

// Held reports whether a lease is currently held through s.
func (s *Shared) Held() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held != nil
}

// Join returns a reference to the lease s already holds, or calls mint to
// acquire one and holds that. A held lease that has been fenced is not
// joined: ErrNotOwner, and mint is not called.
func (s *Shared) Join(mint func() (*Lease, error)) (*Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.held; l != nil {
		if err := l.Valid(); err != nil {
			return nil, fmt.Errorf("%w: %s was fenced in this process: %v", ErrNotOwner, l.key, err)
		}
		s.refs++
		return &Ref{s: s, l: l}, nil
	}
	l, err := mint()
	if err != nil {
		return nil, err
	}
	s.held, s.refs = l, 1
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

// Release drops this reference; the last one releases the lease. Calling
// it twice, or on a nil Ref, is a no-op.
func (r *Ref) Release() {
	if r == nil {
		return
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	if r.s.held != r.l {
		return
	}
	r.s.refs--
	if r.s.refs > 0 {
		return
	}
	r.s.held = nil
	r.l.Release()
}
