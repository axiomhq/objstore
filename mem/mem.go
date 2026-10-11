// Package mem is a Backend in process memory: for tests, and for a process
// whose data need not outlive it. Objects are copied in and out, so callers
// may reuse their buffers. An ETag is the hex SHA-256 of the content, as in
// package fs.
package mem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/axiomhq/objstore"
)

// Backend is one in-memory bucket. Its zero value is a missing bucket;
// New returns one that exists.
type Backend struct {
	mu      sync.RWMutex
	objects map[string][]byte // nil: the bucket does not exist
}

// New returns an existing, empty bucket.
func New() *Backend { return &Backend{objects: make(map[string][]byte)} }

// Open returns a Store on a new, empty in-memory bucket.
func Open(cfg objstore.Config) *objstore.Store { return objstore.Open(New(), cfg) }

var errBucket = fmt.Errorf("store: bucket mem: %w", objstore.ErrNotFound)

func etag(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// read runs f under the read lock once ctx and the bucket allow it.
func (m *Backend) read(ctx context.Context, op, key string, f func() error) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr(op, key, err)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.objects == nil {
		return objstore.OpErr(op, key, errBucket)
	}
	return objstore.OpErr(op, key, f())
}

// write is read under the write lock.
func (m *Backend) write(ctx context.Context, op, key string, f func() error) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr(op, key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objects == nil {
		return objstore.OpErr(op, key, errBucket)
	}
	return objstore.OpErr(op, key, f())
}

func validKey(key string) error {
	if key == "" {
		return objstore.ErrInvalidKey
	}
	return nil
}

func (m *Backend) Put(ctx context.Context, key string, data []byte) error {
	return m.write(ctx, "put", key, func() error {
		if err := validKey(key); err != nil {
			return err
		}
		m.objects[key] = bytes.Clone(data)
		return nil
	})
}

func (m *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (ok bool, err error) {
	err = m.write(ctx, "put-if-absent", key, func() error {
		if err := validKey(key); err != nil {
			return err
		}
		if _, exists := m.objects[key]; !exists {
			m.objects[key], ok = bytes.Clone(data), true
		}
		return nil
	})
	return ok, err
}

func (m *Backend) PutIfMatch(ctx context.Context, key string, data []byte, tag string) (ok bool, err error) {
	err = m.write(ctx, "put-if-match", key, func() error {
		if cur, exists := m.objects[key]; exists && etag(cur) == tag {
			m.objects[key], ok = bytes.Clone(data), true
		}
		return nil
	})
	return ok, err
}

func (m *Backend) get(ctx context.Context, op, key string) (data []byte, err error) {
	err = m.read(ctx, op, key, func() error {
		b, ok := m.objects[key]
		if !ok {
			return objstore.ErrNotFound
		}
		data = bytes.Clone(b)
		return nil
	})
	return data, err
}

func (m *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	return m.get(ctx, "get", key)
}

func (m *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, err := m.get(ctx, "get-with-etag", key)
	if err != nil {
		return nil, "", err
	}
	return data, etag(data), nil
}

func (m *Backend) GetIfChanged(ctx context.Context, key, tag string) ([]byte, string, bool, error) {
	data, current, err := m.GetWithETag(ctx, key)
	if err != nil {
		return nil, "", false, err
	}
	if tag != "" && current == tag {
		return nil, current, true, nil
	}
	return data, current, false, nil
}

// GetRange returns exactly length bytes of key at offset; a range past the
// end is ErrRange.
func (m *Backend) GetRange(ctx context.Context, key string, offset, length int64) (data []byte, err error) {
	err = m.read(ctx, "get-range", key, func() error {
		b, ok := m.objects[key]
		if !ok {
			return objstore.ErrNotFound
		}
		size := int64(len(b))
		if offset < 0 || length < 0 || offset > size || length > size-offset {
			return objstore.ErrRange
		}
		data = bytes.Clone(b[offset : offset+length])
		return nil
	})
	return data, err
}

// list returns the sorted, distinct names name(key) gives the keys under
// prefix, keeping those after after, paged by limit.
func (m *Backend) list(ctx context.Context, op, prefix, after string, limit int, name func(key string) (string, bool)) (out []string, next string, err error) {
	err = m.read(ctx, op, prefix, func() error {
		if limit < 1 {
			return fmt.Errorf("limit %d < 1", limit)
		}
		for key := range m.objects {
			if n, ok := name(key); ok && strings.HasPrefix(key, prefix) && n > after {
				out = append(out, n)
			}
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) > limit {
		out = out[:limit]
		next = out[limit-1]
	}
	return out, next, nil
}

// ListPage lists up to limit keys under prefix after after.
func (m *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	return m.list(ctx, "list-page", prefix, after, limit, func(key string) (string, bool) { return key, true })
}

// ListPrefixesPage lists up to limit child prefixes under prefix after
// after: each key under prefix with a '/' past it gives the prefix up to
// and including that '/'.
func (m *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	return m.list(ctx, "list-prefixes-page", prefix, after, limit, func(key string) (string, bool) {
		if !strings.HasPrefix(key, prefix) {
			return "", false
		}
		i := strings.IndexByte(key[len(prefix):], '/')
		if i < 0 {
			return "", false
		}
		return key[:len(prefix)+i+1], true
	})
}

// Delete removes the object at key. A missing key is not an error.
func (m *Backend) Delete(ctx context.Context, key string) error {
	return m.write(ctx, "delete", key, func() error {
		delete(m.objects, key)
		return nil
	})
}

// DeleteMany removes keys at once: all of them, or, on a cancelled context
// or a missing bucket, none.
func (m *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	err := m.write(ctx, "delete-many", "", func() error {
		for _, k := range keys {
			delete(m.objects, k)
		}
		return nil
	})
	if err == nil || len(keys) == 0 {
		return err
	}
	e := &objstore.DeleteError{}
	for _, k := range keys {
		e.Failures = append(e.Failures, objstore.DeleteFailure{Key: k, Err: err, Unattempted: true})
	}
	return e
}

// EnsureBucket creates the bucket if it is missing.
func (m *Backend) EnsureBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr("create-bucket", "mem", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objects == nil {
		m.objects = make(map[string][]byte)
	}
	return nil
}

// DropBucket removes the bucket and everything in it. A missing bucket is
// not an error.
func (m *Backend) DropBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr("drop-bucket", "mem", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects = nil
	return nil
}
