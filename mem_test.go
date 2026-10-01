package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
)

// memBackend is an in-memory Backend for this package's own tests, which
// cannot import objstore/fs (it imports this package).
type memBackend struct {
	mu      sync.Mutex
	exists  bool
	objects map[string][]byte
}

func newMemBackend() *memBackend { return &memBackend{exists: true, objects: map[string][]byte{}} }

func memETag(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }

var errNoBucket = errors.New("bucket does not exist")

// check reports ctx's error or a missing bucket. Callers hold m.mu.
func (m *memBackend) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.exists {
		return errNoBucket
	}
	return nil
}

func (m *memBackend) Put(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return OpErr("put", key, err)
	}
	m.objects[key] = slices.Clone(data)
	return nil
}

func (m *memBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return false, OpErr("put-if-absent", key, err)
	}
	if _, ok := m.objects[key]; ok {
		return false, nil
	}
	m.objects[key] = slices.Clone(data)
	return true, nil
}

func (m *memBackend) get(ctx context.Context, op, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return nil, OpErr(op, key, err)
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, OpErr(op, key, ErrNotFound)
	}
	return slices.Clone(data), nil
}

func (m *memBackend) Get(ctx context.Context, key string) ([]byte, error) {
	return m.get(ctx, "get", key)
}

func (m *memBackend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	data, err := m.get(ctx, "get-range", key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || offset > int64(len(data)) || length > int64(len(data))-offset {
		return nil, OpErr("get-range", key, ErrRange)
	}
	return data[offset : offset+length], nil
}

func (m *memBackend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, err := m.get(ctx, "get-with-etag", key)
	if err != nil {
		return nil, "", err
	}
	return data, memETag(data), nil
}

func (m *memBackend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	data, err := m.get(ctx, "get-if-changed", key)
	if err != nil {
		return nil, "", false, err
	}
	tag := memETag(data)
	if etag != "" && tag == etag {
		return nil, tag, true, nil
	}
	return data, tag, false, nil
}

func (m *memBackend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return false, OpErr("put-if-match", key, err)
	}
	cur, ok := m.objects[key]
	if !ok || memETag(cur) != etag {
		return false, nil
	}
	m.objects[key] = slices.Clone(data)
	return true, nil
}

// page returns up to limit sorted names after after, and the cursor.
func page(names []string, after string, limit int) ([]string, string) {
	slices.Sort(names)
	names = slices.Compact(names)
	i, _ := slices.BinarySearch(names, after)
	if i < len(names) && names[i] == after {
		i++
	}
	names = names[i:]
	if len(names) <= limit {
		return names, ""
	}
	return names[:limit], names[limit-1]
}

func (m *memBackend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return nil, "", OpErr("list-page", prefix, err)
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	keys, next := page(keys, after, limit)
	return keys, next, nil
}

func (m *memBackend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return nil, "", OpErr("list-prefixes-page", prefix, err)
	}
	var prefixes []string
	for k := range m.objects {
		rest, ok := strings.CutPrefix(k, prefix)
		if i := strings.IndexByte(rest, '/'); ok && i >= 0 {
			prefixes = append(prefixes, prefix+rest[:i+1])
		}
	}
	prefixes, next := page(prefixes, after, limit)
	return prefixes, next, nil
}

func (m *memBackend) Delete(ctx context.Context, key string) error {
	return m.DeleteMany(ctx, key)
}

func (m *memBackend) DeleteMany(ctx context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		if err := m.check(ctx); err != nil {
			return OpErr("delete-many", k, err)
		}
		delete(m.objects, k)
	}
	return nil
}

func (m *memBackend) EnsureBucket(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return OpErr("create-bucket", "mem", err)
	}
	m.exists = true
	return nil
}

func (m *memBackend) DropBucket(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return OpErr("drop-bucket", "mem", err)
	}
	m.exists, m.objects = false, map[string][]byte{}
	return nil
}
