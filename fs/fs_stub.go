//go:build !unix || aix || (solaris && !illumos)

package fs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/axiomhq/objstore"
)

// Backend is the file:// backend. This platform has no flock, so every
// operation fails with an error wrapping errors.ErrUnsupported; the
// package builds so that importers still build here.
type Backend struct {
	// IsolatedKeys is accepted and ignored on this platform.
	IsolatedKeys func(key string) bool

	root string
}

var errUnsupported = fmt.Errorf("fs: unsupported on %s: %w", runtime.GOOS, errors.ErrUnsupported)

var _ objstore.Backend = (*Backend)(nil)

// New returns the file backend for bucket, a directory under root.
func New(root, bucket string) *Backend {
	return &Backend{root: filepath.Join(filepath.FromSlash(root), bucket)}
}

// Open returns a Store over New(root, bucket) with cfg's pacing, write
// bound and encryption.
func Open(root, bucket string, cfg objstore.Config) *objstore.Store {
	return objstore.Open(New(root, bucket), cfg)
}

// Put fails: unsupported on this platform.
func (f *Backend) Put(_ context.Context, key string, _ []byte) error {
	return objstore.OpErr("put", key, errUnsupported)
}

// PutIfAbsent fails: unsupported on this platform.
func (f *Backend) PutIfAbsent(_ context.Context, key string, _ []byte) (bool, error) {
	return false, objstore.OpErr("put-if-absent", key, errUnsupported)
}

// Get fails: unsupported on this platform.
func (f *Backend) Get(_ context.Context, key string) ([]byte, error) {
	return nil, objstore.OpErr("get", key, errUnsupported)
}

// GetRange fails: unsupported on this platform.
func (f *Backend) GetRange(_ context.Context, key string, _, _ int64) ([]byte, error) {
	return nil, objstore.OpErr("get-range", key, errUnsupported)
}

// GetWithETag fails: unsupported on this platform.
func (f *Backend) GetWithETag(_ context.Context, key string) ([]byte, string, error) {
	return nil, "", objstore.OpErr("get-with-etag", key, errUnsupported)
}

// GetIfChanged fails: unsupported on this platform.
func (f *Backend) GetIfChanged(_ context.Context, key, _ string) ([]byte, string, bool, error) {
	return nil, "", false, objstore.OpErr("get-if-changed", key, errUnsupported)
}

// PutIfMatch fails: unsupported on this platform.
func (f *Backend) PutIfMatch(_ context.Context, key string, _ []byte, _ string) (bool, error) {
	return false, objstore.OpErr("put-if-match", key, errUnsupported)
}

// ListPage fails: unsupported on this platform.
func (f *Backend) ListPage(_ context.Context, prefix, _ string, _ int) ([]string, string, error) {
	return nil, "", objstore.OpErr("list-page", prefix, errUnsupported)
}

// ListPrefixesPage fails: unsupported on this platform.
func (f *Backend) ListPrefixesPage(_ context.Context, prefix, _ string, _ int) ([]string, string, error) {
	return nil, "", objstore.OpErr("list-prefixes-page", prefix, errUnsupported)
}

// Delete fails: unsupported on this platform.
func (f *Backend) Delete(_ context.Context, key string) error {
	return objstore.OpErr("delete", key, errUnsupported)
}

// DeleteMany fails: unsupported on this platform.
func (f *Backend) DeleteMany(_ context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return objstore.OpErr("delete-many", keys[0], errUnsupported)
}

// EnsureBucket fails: unsupported on this platform.
func (f *Backend) EnsureBucket(context.Context) error {
	return objstore.OpErr("create-bucket", f.root, errUnsupported)
}

// DropBucket fails: unsupported on this platform.
func (f *Backend) DropBucket(context.Context) error {
	return objstore.OpErr("drop-bucket", f.root, errUnsupported)
}
