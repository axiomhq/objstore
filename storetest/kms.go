package storetest

import (
	"context"
	"sync"

	"github.com/axiomhq/objstore"
)

// KMS stands in for S3 SSE-KMS over any backend: it remembers the KMS key
// each object was written with and, while that key is revoked, answers
// objstore.ErrAccessDenied for the object's reads and for writes under it,
// as S3 does. It encrypts nothing.
type KMS struct {
	objstore.Backend
	mutation sync.Mutex // backing mutations and their key bookkeeping
	mu       sync.Mutex
	keys     map[string]string // object -> KMS key id
	revoked  map[string]bool
}

// NewKMS wraps s's backend in a KMS. The returned Store reports KMS() true.
func NewKMS(s *objstore.Store) (*objstore.Store, *KMS) {
	k := &KMS{keys: map[string]string{}, revoked: map[string]bool{}}
	return s.WithBackend(func(b objstore.Backend) objstore.Backend { k.Backend = b; return k }), k
}

func (k *KMS) SupportsKMS() bool { return true }

// Close passes through to the wrapped backend's Close, if any (Store.Close).
func (k *KMS) Close() error {
	if c, ok := k.Backend.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// KeyOf is the KMS key id object was last written with, "" for none.
func (k *KMS) KeyOf(object string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.keys[object]
}

// Revoke denies every object written under key id until Restore.
func (k *KMS) Revoke(id string) { k.mu.Lock(); k.revoked[id] = true; k.mu.Unlock() }

func (k *KMS) Restore(id string) { k.mu.Lock(); delete(k.revoked, id); k.mu.Unlock() }

func (k *KMS) denied(id string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if id != "" && k.revoked[id] {
		return objstore.ErrAccessDenied
	}
	return nil
}

func (k *KMS) readable(object string) error { return k.denied(k.KeyOf(object)) }

func (k *KMS) wrote(ctx context.Context, object string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if id := objstore.KMSKey(ctx); id != "" {
		k.keys[object] = id
	} else {
		delete(k.keys, object)
	}
}

// plain strips the KMS key from ctx before a write reaches the wrapped
// backend. KMS stands in for the key service, so the backend underneath must
// not see a key: a real S3 without KMS (MinIO in CI) refuses SSE-KMS writes.
func plain(ctx context.Context) context.Context { return objstore.WithKMSKey(ctx, "") }

func (k *KMS) Put(ctx context.Context, key string, data []byte) error {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	if err := k.denied(objstore.KMSKey(ctx)); err != nil {
		return err
	}
	err := k.Backend.Put(plain(ctx), key, data)
	if err == nil {
		k.wrote(ctx, key)
	}
	return err
}

func (k *KMS) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	if err := k.denied(objstore.KMSKey(ctx)); err != nil {
		return false, err
	}
	ok, err := k.Backend.PutIfAbsent(plain(ctx), key, data)
	if ok && err == nil {
		k.wrote(ctx, key)
	}
	return ok, err
}

func (k *KMS) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	if err := k.denied(objstore.KMSKey(ctx)); err != nil {
		return false, err
	}
	ok, err := k.Backend.PutIfMatch(plain(ctx), key, data, etag)
	if ok && err == nil {
		k.wrote(ctx, key)
	}
	return ok, err
}

func (k *KMS) forget(keys ...string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, key := range keys {
		delete(k.keys, key)
	}
}

func (k *KMS) Delete(ctx context.Context, key string) error {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	err := k.Backend.Delete(ctx, key)
	if err == nil {
		k.forget(key)
	}
	return err
}

func (k *KMS) DeleteMany(ctx context.Context, keys ...string) error {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	err := k.Backend.DeleteMany(ctx, keys...)
	if err == nil {
		k.forget(keys...)
	}
	return err
}

func (k *KMS) DropBucket(ctx context.Context) error {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	err := k.Backend.DropBucket(ctx)
	if err == nil {
		k.mu.Lock()
		clear(k.keys)
		k.mu.Unlock()
	}
	return err
}

func (k *KMS) Get(ctx context.Context, key string) ([]byte, error) {
	if err := k.readable(key); err != nil {
		return nil, err
	}
	return k.Backend.Get(ctx, key)
}

func (k *KMS) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := k.readable(key); err != nil {
		return nil, err
	}
	return k.Backend.GetRange(ctx, key, offset, length)
}

func (k *KMS) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if err := k.readable(key); err != nil {
		return nil, "", err
	}
	return k.Backend.GetWithETag(ctx, key)
}

func (k *KMS) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := k.readable(key); err != nil {
		return nil, "", false, err
	}
	return k.Backend.GetIfChanged(ctx, key, etag)
}
