package objstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/semaphore"
	"time"
)

var ErrNotFound = errors.New("object not found")
var ErrRange = errors.New("invalid object range")

// ErrConflict is a conditional write whose outcome the backend never
// decided: another conditional write on the same key was in flight (S3's
// 409 ConditionalRequestConflict). The object may or may not have been
// written; the caller retries. It is never a lost race, which is
// (false, nil).
var ErrConflict = errors.New("conditional write conflict, retry")

const MaxListPage = 1000

// Backend is the object-store contract every provider package satisfies
// (fs, s3, gcs); storetest.Conformance runs against each. Every error a
// backend returns is wrapped by OpErr. A failed condition is (false, nil),
// a missing object wraps ErrNotFound, a bad range wraps ErrRange. ETags
// are opaque. Listing is lexical, after is exclusive, next is the last
// key returned when more remain and "" otherwise. The file backend
// reserves terminal .lock and .tmp-* files; dot-prefixed directory
// elements remain valid so documented namespace names work.
type Backend interface {
	Put(ctx context.Context, key string, data []byte) error
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	GetWithETag(ctx context.Context, key string) ([]byte, string, error)
	GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error)
	PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error)
	ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error)
	ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error)
	Delete(ctx context.Context, key string) error
	DeleteMany(ctx context.Context, keys ...string) error
	EnsureBucket(ctx context.Context) error
	DropBucket(ctx context.Context) error
}

// Store is one bucket of write-once objects with the OCC primitives a
// log and a manifest build on (PutIfAbsent to append, PutIfMatch to swap).
type Store struct {
	b    Backend
	cmek *cmekState
	// writes bounds non-urgent object writes and deletes in flight; see
	// Config.MaxInflightWrites. nil (a zero Store) means unbounded.
	writes *semaphore.Weighted
}

// Open returns a Store over b with cfg's pacing and write bound. The
// provider packages (fs, s3, gcs) each have an Open that builds the
// backend and calls this.
func Open(b Backend, cfg Config) *Store {
	if cfg.RequestsPerSecond > 0 {
		b = &paced{Backend: b, pace: newPacer(cfg.RequestsPerSecond)}
	}
	writes := semaphore.NewWeighted(int64(cmp.Or(cfg.MaxInflightWrites, defaultMaxInflightWrites)))
	return &Store{b: b, writes: writes}
}

// WithBackend returns a Store over wrap(s's backend) with s's encryption
// and write bound: the same bucket seen through a wrapper.
func (s *Store) WithBackend(wrap func(Backend) Backend) *Store {
	return &Store{b: wrap(s.b), writes: s.writes, cmek: s.cmek}
}

// OpErr attaches the operation and the key to every error leaving a
// backend. A bare SDK error names neither, so a failure surfaced three
// layers up — mid-compaction, mid-replay, inside a query's fan-out — says
// only that storage was unhappy about something. %w keeps ErrNotFound
// unwrappable by errors.Is. nil stays nil.
func OpErr(op, key string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("store: %s %s: %w", op, key, err)
}

const defaultMaxInflightWrites = 16

// enterWrite takes a write slot unless ctx is Urgent. The returned release
// must be called once the backend call has returned.
func (s *Store) enterWrite(ctx context.Context) (func(), error) {
	if s.writes == nil || IsUrgent(ctx) {
		return func() {}, nil
	}
	defer TimingsOf(ctx).Since(CallGate, time.Now())
	if err := s.writes.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { s.writes.Release(1) }, nil
}

func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return err
	}
	if dek != nil {
		data, err = encryptObject(key, data, dek)
		if err != nil {
			return err
		}
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.Put(ctx, key, data)
}

// PutIfAbsent writes key only if it does not already exist. Returns false
// when the key existed. This is the OCC primitive a write-ahead log builds on.
func (s *Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return false, err
	}
	if dek != nil {
		data, err = encryptObject(key, data, dek)
		if err != nil {
			return false, err
		}
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return s.b.PutIfAbsent(ctx, key, data)
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return nil, err
	}
	data, err := s.b.Get(ctx, key)
	if err != nil || dek == nil {
		return data, err
	}
	if plaintextDeletedManifest(key, data) || plaintextFence(key, data) {
		return data, nil
	}
	return decryptObject(key, data, dek)
}

// GetRange reads exactly length bytes starting at offset. Short/out-of-bounds
// ranges fail instead of returning a plausible partial index block.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > int64(^uint64(0)>>1)-length {
		return nil, OpErr("get-range", key, ErrRange)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return nil, err
	}
	if dek == nil {
		return s.b.GetRange(ctx, key, offset, length)
	}
	return s.encryptedRange(ctx, key, offset, length, dek)
}

// GetWithETag returns the object and its ETag for conditional replacement.
func (s *Store) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return nil, "", err
	}
	data, tag, err := s.b.GetWithETag(ctx, key)
	if err != nil || dek == nil {
		return data, tag, err
	}
	if plaintextDeletedManifest(key, data) || plaintextFence(key, data) {
		return data, tag, nil
	}
	plain, err := decryptObject(key, data, dek)
	return plain, tag, err
}

// GetIfChanged conditionally reads one revision. Empty etag is unconditional.
// An unchanged object returns nil bytes, its ETag, true, nil. A changed object
// returns its complete bytes and ETag with false. Absence is ErrNotFound;
// failures never report unchanged. ETags are opaque backend values.
func (s *Store) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return nil, "", false, err
	}
	data, tag, unchanged, err := s.b.GetIfChanged(ctx, key, etag)
	if err != nil || unchanged || dek == nil {
		return data, tag, unchanged, err
	}
	if plaintextDeletedManifest(key, data) || plaintextFence(key, data) {
		return data, tag, false, nil
	}
	plain, err := decryptObject(key, data, dek)
	return plain, tag, false, err
}

// PutIfMatch replaces key only if its current ETag equals etag — compare-and-
// swap, the manifest-swap primitive. (false, nil) = precondition failed: the
// object changed under us, or no longer exists.
func (s *Store) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	dek, err := s.objectKey(ctx, key)
	if err != nil {
		return false, err
	}
	if dek != nil {
		data, err = encryptObject(key, data, dek)
		if err != nil {
			return false, err
		}
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return s.b.PutIfMatch(ctx, key, data, etag)
}

// ListPage returns at most limit keys under prefix, lexically after after.
// next is the last returned key when another page remains, otherwise empty.
func (s *Store) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > MaxListPage {
		return nil, "", OpErr("list-page", prefix, errors.New("limit must be between 1 and 1000"))
	}
	return s.b.ListPage(ctx, prefix, after, limit)
}

// List returns all keys under prefix, lexically sorted.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for after := ""; ; {
		page, next, err := s.ListPage(ctx, prefix, after, 1000)
		if err != nil {
			return nil, err
		}
		keys = append(keys, page...)
		if next == "" {
			return keys, nil
		}
		after = next
	}
}

// ListPrefixes returns the immediate child prefixes under prefix — S3's
// CommonPrefixes with Delimiter "/", each ending in "/", lexically sorted.
// Discovering top-level names costs one request per page of names
// instead of one key per object in the bucket. A prefix appears
// only if at least one object lives under it, on both backends.
func (s *Store) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var prefixes []string
	for after := ""; ; {
		page, next, err := s.ListPrefixesPage(ctx, prefix, after, 1000)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, page...)
		if next == "" {
			return prefixes, nil
		}
		after = next
	}
}

func (s *Store) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > MaxListPage {
		return nil, "", OpErr("list-prefixes-page", prefix, errors.New("limit must be between 1 and 1000"))
	}
	return s.b.ListPrefixesPage(ctx, prefix, after, limit)
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if _, err := s.objectKey(ctx, key); err != nil {
		return err
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.Delete(ctx, key)
}

// DeleteMany removes keys. Missing keys are not an error. Empty input is a no-op.
func (s *Store) DeleteMany(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		if _, err := s.objectKey(ctx, key); err != nil {
			return err
		}
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.DeleteMany(ctx, keys...)
}

// EnsureBucket creates the bucket if missing (dev/test convenience).
func (s *Store) EnsureBucket(ctx context.Context) error { return s.b.EnsureBucket(ctx) }

// DropBucket deletes every object and then the bucket itself (test
// teardown; the harness's fresh bucket per test must not outlive the test).
func (s *Store) DropBucket(ctx context.Context) error { return s.b.DropBucket(ctx) }
