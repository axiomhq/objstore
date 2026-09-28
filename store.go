package objstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// ErrNotFound is wrapped by every error for a missing object or bucket.
var ErrNotFound = errors.New("store: object not found")

// ErrRange is wrapped by every error for a range outside the object, or an
// offset or length that is not a valid range at all.
var ErrRange = errors.New("store: invalid object range")

// ErrInvalidKey is wrapped by every error for a key a backend refuses to
// store (the file backend rejects empty elements, "." and "..", and its own
// reserved file names).
var ErrInvalidKey = errors.New("store: invalid key")

// ErrConflict is a conditional write whose outcome the backend never
// decided: another conditional write on the same key was in flight (S3's
// 409 ConditionalRequestConflict). The object may or may not have been
// written; the caller retries. It is never a lost race, which is
// (false, nil).
var ErrConflict = errors.New("store: conditional write conflict, retry")

// MaxListPage is the largest limit ListPage and ListPrefixesPage accept,
// S3's own page size.
const MaxListPage = 1000

// Backend is the object-store contract every provider package satisfies
// (fs, s3, gcs); storetest.Conformance runs against each. It may grow.
//
// Every error a backend returns is wrapped by OpErr. A failed condition is
// (false, nil), a missing object wraps ErrNotFound, a bad range wraps
// ErrRange. The ok of a conditional write is meaningful only when err ==
// nil, except that a backend may report (true, err) when the object is
// visible but not proven durable (the file backend, when the directory
// sync after the publish fails); a retry of the same write settles it.
// ETags are opaque. Listing is lexical, after is exclusive, next is the
// last key returned when more remain and "" otherwise. The file backend
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
	b Backend
	// cmek is shared with every Store WithBackend derives, so a key cached
	// or configured through one is seen through all. nil (a zero Store)
	// means no encryption.
	cmek *atomic.Pointer[cmekState]
	// refresh is Config.KeyRefreshInterval, shared the same way and with
	// the encryption state, so it holds whether set before or after a
	// provider is configured.
	refresh *atomic.Int64
	// accept is Config.AcceptPlaintext, or DefaultAcceptPlaintext.
	accept func(key string, data []byte) bool
	// writes bounds non-urgent object writes and deletes in flight; see
	// Config.MaxInflightWrites. nil (a zero Store) means unbounded.
	writes *semaphore.Weighted
}

// Open returns a Store over b with cfg's pacing, write bound and
// encryption. The provider packages (fs, s3, gcs) each have an Open that
// builds the backend and calls this.
func Open(b Backend, cfg Config) *Store {
	if cfg.RequestsPerSecond > 0 {
		b = &paced{Backend: b, pace: newPacer(cfg.RequestsPerSecond)}
	}
	s := &Store{b: b, cmek: new(atomic.Pointer[cmekState]), refresh: new(atomic.Int64), accept: cfg.AcceptPlaintext}
	s.refresh.Store(int64(cfg.KeyRefreshInterval))
	if s.accept == nil {
		s.accept = DefaultAcceptPlaintext
	}
	if cfg.MaxInflightWrites >= 0 {
		s.writes = semaphore.NewWeighted(int64(cmp.Or(cfg.MaxInflightWrites, defaultMaxInflightWrites)))
	}
	if cfg.KeyProvider != nil {
		s.cmek.Store(newCMEKState(cfg.KeyProvider, s.refresh))
	}
	return s
}

// WithBackend returns a Store over wrap(s's backend) with s's encryption
// and write bound: the same bucket seen through a wrapper.
func (s *Store) WithBackend(wrap func(Backend) Backend) *Store {
	c := *s
	c.b = wrap(s.b)
	return &c
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

// seal returns data as it is stored: encrypted when key lies in a
// namespace with a key record.
func (s *Store) seal(ctx context.Context, op, key string, data []byte) ([]byte, error) {
	dek, err := s.objectKey(ctx, key, false)
	if err != nil {
		return nil, OpErr(op, key, err)
	}
	if dek == nil {
		return data, nil
	}
	sealed, err := encryptObject(key, data, dek)
	return sealed, OpErr(op, key, err)
}

// Put writes key unconditionally, replacing any object there.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	data, err := s.seal(ctx, "put", key, data)
	if err != nil {
		return err
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.Put(ctx, key, data)
}

// PutIfAbsent writes key only if it does not already exist. Returns false
// when the key existed. This is the OCC primitive a write-ahead log builds
// on. See Backend for the one case where ok is true alongside an error.
func (s *Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	data, err := s.seal(ctx, "put-if-absent", key, data)
	if err != nil {
		return false, err
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return s.b.PutIfAbsent(ctx, key, data)
}

// Get returns the whole object at key; a missing object wraps ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	dek, err := s.objectKey(ctx, key, true)
	if err != nil {
		return nil, OpErr("get", key, err)
	}
	data, err := s.b.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.open(ctx, "get", key, data, dek)
}

// GetRange reads exactly length bytes starting at offset. Short/out-of-bounds
// ranges fail instead of returning a plausible partial index block.
// In an encrypted namespace the range is always decrypted:
// Config.AcceptPlaintext is never consulted.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > math.MaxInt64-length {
		return nil, OpErr("get-range", key, ErrRange)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dek, err := s.objectKey(ctx, key, false)
	if err != nil {
		return nil, OpErr("get-range", key, err)
	}
	if dek == nil {
		return s.b.GetRange(ctx, key, offset, length)
	}
	return s.encryptedRange(ctx, key, offset, length, dek)
}

// GetWithETag returns the object and its ETag for conditional replacement.
func (s *Store) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	dek, err := s.objectKey(ctx, key, true)
	if err != nil {
		return nil, "", OpErr("get-with-etag", key, err)
	}
	data, tag, err := s.b.GetWithETag(ctx, key)
	if err != nil {
		return nil, "", err
	}
	plain, err := s.open(ctx, "get-with-etag", key, data, dek)
	if err != nil {
		return nil, "", err
	}
	return plain, tag, nil
}

// GetIfChanged conditionally reads one revision. Empty etag is unconditional.
// An unchanged object returns nil bytes, its ETag, true, nil. A changed object
// returns its complete bytes and ETag with false. Absence is ErrNotFound;
// failures never report unchanged. ETags are opaque backend values.
func (s *Store) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}
	dek, err := s.objectKey(ctx, key, true)
	if err != nil {
		return nil, "", false, OpErr("get-if-changed", key, err)
	}
	data, tag, unchanged, err := s.b.GetIfChanged(ctx, key, etag)
	if err != nil || unchanged {
		return data, tag, unchanged, err
	}
	plain, err := s.open(ctx, "get-if-changed", key, data, dek)
	if err != nil {
		return nil, "", false, err
	}
	return plain, tag, false, nil
}

// PutIfMatch replaces key only if its current ETag equals etag — compare-and-
// swap, the manifest-swap primitive. (false, nil) = precondition failed: the
// object changed under us, or no longer exists. See Backend for the one
// case where ok is true alongside an error.
func (s *Store) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	data, err := s.seal(ctx, "put-if-match", key, data)
	if err != nil {
		return false, err
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return s.b.PutIfMatch(ctx, key, data, etag)
}

var errListLimit = fmt.Errorf("limit must be between 1 and %d", MaxListPage)

// ListPage returns at most limit (1 to MaxListPage) keys under prefix,
// lexically after after. next is the last returned key when another page
// remains, otherwise empty.
func (s *Store) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > MaxListPage {
		return nil, "", OpErr("list-page", prefix, errListLimit)
	}
	return s.b.ListPage(ctx, prefix, after, limit)
}

// List returns all keys under prefix, lexically sorted.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for after := ""; ; {
		page, next, err := s.ListPage(ctx, prefix, after, MaxListPage)
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
// only if at least one object lives under it, on every backend.
func (s *Store) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var prefixes []string
	for after := ""; ; {
		page, next, err := s.ListPrefixesPage(ctx, prefix, after, MaxListPage)
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

// ListPrefixesPage is one page of ListPrefixes: at most limit (1 to
// MaxListPage) prefixes lexically after after, and the cursor as in
// ListPage.
func (s *Store) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > MaxListPage {
		return nil, "", OpErr("list-prefixes-page", prefix, errListLimit)
	}
	return s.b.ListPrefixesPage(ctx, prefix, after, limit)
}

// Delete removes key. A missing key is not an error. It never needs the
// namespace key, so deleting (crypto-shredding) works after revocation.
func (s *Store) Delete(ctx context.Context, key string) error {
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.Delete(ctx, key)
}

// DeleteMany removes keys. Missing keys are not an error. Empty input is a
// no-op. Like Delete it never needs the namespace key.
func (s *Store) DeleteMany(ctx context.Context, keys ...string) error {
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
// A bucket that is already gone is not an error.
func (s *Store) DropBucket(ctx context.Context) error { return s.b.DropBucket(ctx) }
