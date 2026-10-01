package objstore

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
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

// ErrAccessDenied is wrapped by every error for a request the store refused
// (403), or for an object whose KMS key is disabled, deleted or out of reach.
var ErrAccessDenied = errors.New("store: access denied")

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
	// kmsKeys names the KMS key each written object gets (WithKMSKeys).
	kmsKeys KMSKeyFunc
	// writes bounds non-urgent object writes and deletes in flight; see
	// Config.MaxInflightWrites. nil means unbounded.
	writes *semaphore.Weighted
	// id names the bucket; see ID.
	id string
}

// Open returns a Store over b with cfg's pacing, write bound and
// KMS keys. The provider packages (fs, s3, gcs) each have an Open that
// builds the backend and calls this.
func Open(b Backend, cfg Config) *Store {
	s := &Store{b: b, id: rand.Text()}
	if n, ok := b.(interface{ ID() string }); ok {
		s.id = n.ID()
	}
	if cfg.RequestsPerSecond > 0 {
		s.b = &paced{Backend: b, pace: newPacer(cfg.RequestsPerSecond)}
	}
	if cfg.MaxInflightWrites >= 0 {
		s.writes = semaphore.NewWeighted(int64(cmp.Or(cfg.MaxInflightWrites, defaultMaxInflightWrites)))
	}
	return s
}

// ID names the bucket: Stores over the same bucket, opened apart or seen
// through WithBackend, share it. A backend without an ID method gets a
// random one per Open.
func (s *Store) ID() string { return s.id }

// WithBackend returns a Store over wrap(s's backend) with s's KMS keys
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

// WithKMSKeys returns s with every write asking fn for its object's KMS
// key. fn decides: its answer replaces any WithKMSKey on the write's
// context, which fn can read with KMSKey. The store encrypts and decrypts: reads need no key, and an object
// whose key is disabled or revoked fails with ErrAccessDenied. The file
// backend ignores keys (KMS reports false).
func (s *Store) WithKMSKeys(fn KMSKeyFunc) *Store {
	c := *s
	c.kmsKeys = fn
	return &c
}

// withKMSKey returns ctx carrying the KMS key s.kmsKeys names for key.
func (s *Store) withKMSKey(ctx context.Context, op, key string) (context.Context, error) {
	if s.kmsKeys == nil {
		return ctx, nil
	}
	id, err := s.kmsKeys(ctx, key)
	if err != nil {
		return ctx, OpErr(op, key, err)
	}
	return WithKMSKey(ctx, id), nil
}

// Put writes key unconditionally, replacing any object there.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	ctx, err := s.withKMSKey(ctx, "put", key)
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
	ctx, err := s.withKMSKey(ctx, "put-if-absent", key)
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
	return s.b.Get(ctx, key)
}

// GetRange reads exactly length bytes starting at offset. Short/out-of-bounds
// ranges fail instead of returning a plausible partial index block.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > math.MaxInt64-length {
		return nil, OpErr("get-range", key, ErrRange)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.b.GetRange(ctx, key, offset, length)
}

// GetWithETag returns the object and its ETag for conditional replacement.
func (s *Store) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	return s.b.GetWithETag(ctx, key)
}

// GetIfChanged conditionally reads one revision. Empty etag is unconditional.
// An unchanged object returns nil bytes, its ETag, true, nil. A changed object
// returns its complete bytes and ETag with false. Absence is ErrNotFound;
// failures never report unchanged. ETags are opaque backend values.
func (s *Store) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}
	return s.b.GetIfChanged(ctx, key, etag)
}

// PutIfMatch replaces key only if its current ETag equals etag — compare-and-
// swap, the manifest-swap primitive. (false, nil) = precondition failed: the
// object changed under us, or no longer exists. See Backend for the one
// case where ok is true alongside an error.
func (s *Store) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	ctx, err := s.withKMSKey(ctx, "put-if-match", key)
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

// Delete removes key. A missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	release, err := s.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.b.Delete(ctx, key)
}

// DeleteMany removes keys. Missing keys are not an error. Empty input is a
// no-op.
func (s *Store) DeleteMany(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
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
// A bucket that is already gone is not an error.
func (s *Store) DropBucket(ctx context.Context) error { return s.b.DropBucket(ctx) }

// KMS reports whether the backend applies per-object KMS keys
// (WithKMSKeys). The file backend does not: it ignores them and
// encrypts nothing.
func (s *Store) KMS() bool {
	k, ok := s.b.(interface{ SupportsKMS() bool })
	return ok && k.SupportsKMS()
}

// CheckConditionalWrites proves the backend honours If-None-Match: a first
// PutIfAbsent of a fresh _probe/ifnonematch/ key must create it and a second
// must not. A store that ignores the header would let two log writers both
// win one sequence. Three requests; call it once before writing.
func (s *Store) CheckConditionalWrites(ctx context.Context) error {
	name := fmt.Sprintf("%T", s.b)
	if n, ok := s.b.(fmt.Stringer); ok {
		name = n.String()
	}
	key := "_probe/ifnonematch/" + rand.Text()
	first, err := s.PutIfAbsent(ctx, key, []byte("1"))
	if err != nil {
		return fmt.Errorf("store %s: conditional write probe: %w", name, err)
	}
	second, err := s.PutIfAbsent(ctx, key, []byte("2"))
	if first {
		if derr := s.Delete(ctx, key); err == nil {
			err = derr
		}
	}
	if err != nil {
		return fmt.Errorf("store %s: conditional write probe: %w", name, err)
	}
	if !first || second {
		return fmt.Errorf("store %s does not honour conditional writes (If-None-Match): PutIfAbsent on a fresh key = %v, on an existing key = %v", name, first, second)
	}
	return nil
}
