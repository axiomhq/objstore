package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"
)

// ObjectInfo describes one revision. ETag is the backend's opaque CAS token,
// interchangeable with GetWithETag, not a portable checksum. Upload results
// always contain Size and ETag from the commit; other fields may be absent.
type ObjectInfo struct {
	Size         int64
	ETag         string
	Metadata     map[string]string
	ContentType  string
	CacheControl string
	StorageClass string
	LastModified time.Time
}

// UploadOptions controls a streaming write. Size nil means read to EOF; a
// non-nil Size means consume exactly that many bytes (zero is an empty object).
// Surplus bytes are left unread. A short body fails, but any error may have an
// ambiguous commit outcome. The caller owns body and must unblock a blocked
// Read on cancellation; Upload never closes it. Options unsupported by a
// backend are rejected before writing, never silently ignored.
type UploadOptions struct {
	Size         *int64
	Metadata     map[string]string
	ContentType  string
	CacheControl string
	StorageClass string
}

// ReaderBackend is the optional streaming read capability. The caller closes
// the reader. length 0 means to EOF; positive lengths are capped at EOF.
// At or beyond EOF an existing object returns an empty reader. Negative or
// overflowing ranges wrap ErrRange. This does not change exact GetRange.
type ReaderBackend interface {
	NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// StatBackend is the optional object metadata capability.
type StatBackend interface {
	Stat(ctx context.Context, key string) (ObjectInfo, error)
}

// UploadBackend is the optional unconditional streaming upload capability.
type UploadBackend interface {
	Upload(ctx context.Context, key string, body io.Reader, opts UploadOptions) (ObjectInfo, error)
}

// ConditionalUploadBackend is streaming CAS. A lost condition returns zero
// info, false, nil. An error is not proof of non-commit; see Backend.
type ConditionalUploadBackend interface {
	UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts UploadOptions) (ObjectInfo, bool, error)
	UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts UploadOptions) (ObjectInfo, bool, error)
}

// SignOptions selects an HTTP method, positive lifetime and required headers.
// Signing support and allowed headers are provider-specific; unsupported
// methods or headers wrap errors.ErrUnsupported. It never uploads a body.
type SignOptions struct {
	Method  string
	Expires time.Duration
	Headers http.Header
}

// SignedRequest must be used with its Method and all its Headers unchanged.
// URL contains credentials: do not log it. Signing bypasses the write gate
// because it performs no object mutation.
type SignedRequest struct {
	URL     string
	Method  string
	Headers http.Header
}

// SignBackend is the optional signed request capability.
type SignBackend interface {
	Sign(ctx context.Context, key string, opts SignOptions) (SignedRequest, error)
}

// NewReader streams an EOF-tolerant range; it never falls back to Get.
func (s *Store) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 || offset > math.MaxInt64-length {
		return nil, OpErr("new-reader", key, ErrRange)
	}
	if err := ctx.Err(); err != nil {
		return nil, OpErr("new-reader", key, err)
	}
	if b, ok := s.b.(ReaderBackend); ok {
		return b.NewReader(ctx, key, offset, length)
	}
	return nil, OpErr("new-reader", key, errors.ErrUnsupported)
}

// Stat returns information for the current revision without downloading it
// on cloud providers. The file backend hashes the file to obtain its CAS token.
func (s *Store) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, OpErr("stat", key, err)
	}
	if b, ok := s.b.(StatBackend); ok {
		return b.Stat(ctx, key)
	}
	return ObjectInfo{}, OpErr("stat", key, errors.ErrUnsupported)
}

// Upload writes a stream unconditionally. A provider may use bounded multipart
// or resumable uploads. The returned token belongs to this commit, not a later
// HEAD. The write slot is held until the complete upload returns.
func (s *Store) Upload(ctx context.Context, key string, body io.Reader, opts UploadOptions) (ObjectInfo, error) {
	info, _, err := s.upload(ctx, "upload", key, body, "", opts)
	return info, err
}

// UploadIfAbsent creates an object only when no live object exists.
func (s *Store) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts UploadOptions) (ObjectInfo, bool, error) {
	return s.upload(ctx, "upload-if-absent", key, body, "", opts)
}

// UploadIfMatch replaces an object only if its opaque CAS token matches etag.
func (s *Store) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts UploadOptions) (ObjectInfo, bool, error) {
	return s.upload(ctx, "upload-if-match", key, body, etag, opts)
}

func (s *Store) upload(ctx context.Context, op, key string, body io.Reader, etag string, opts UploadOptions) (ObjectInfo, bool, error) {
	if body == nil || opts.Size != nil && *opts.Size < 0 {
		return ObjectInfo{}, false, OpErr(op, key, errors.New("nil body or negative size"))
	}
	ctx, err := s.withKMSKey(ctx, op, key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, false, OpErr(op, key, err)
	}
	release, err := s.enterWrite(ctx)
	if err != nil {
		return ObjectInfo{}, false, OpErr(op, key, err)
	}
	defer release()
	if op == "upload" {
		if b, ok := s.b.(UploadBackend); ok {
			info, err := b.Upload(ctx, key, body, opts)
			return info, err == nil, err
		}
	} else if b, ok := s.b.(ConditionalUploadBackend); ok {
		if op == "upload-if-absent" {
			return b.UploadIfAbsent(ctx, key, body, opts)
		}
		return b.UploadIfMatch(ctx, key, body, etag, opts)
	}
	return ObjectInfo{}, false, OpErr(op, key, errors.ErrUnsupported)
}

// Sign produces a signed request, or errors.ErrUnsupported; it does not write.
func (s *Store) Sign(ctx context.Context, key string, opts SignOptions) (SignedRequest, error) {
	if opts.Expires <= 0 {
		return SignedRequest{}, OpErr("sign", key, errors.New("expiry must be positive"))
	}
	if opts.Method == http.MethodPut {
		var err error
		ctx, err = s.withKMSKey(ctx, "sign", key)
		if err != nil {
			return SignedRequest{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return SignedRequest{}, OpErr("sign", key, err)
	}
	if b, ok := s.b.(SignBackend); ok {
		return b.Sign(ctx, key, opts)
	}
	return SignedRequest{}, OpErr("sign", key, errors.ErrUnsupported)
}

// DeleteFailure names one input key whose deletion was not confirmed.
// Unattempted means no request for that input was sent; otherwise its outcome
// may be ambiguous. Missing objects count as successfully deleted.
type DeleteFailure struct {
	Key         string
	Err         error
	Unattempted bool
}

// DeleteError reports every failed or unattempted input, preserving input
// order (including duplicates). All underlying errors participate in Is/As.
type DeleteError struct {
	Failures []DeleteFailure
}

func (e *DeleteError) Error() string {
	if len(e.Failures) > 0 {
		return fmt.Sprintf("store: delete-many: %d keys not confirmed deleted; %s: %v", len(e.Failures), e.Failures[0].Key, e.Failures[0].Err)
	}
	return fmt.Sprintf("store: delete-many: %d keys not confirmed deleted", len(e.Failures))
}
func (e *DeleteError) Unwrap() []error {
	errs := make([]error, len(e.Failures))
	for i, f := range e.Failures {
		errs[i] = f.Err
	}
	return errs
}

// DeleteFailures reports a common failure for all keys. It returns nil for
// empty input or a nil error; useful to backends and admission wrappers.
func DeleteFailures(keys []string, err error, unattempted bool) error {
	if err == nil || len(keys) == 0 {
		return nil
	}
	e := &DeleteError{Failures: make([]DeleteFailure, len(keys))}
	for i, key := range keys {
		e.Failures[i] = DeleteFailure{Key: key, Err: err, Unattempted: unattempted}
	}
	return e
}
