// Package s3 is the S3 backend for objstore. Cloudflare R2, MinIO, Ceph and
// Hetzner use it too because they speak the S3 API. A write whose context
// carries objstore.WithKMSKey is stored with SSE-KMS under that key.
package s3

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/axiomhq/objstore"
)

// Backend is the S3 backend: AWS S3, MinIO, Ceph RGW (Hetzner), R2.
type Backend struct {
	client   *awss3.Client
	bucket   string
	endpoint string
	sse      string
	kmsKeyID string
}

// httpTimeout bounds one request end to end; a hung store never pins a
// goroutine past it.
const httpTimeout = 60 * time.Second

// idleConnsPerHost is the warm connection pool to one endpoint; above a
// typical caller's request gate width (32) so a full burst reuses connections.
const idleConnsPerHost = 64

// Config selects the bucket and the S3 endpoint.
type Config struct {
	// Endpoint is "" for AWS S3 (the SDK's region), or an http(s) URL for
	// an S3-compatible service (MinIO, Ceph, Hetzner, R2), addressed
	// path-style.
	Endpoint string
	// Bucket is the bucket every key lives in.
	Bucket string
	// AWS, when non-nil, replaces the SDK's default config loading. It is
	// copied; credentials, region and HTTP client are used as supplied.
	// A nil HTTP client uses this backend's bounded, pooled default.
	AWS *aws.Config
	// AllowedEndpoints, when set, lists the only endpoints (scheme://host)
	// New accepts; any other is ErrEndpointDenied. With a list set, the
	// empty Endpoint (AWS's default) and anything not parsing as
	// scheme://host (file://, a bare path) are denied too.
	AllowedEndpoints []string
	// SSE is the server-side encryption mode: "", "AES256" or "aws:kms".
	SSE string
	// KMSKeyID is the KMS key for SSE "aws:kms"; any other mode rejects it.
	KMSKeyID string
	// RequestTimeout bounds one S3 request end to end when AWS.HTTPClient is
	// not supplied; 0 = 60 s. A store whose queue is deeper than that
	// truncates large uploads mid-body
	// (MinIO answers 400 IncompleteBody) rather than finishing them.
	RequestTimeout time.Duration
}

// ErrEndpointDenied is New's error for an Endpoint outside
// Config.AllowedEndpoints.
var ErrEndpointDenied = errors.New("store: endpoint denied by allow-list")

func endpointAllowed(endpoint string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	u.Path, u.RawQuery, u.Fragment = "", "", ""
	return slices.Contains(allowed, u.String())
}

// Open returns a Store over New(ctx, cfg) with ocfg's pacing and write bound.
func Open(ctx context.Context, cfg Config, ocfg objstore.Config) (*objstore.Store, error) {
	b, err := New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return objstore.Open(b, ocfg), nil
}

// New connects to cfg.Bucket. Unless cfg.AWS is supplied, credentials and
// region come from the SDK's default chain (environment, shared config,
// instance role).
func New(ctx context.Context, cfg Config) (*Backend, error) {
	endpoint, bucket, sse, kmsKeyID, timeout := cfg.Endpoint, cfg.Bucket, cfg.SSE, cfg.KMSKeyID, cfg.RequestTimeout
	if !endpointAllowed(endpoint, cfg.AllowedEndpoints) {
		return nil, ErrEndpointDenied
	}
	if timeout <= 0 {
		timeout = httpTimeout
	}
	if sse != "" && sse != "AES256" && sse != "aws:kms" {
		return nil, fmt.Errorf("store: invalid SSE mode %q", sse)
	}
	if kmsKeyID != "" && sse != "aws:kms" {
		return nil, errors.New("store: KMS key id requires aws:kms SSE mode")
	}
	// Keep at least a gate's worth of connections warm: the SDK default of
	// 10 idle connections per host makes a 32-wide prefetch burst churn TLS
	// handshakes, and a lease renewal queued behind them missed its 2.5 s
	// budget three times running and fenced the namespace (1M, Hetzner).
	httpClient := awshttp.NewBuildableClient().WithTimeout(timeout).WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = idleConnsPerHost
		t.MaxIdleConns = idleConnsPerHost
	})
	var awsCfg aws.Config
	if cfg.AWS != nil {
		awsCfg = *cfg.AWS
		if awsCfg.HTTPClient == nil {
			awsCfg.HTTPClient = httpClient
		}
	} else {
		var err error
		awsCfg, err = config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
		if err != nil {
			return nil, err
		}
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
		// Stores that do not return a checksum (Ceph RGW among them) would
		// otherwise log a warning on every GET.
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Backend{client: client, bucket: bucket, endpoint: endpoint, sse: sse, kmsKeyID: kmsKeyID}, nil
}

// ID is the endpoint and bucket, or s3://bucket on AWS's default endpoint.
func (s *Backend) ID() string { return cmp.Or(s.endpoint, "s3:/") + "/" + s.bucket }

// SSE reports the bucket's configured server-side encryption: the S3 mode
// ("", "AES256" or "aws:kms") and the KMS key id when one is set. The caller
// decides what it means for its users; the store does not interpret it.
func (s *Backend) SSE() (mode, kmsKeyID string) { return s.sse, s.kmsKeyID }

func (s *Backend) putInput(ctx context.Context, key string, data []byte) *awss3.PutObjectInput {
	in := &awss3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)}
	if s.sse != "" {
		in.ServerSideEncryption = types.ServerSideEncryption(s.sse)
	}
	if s.kmsKeyID != "" {
		in.SSEKMSKeyId = &s.kmsKeyID
	}
	if id := objstore.KMSKey(ctx); id != "" {
		in.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		in.SSEKMSKeyId = &id
	}
	return in
}

// SupportsKMS: S3 applies a per-object KMS key (objstore.Store.KMS).
func (s *Backend) SupportsKMS() bool { return true }

// opErr is objstore.OpErr, adding objstore.ErrAccessDenied for a 403 or an
// SSE-KMS failure S3 reports under a KMS.* code (a disabled,
// pending-deletion or unreachable key), ErrNotFound for a missing bucket
// or a missing object on reads, and ErrRange for an invalid read range.
func opErr(op, key string, err error) error {
	if err == nil {
		return nil
	}
	switch op {
	case "get", "get-with-etag", "get-if-changed", "get-range":
		if code := apiErrorCode(err); code == "NoSuchKey" {
			err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
		} else if op == "get-range" && code == "InvalidRange" {
			err = fmt.Errorf("%w: %w", objstore.ErrRange, err)
		}
	}
	if apiErrorCode(err) == "NoSuchBucket" && !errors.Is(err, objstore.ErrNotFound) {
		err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusForbidden || strings.HasPrefix(apiErrorCode(err), "KMS.") {
		err = fmt.Errorf("%w: %w", objstore.ErrAccessDenied, err)
	}
	return objstore.OpErr(op, key, err)
}

// readBody drains an object body and proves it arrived whole. A GET can end
// early — a truncated connection reads back as a short, error-free body —
// and every decoder downstream would then be parsing a prefix: a short
// pack.bin is "not a pack", a short vec block silently loses its trailing
// vectors. ContentLength is the server's own count of what it sent, so the
// comparison is free and turns a silent truncation into a named error.
func readBody(op, key string, body io.ReadCloser, contentLength *int64) ([]byte, error) {
	defer body.Close()
	if contentLength != nil && *contentLength >= 0 && *contentLength <= 64<<20 {
		// One exact-size buffer, as fs.go does: bytes.Buffer's MinRead slack
		// stayed on every returned slice, and caches holding small blocks
		// pinned 512 spare bytes each. Cap speculative allocation: a bogus
		// ContentLength must not cause an enormous make.
		data := make([]byte, *contentLength)
		n, err := io.ReadFull(body, data)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, objstore.OpErr(op, key, fmt.Errorf("short read: got %d bytes, ContentLength says %d", n, *contentLength))
		}
		if err != nil {
			return nil, objstore.OpErr(op, key, err)
		}
		// Read on to EOF: the SDK validates its checksum there, and a body
		// longer than ContentLength is as wrong as a short one.
		var extra [1]byte
		if _, err := io.ReadFull(body, extra[:]); err == nil {
			return nil, objstore.OpErr(op, key, fmt.Errorf("long read: body exceeds ContentLength %d", *contentLength))
		} else if !errors.Is(err, io.EOF) {
			return nil, objstore.OpErr(op, key, err)
		}
		return data, nil
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, objstore.OpErr(op, key, err)
	}
	if contentLength != nil && int64(len(data)) != *contentLength {
		return nil, objstore.OpErr(op, key, fmt.Errorf("short read: got %d bytes, ContentLength says %d",
			len(data), *contentLength))
	}
	return data, nil
}

// Put writes key unconditionally.
func (s *Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.putInput(ctx, key, data))
	return opErr("put", key, err)
}

// PutIfAbsent writes key only if it does not already exist (If-None-Match: *).
// Returns false when the key existed. This is the OCC primitive the WAL builds on.
// Only PreconditionFailed is a verdict. ConditionalRequestConflict (409) is
// "another conditional write on this key is in flight, retry" — the object
// may or may not exist — so it surfaces as an error wrapping
// objstore.ErrConflict and the caller's retry loop handles it, never as
// "another writer won".
func (s *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	in := s.putInput(ctx, key, data)
	in.IfNoneMatch = aws.String("*")
	_, err := s.client.PutObject(ctx, in)
	if err != nil {
		switch apiErrorCode(err) {
		case "PreconditionFailed":
			return false, nil
		case "ConditionalRequestConflict":
			return false, opErr("put-if-absent", key, fmt.Errorf("%w: %w", objstore.ErrConflict, err))
		}
		return false, opErr("put-if-absent", key, err)
	}
	return true, nil
}

// apiErrorCode is the S3 error code err carries ("NoSuchKey", ...), or "".
func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// Get reads the whole object at key.
func (s *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, opErr("get", key, err)
	}
	return readBody("get", key, out.Body, out.ContentLength)
}

// ListPage is one ListObjectsV2 request with StartAfter = after.
func (s *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	in := &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, MaxKeys: aws.Int32(int32(limit))}
	if after != "" {
		in.StartAfter = &after
	}
	page, err := s.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, "", opErr("list-page", prefix, err)
	}
	keys := make([]string, 0, len(page.Contents))
	for _, obj := range page.Contents {
		keys = append(keys, *obj.Key)
	}
	if page.IsTruncated != nil && *page.IsTruncated && len(keys) > 0 {
		return keys, keys[len(keys)-1], nil
	}
	return keys, "", nil
}

// ListPrefixesPage asks S3 for the common prefixes one level under prefix: with
// Delimiter "/" the server collapses everything below each child prefix, so
// discovery costs one request per 1000 namespaces instead of one key per
// object in the bucket. MaxKeys counts objects AT the prefix level too, so
// a truncated page can carry no prefixes at all; that is not the end of the
// listing, so keep walking the server's continuation until a prefix appears
// or the listing ends. The public cursor stays the last prefix returned.
func (s *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	in := &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, Delimiter: aws.String("/"), MaxKeys: aws.Int32(int32(limit))}
	if after != "" {
		in.StartAfter = &after
	}
	for {
		page, err := s.client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, "", opErr("list-prefixes-page", prefix, err)
		}
		truncated := page.IsTruncated != nil && *page.IsTruncated
		if len(page.CommonPrefixes) == 0 && truncated && page.NextContinuationToken != nil {
			in.ContinuationToken = page.NextContinuationToken
			continue
		}
		out := make([]string, 0, len(page.CommonPrefixes))
		for _, cp := range page.CommonPrefixes {
			out = append(out, *cp.Prefix)
		}
		if truncated && len(out) > 0 {
			return out, out[len(out)-1], nil
		}
		return out, "", nil
	}
}

// Delete removes key. A missing key is not an error.
func (s *Backend) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	return opErr("delete", key, err)
}

// deleteBatch is the most keys one DeleteObjects request may carry.
const deleteBatch = 1000

// DeleteMany removes keys. Missing keys are not an error. Empty input is a no-op.
// A batch DELETE answers 200 even when individual keys failed; Quiet mode only
// suppresses the successes, so Errors is the real verdict per key.
func (s *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	objs := make([]types.ObjectIdentifier, len(keys))
	for i, k := range keys {
		objs[i] = types.ObjectIdentifier{Key: &k}
	}
	for i := 0; i < len(objs); i += deleteBatch {
		end := min(i+deleteBatch, len(objs))
		out, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &types.Delete{Objects: objs[i:end], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return opErr("delete-many", keys[i], err)
		}
		if len(out.Errors) > 0 {
			e := out.Errors[0]
			return opErr("delete-many", aws.ToString(e.Key), fmt.Errorf("%s: %s (%d keys failed)", aws.ToString(e.Code), aws.ToString(e.Message), len(out.Errors)))
		}
	}
	return nil
}

// EnsureBucket creates the bucket if missing (dev/test convenience).
func (s *Backend) EnsureBucket(ctx context.Context) error {
	// Reuse an existing bucket without CreateBucket permission or an AWS
	// location constraint. Custom endpoints also include regional AWS S3.
	_, err := s.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: &s.bucket})
	if err == nil {
		return nil
	}
	if code := apiErrorCode(err); code != "NotFound" && code != "NoSuchBucket" {
		return opErr("head-bucket", s.bucket, err)
	}
	in := &awss3.CreateBucketInput{Bucket: &s.bucket}
	opts := s.client.Options()
	if aws.ToString(opts.BaseEndpoint) == "" && opts.Region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(opts.Region)}
	}
	_, err = s.client.CreateBucket(ctx, in)
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return opErr("create-bucket", s.bucket, err)
}

// DropBucket empties the bucket (List + DeleteMany, both already bounded
// by the client timeout) and deletes it. A bucket that is already gone
// (NoSuchBucket on either step) is not an error.
func (s *Backend) DropBucket(ctx context.Context) error {
	for {
		keys, _, err := s.ListPage(ctx, "", "", objstore.MaxListPage)
		if apiErrorCode(err) == "NoSuchBucket" {
			return nil
		}
		if err != nil {
			return opErr("drop-bucket", s.bucket, err)
		}
		if len(keys) == 0 {
			break
		}
		if err := s.DeleteMany(ctx, keys...); err != nil {
			return opErr("drop-bucket", s.bucket, err)
		}
	}
	_, err := s.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: &s.bucket})
	if apiErrorCode(err) == "NoSuchBucket" {
		return nil
	}
	return opErr("drop-bucket", s.bucket, err)
}

// GetWithETag returns the object and its ETag for conditional replacement.
func (s *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, "", opErr("get-with-etag", key, err)
	}
	data, err := readBody("get-with-etag", key, out.Body, out.ContentLength)
	if err != nil {
		return nil, "", err
	}
	return data, aws.ToString(out.ETag), nil
}

// GetIfChanged uses a single conditional request, including the 304 path.
func (s *Backend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	in := &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if etag != "" {
		in.IfNoneMatch = &etag
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		var response *awshttp.ResponseError
		if etag != "" && errors.As(err, &response) && response.HTTPStatusCode() == http.StatusNotModified {
			return nil, etag, true, nil
		}
		return nil, "", false, opErr("get-if-changed", key, err)
	}
	data, err := readBody("get-if-changed", key, out.Body, out.ContentLength)
	if err != nil {
		return nil, "", false, err
	}
	return data, aws.ToString(out.ETag), false, nil
}

// PutIfMatch replaces key only if its current ETag equals etag — compare-and-
// swap, the manifest-swap primitive. (false, nil) = precondition failed: the
// object changed under us, or no longer exists. ConditionalRequestConflict
// is an error wrapping objstore.ErrConflict, as in PutIfAbsent: the CAS
// was never evaluated.
//
// The ETag is sent without its surrounding quotes. S3 returns ETags quoted
// and accepts either form in If-Match, but Ceph RGW (Hetzner Object Storage)
// compares the quoted header literally against the bare stored value and
// answers 412 for every conditional PUT, which would make manifest CAS
// publication impossible there. Cloudflare R2 documents If-Match per RFC
// 9110 (quoted); whether it accepts the unquoted form is unverified, and
// TestR2 is the check. TestS3PutIfMatchSendsUnquotedETag pins the header.
func (s *Backend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	in := s.putInput(ctx, key, data)
	in.IfMatch = aws.String(strings.Trim(etag, `"`))
	_, err := s.client.PutObject(ctx, in)
	if err != nil {
		switch apiErrorCode(err) {
		case "PreconditionFailed", "NoSuchKey":
			return false, nil
		case "ConditionalRequestConflict":
			return false, opErr("put-if-match", key, fmt.Errorf("%w: %w", objstore.ErrConflict, err))
		}
		return false, opErr("put-if-match", key, err)
	}
	return true, nil
}

// GetRange reads exactly length bytes of key at offset. The response's
// Content-Range and Content-Length must match the request: a server that
// ignores Range is ErrRange, not a whole-object download.
func (s *Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	span := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &span})
	if err != nil {
		return nil, opErr("get-range", key, err)
	}
	// Check both the returned range and length: a server ignoring Range must
	// not turn a block read into an unbounded whole-object download.
	var first, last, total int64
	if out.ContentRange == nil || out.ContentLength == nil || *out.ContentLength != length {
		out.Body.Close()
		return nil, opErr("get-range", key, objstore.ErrRange)
	}
	if n, _ := fmt.Sscanf(*out.ContentRange, "bytes %d-%d/%d", &first, &last, &total); n != 3 || first != offset || last != offset+length-1 || total <= last {
		out.Body.Close()
		return nil, opErr("get-range", key, objstore.ErrRange)
	}
	return readBody("get-range", key, out.Body, out.ContentLength)
}
