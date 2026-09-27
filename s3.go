package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// s3Store is the S3 / MinIO backend.
type s3Store struct {
	client   *s3.Client
	bucket   string
	sse      string
	kmsKeyID string
	pace     *pacer
}

// pacer is a token bucket with a one-request burst: a 32-wide prefetch
// never exceeds the configured rate, and slots go out in call order so a
// lease renewal never starves behind bulk reads. nil means unpaced.
type pacer struct{ l *rate.Limiter }

func newPacer(rps float64) *pacer {
	if rps <= 0 {
		return nil
	}
	return &pacer{l: rate.NewLimiter(rate.Limit(rps), 1)}
}

func (p *pacer) wait(ctx context.Context) error {
	if p == nil || isUrgent(ctx) {
		return nil
	}
	return p.l.Wait(ctx)
}

// httpTimeout bounds one request end to end; a hung store never pins a
// goroutine past it.
const httpTimeout = 60 * time.Second

// idleConnsPerHost is the warm connection pool to one endpoint; above a
// typical caller's request gate width (32) so a full burst reuses connections.
const idleConnsPerHost = 64

func newS3(ctx context.Context, endpoint, bucket, sse, kmsKeyID string, rps float64, timeout time.Duration) (*s3Store, error) {
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
	cfg, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
		// Stores that do not return a checksum (Ceph RGW among them) would
		// otherwise log a warning on every GET.
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &s3Store{client: client, bucket: bucket, sse: sse, kmsKeyID: kmsKeyID, pace: newPacer(rps)}, nil
}

func (s *s3Store) putInput(key string, data []byte) *s3.PutObjectInput {
	in := &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)}
	if s.sse != "" {
		in.ServerSideEncryption = types.ServerSideEncryption(s.sse)
	}
	if s.kmsKeyID != "" {
		in.SSEKMSKeyId = &s.kmsKeyID
	}
	return in
}

// opErr attaches the operation and the key to every error leaving this
// backend. A bare SDK error names neither, so a failure surfaced three
// layers up — mid-compaction, mid-replay, inside a query's fan-out — says
// only that S3 was unhappy about something. %w keeps ErrNotFound
// unwrappable by errors.Is.
func opErr(op, key string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("store: %s %s: %w", op, key, err)
}

// readBody drains an object body and proves it arrived whole. A GET can end
// early — a truncated connection reads back as a short, error-free body —
// and every decoder downstream would then be parsing a prefix: a short
// pack.bin is "not a pack", a short vec block silently loses its trailing
// vectors. ContentLength is the server's own count of what it sent, so the
// comparison is free and turns a silent truncation into a named error.
func readBody(op, key string, body io.ReadCloser, contentLength *int64) ([]byte, error) {
	defer body.Close()
	var data []byte
	var err error
	if contentLength != nil && *contentLength > 0 && *contentLength <= 64<<20 {
		// Avoid repeated buffer growth for pack ranges. Cap speculative
		// allocation: a bogus ContentLength must not cause an enormous make.
		// ReadFrom needs MinRead slack for its final EOF/checksum read, even
		// when the body exactly matches the advertised length.
		var buf bytes.Buffer
		buf.Grow(int(*contentLength) + bytes.MinRead)
		_, err = buf.ReadFrom(body)
		data = buf.Bytes()
	} else {
		data, err = io.ReadAll(body)
	}
	if err != nil {
		return nil, opErr(op, key, err)
	}
	if contentLength != nil && int64(len(data)) != *contentLength {
		return nil, opErr(op, key, fmt.Errorf("short read: got %d bytes, ContentLength says %d",
			len(data), *contentLength))
	}
	return data, nil
}

func (s *s3Store) Put(ctx context.Context, key string, data []byte) error {
	if err := s.pace.wait(ctx); err != nil {
		return err
	}
	_, err := s.client.PutObject(ctx, s.putInput(key, data))
	return opErr("put", key, err)
}

// PutIfAbsent writes key only if it does not already exist (If-None-Match: *).
// Returns false when the key existed. This is the OCC primitive the WAL builds on.
// Only PreconditionFailed is a verdict. ConditionalRequestConflict (409) is
// "another conditional write on this key is in flight, retry" — the object
// may or may not exist — so it surfaces as an error and the caller's retry
// loop handles it, never as "another writer won".
func (s *s3Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if err := s.pace.wait(ctx); err != nil {
		return false, err
	}
	in := s.putInput(key, data)
	in.IfNoneMatch = aws.String("*")
	_, err := s.client.PutObject(ctx, in)
	if err != nil {
		if apiErrorCode(err) == "PreconditionFailed" {
			return false, nil
		}
		return false, opErr("put-if-absent", key, err)
	}
	return true, nil
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func (s *s3Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if apiErrorCode(err) == "NoSuchKey" {
			return nil, opErr("get", key, ErrNotFound)
		}
		return nil, opErr("get", key, err)
	}
	return readBody("get", key, out.Body, out.ContentLength)
}

func (s *s3Store) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	in := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, MaxKeys: aws.Int32(int32(limit))}
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

// ListPrefixes asks S3 for the common prefixes one level under prefix: with
// Delimiter "/" the server collapses everything below each child prefix, so
// discovery costs one request per 1000 namespaces instead of one key per
// object in the bucket. MaxKeys counts objects AT the prefix level too, so
// a truncated page can carry no prefixes at all; that is not the end of the
// listing, so keep walking the server's continuation until a prefix appears
// or the listing ends. The public cursor stays the last prefix returned.
func (s *s3Store) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	in := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, Delimiter: aws.String("/"), MaxKeys: aws.Int32(int32(limit))}
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

func (s *s3Store) Delete(ctx context.Context, key string) error {
	if err := s.pace.wait(ctx); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	return opErr("delete", key, err)
}

// DeleteMany removes keys. Missing keys are not an error. Empty input is a no-op.
// A batch DELETE answers 200 even when individual keys failed; Quiet mode only
// suppresses the successes, so Errors is the real verdict per key.
func (s *s3Store) DeleteMany(ctx context.Context, keys ...string) error {
	if err := s.pace.wait(ctx); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	objs := make([]types.ObjectIdentifier, len(keys))
	for i, k := range keys {
		k := k
		objs[i] = types.ObjectIdentifier{Key: &k}
	}
	const batch = 1000
	for i := 0; i < len(objs); i += batch {
		end := min(i+batch, len(objs))
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
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
func (s *s3Store) EnsureBucket(ctx context.Context) error {
	// Reuse an existing bucket without CreateBucket permission or an AWS
	// location constraint. Custom endpoints also include regional AWS S3.
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.bucket})
	if err == nil {
		return nil
	}
	if code := apiErrorCode(err); code != "NotFound" && code != "NoSuchBucket" {
		return opErr("head-bucket", s.bucket, err)
	}
	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return opErr("create-bucket", s.bucket, err)
}

// DropBucket empties the bucket (List + DeleteMany, both already bounded
// by the client timeout) and deletes it. A bucket that is already gone is
// not an error.
func (s *s3Store) DropBucket(ctx context.Context) error {
	for {
		keys, _, err := s.ListPage(ctx, "", "", 1000)
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
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &s.bucket})
	var nsb *types.NoSuchBucket
	if errors.As(err, &nsb) {
		return nil
	}
	return opErr("drop-bucket", s.bucket, err)
}

// GetWithETag returns the object and its ETag for conditional replacement.
func (s *s3Store) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if apiErrorCode(err) == "NoSuchKey" {
			return nil, "", opErr("get-with-etag", key, ErrNotFound)
		}
		return nil, "", opErr("get-with-etag", key, err)
	}
	data, err := readBody("get-with-etag", key, out.Body, out.ContentLength)
	if err != nil {
		return nil, "", err
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return data, etag, nil
}

// GetIfChanged uses a single conditional request, including the 304 path.
func (s *s3Store) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, "", false, err
	}
	in := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if etag != "" {
		in.IfNoneMatch = &etag
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		var response *awshttp.ResponseError
		if etag != "" && errors.As(err, &response) && response.HTTPStatusCode() == http.StatusNotModified {
			return nil, etag, true, nil
		}
		if apiErrorCode(err) == "NoSuchKey" {
			err = ErrNotFound
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
// is an error, as in PutIfAbsent: the CAS was never evaluated.
//
// The ETag is sent without its surrounding quotes. S3 returns ETags quoted
// and accepts either form in If-Match, but Ceph RGW (Hetzner Object Storage)
// compares the quoted header literally against the bare stored value and
// answers 412 for every conditional PUT, which would make manifest CAS
// publication impossible there.
func (s *s3Store) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	if err := s.pace.wait(ctx); err != nil {
		return false, err
	}
	in := s.putInput(key, data)
	in.IfMatch = aws.String(strings.Trim(etag, `"`))
	_, err := s.client.PutObject(ctx, in)
	if err != nil {
		if code := apiErrorCode(err); code == "PreconditionFailed" || code == "NoSuchKey" {
			return false, nil
		}
		return false, opErr("put-if-match", key, err)
	}
	return true, nil
}

func (s *s3Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.pace.wait(ctx); err != nil {
		return nil, err
	}
	span := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &span})
	if err != nil {
		switch apiErrorCode(err) {
		case "NoSuchKey":
			err = ErrNotFound
		case "InvalidRange":
			err = ErrRange
		}
		return nil, opErr("get-range", key, err)
	}
	// Check both the returned range and length: a server ignoring Range must
	// not turn a block read into an unbounded whole-object download.
	var first, last, total int64
	if out.ContentRange == nil || out.ContentLength == nil || *out.ContentLength != length {
		out.Body.Close()
		return nil, opErr("get-range", key, ErrRange)
	}
	if n, _ := fmt.Sscanf(*out.ContentRange, "bytes %d-%d/%d", &first, &last, &total); n != 3 || first != offset || last != offset+length-1 || total <= last {
		out.Body.Close()
		return nil, opErr("get-range", key, ErrRange)
	}
	return readBody("get-range", key, out.Body, out.ContentLength)
}
