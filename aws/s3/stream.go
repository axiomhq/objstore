package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/internal/stream"
)

// MaxConditionalUploadSize is S3's single-PUT limit. Conditional uploads
// never switch to multipart: the existing write probe tests PutObject only.
const MaxConditionalUploadSize int64 = 5 << 30

// Sequential parts bound memory and preserve caller-owned SDK retry policy:
// each part is seekable and retryable, while arbitrary single-PUT streams are
// not. A failed multipart upload is aborted best-effort, even after cancellation.
const uploadPartSize = 8 << 20

// MaxUploadSize is the streaming multipart limit: 10,000 fixed 8 MiB parts
// (about 78 GiB). Known larger sizes fail before a request; unknown streams
// fail and abort when they exceed this limit.
const MaxUploadSize int64 = 10000 * uploadPartSize

func (s *Backend) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	o, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return objstore.ObjectInfo{}, opErr("stat", key, err)
	}
	storageClass := string(o.StorageClass)
	if storageClass == "" {
		storageClass = string(types.StorageClassStandard) // S3 omits this header for STANDARD.
	}
	return objstore.ObjectInfo{Size: aws.ToInt64(o.ContentLength), ETag: aws.ToString(o.ETag),
		Metadata: o.Metadata, ContentType: aws.ToString(o.ContentType), CacheControl: aws.ToString(o.CacheControl),
		StorageClass: storageClass, LastModified: aws.ToTime(o.LastModified)}, nil
}

func (s *Backend) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := stream.Range(offset, length); err != nil {
		return nil, opErr("new-reader", key, err)
	}
	in := &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if offset != 0 || length != 0 {
		span := fmt.Sprintf("bytes=%d-", offset)
		if length > 0 {
			span = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		}
		in.Range = &span
	}
	o, err := s.client.GetObject(ctx, in)
	if apiErrorCode(err) == "InvalidRange" {
		// 416 proves no body exists for this range, not whether the key
		// exists. HEAD also covers an empty object and preserves 404/403.
		info, herr := s.Stat(ctx, key)
		if herr != nil {
			return nil, herr
		}
		if offset >= info.Size {
			return io.NopCloser(strings.NewReader("")), nil
		}
	}
	if err != nil {
		return nil, opErr("new-reader", key, err)
	}
	if in.Range != nil {
		var first, last, total int64
		n, _ := fmt.Sscanf(aws.ToString(o.ContentRange), "bytes %d-%d/%d", &first, &last, &total)
		want := total - offset
		if length > 0 {
			want = min(want, length)
		}
		if n != 3 || first != offset || last < first || total <= last ||
			o.ContentLength == nil || *o.ContentLength != last-first+1 || last-first+1 != want {
			o.Body.Close()
			return nil, opErr("new-reader", key, objstore.ErrRange)
		}
	}
	return stream.Download(ctx, o.Body, o.ContentLength), nil
}

func (s *Backend) uploadInput(ctx context.Context, key string, opts objstore.UploadOptions) *awss3.PutObjectInput {
	in := s.putInput(ctx, key, nil)
	in.ContentLength = opts.Size
	in.Metadata = opts.Metadata
	if opts.ContentType != "" {
		in.ContentType = &opts.ContentType
	}
	if opts.CacheControl != "" {
		in.CacheControl = &opts.CacheControl
	}
	in.StorageClass = types.StorageClass(opts.StorageClass)
	return in
}

func streamPutOptions(o *awss3.Options) {
	o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
}

func (s *Backend) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	if err := stream.HTTPOptions(opts, false); err != nil {
		return objstore.ObjectInfo{}, opErr("upload", key, err)
	}
	if opts.Size != nil && *opts.Size > MaxUploadSize {
		return objstore.ObjectInfo{}, opErr("upload", key, fmt.Errorf("size exceeds %d: %w", MaxUploadSize, errors.ErrUnsupported))
	}
	r, err := stream.New(ctx, body, opts)
	if err != nil {
		return objstore.ObjectInfo{}, opErr("upload", key, err)
	}
	in := s.uploadInput(ctx, key, opts)
	if opts.Size != nil && *opts.Size <= uploadPartSize {
		in.Body = r
		if *opts.Size == 0 {
			in.Body = bytes.NewReader(nil)
		}
		o, err := s.client.PutObject(ctx, in, streamPutOptions)
		if err != nil {
			return objstore.ObjectInfo{}, opErr("upload", key, err)
		}
		return objstore.ObjectInfo{Size: r.N, ETag: aws.ToString(o.ETag)}, nil
	}
	info, err := s.multipart(ctx, in, r)
	return info, opErr("upload", key, err)
}

func (s *Backend) multipart(ctx context.Context, in *awss3.PutObjectInput, r *stream.Reader) (objstore.ObjectInfo, error) {
	created, err := s.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: in.Bucket, Key: in.Key, Metadata: in.Metadata, ContentType: in.ContentType,
		CacheControl: in.CacheControl, StorageClass: in.StorageClass,
		ServerSideEncryption: in.ServerSideEncryption, SSEKMSKeyId: in.SSEKMSKeyId,
	})
	if err != nil {
		return objstore.ObjectInfo{}, err
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			_, _ = s.client.AbortMultipartUpload(cleanup, &awss3.AbortMultipartUploadInput{
				Bucket: in.Bucket, Key: in.Key, UploadId: created.UploadId,
			})
		}
	}()
	buf := make([]byte, uploadPartSize)
	parts := []types.CompletedPart{}
	for number := int32(1); ; number++ {
		n, err := io.ReadFull(r, buf)
		if r.Err != nil {
			return objstore.ObjectInfo{}, r.Err
		}
		last := err == io.EOF || err == io.ErrUnexpectedEOF
		if err != nil && !last {
			return objstore.ObjectInfo{}, err
		}
		if last && r.Size != nil && r.N != *r.Size {
			return objstore.ObjectInfo{}, io.ErrUnexpectedEOF
		}
		if n == 0 && number > 1 {
			break
		}
		if number > 10000 {
			return objstore.ObjectInfo{}, errors.New("multipart exceeds 10000 parts")
		}
		o, err := s.client.UploadPart(ctx, &awss3.UploadPartInput{Bucket: in.Bucket, Key: in.Key,
			UploadId: created.UploadId, PartNumber: &number, Body: bytes.NewReader(buf[:n]), ContentLength: aws.Int64(int64(n))})
		if err != nil {
			return objstore.ObjectInfo{}, err
		}
		parts = append(parts, types.CompletedPart{ETag: o.ETag, PartNumber: aws.Int32(number)})
		if last {
			break
		}
	}
	o, err := s.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket: in.Bucket, Key: in.Key, UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return objstore.ObjectInfo{}, err
	}
	committed = true
	return objstore.ObjectInfo{Size: r.N, ETag: aws.ToString(o.ETag)}, nil
}

func (s *Backend) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return s.conditionalUpload(ctx, key, body, "", true, opts)
}

func (s *Backend) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return s.conditionalUpload(ctx, key, body, etag, false, opts)
}

func (s *Backend) conditionalUpload(ctx context.Context, key string, body io.Reader, etag string, absent bool, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	const op = "conditional-upload"
	if err := stream.HTTPOptions(opts, false); err != nil {
		return objstore.ObjectInfo{}, false, opErr(op, key, err)
	}
	if opts.Size == nil || *opts.Size > MaxConditionalUploadSize {
		return objstore.ObjectInfo{}, false, opErr(op, key, fmt.Errorf("known size <= %d required: %w", MaxConditionalUploadSize, errors.ErrUnsupported))
	}
	r, err := stream.New(ctx, body, opts)
	if err != nil {
		return objstore.ObjectInfo{}, false, opErr(op, key, err)
	}
	if !absent && etag == "" {
		return objstore.ObjectInfo{}, false, nil
	}
	in := s.uploadInput(ctx, key, opts)
	in.Body = r
	if *opts.Size == 0 {
		in.Body = bytes.NewReader(nil)
	}
	if absent {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(strings.Trim(etag, `"`))
	}
	o, err := s.client.PutObject(ctx, in, streamPutOptions)
	if err != nil {
		switch apiErrorCode(err) {
		case "PreconditionFailed", "NoSuchKey":
			return objstore.ObjectInfo{}, false, nil
		case "ConditionalRequestConflict":
			err = fmt.Errorf("%w: %w", objstore.ErrConflict, err)
		}
		return objstore.ObjectInfo{}, false, opErr(op, key, err)
	}
	return objstore.ObjectInfo{Size: r.N, ETag: aws.ToString(o.ETag)}, true, nil
}

// Sign supports GET/HEAD/DELETE without custom headers, and PUT with content
// type, cache control and x-amz-meta-* headers. KMS/SSE comes from backend/context.
func (s *Backend) Sign(ctx context.Context, key string, opts objstore.SignOptions) (objstore.SignedRequest, error) {
	if opts.Expires <= 0 || opts.Expires > 7*24*time.Hour {
		return objstore.SignedRequest{}, opErr("sign", key, errors.New("expiry must be in (0, 7 days]"))
	}
	in := s.putInput(ctx, key, nil)
	in.Body = nil
	in.Metadata = map[string]string{}
	for k, vs := range opts.Headers {
		if opts.Method != http.MethodPut || len(vs) != 1 {
			return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
		}
		switch lower := strings.ToLower(k); {
		case lower == "content-type":
			in.ContentType = aws.String(vs[0])
		case lower == "cache-control":
			in.CacheControl = aws.String(vs[0])
		case strings.HasPrefix(lower, "x-amz-meta-"):
			in.Metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = vs[0]
		default:
			return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
		}
	}
	p := awss3.NewPresignClient(s.client, func(o *awss3.PresignOptions) { o.Expires = opts.Expires })
	var req *v4.PresignedHTTPRequest
	var err error
	switch opts.Method {
	case http.MethodPut:
		req, err = p.PresignPutObject(ctx, in, awss3.WithPresignClientFromClientOptions(func(o *awss3.Options) {
			// A body-independent presign has length zero. The SDK otherwise
			// deletes even explicitly requested Content-Type before signing.
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				_, err := stack.Build.Remove("RemoveContentTypeHeader")
				return err
			})
		}))
	case http.MethodGet:
		req, err = p.PresignGetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	case http.MethodHead:
		req, err = p.PresignHeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	case http.MethodDelete:
		req, err = p.PresignDeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	default:
		err = errors.ErrUnsupported
	}
	if err != nil {
		return objstore.SignedRequest{}, opErr("sign", key, err)
	}
	return objstore.SignedRequest{URL: req.URL, Method: req.Method, Headers: req.SignedHeader}, nil
}
