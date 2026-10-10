// Package blob implements objstore on Azure block blobs. Callers construct
// the Azure container client, retaining credentials, retry and transport policy.
package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	azblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/internal/stream"
)

// Config supplies a caller-owned SDK client. Construct it with token/shared-key
// credentials, a SAS URL, or a connection string using the Azure SDK. This
// backend never closes its transport. Shared-key credentials enable signing.
type Config struct{ Client *container.Client }

type Backend struct{ client *container.Client }

var _ objstore.Backend = (*Backend)(nil)

func New(cfg Config) (*Backend, error) {
	if cfg.Client == nil {
		return nil, errors.New("azure/blob: nil container client")
	}
	return &Backend{client: cfg.Client}, nil
}

func Open(cfg Config, ocfg objstore.Config) (*objstore.Store, error) {
	b, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return objstore.Open(b, ocfg), nil
}

// ID excludes query credentials from a SAS-backed client's URL.
func (b *Backend) ID() string {
	u, _ := url.Parse(b.client.URL())
	u.RawQuery, u.Fragment, u.User = "", "", nil
	return u.String()
}

func value[T any](p *T) (v T) {
	if p != nil {
		return *p
	}
	return v
}

func status(err error) int {
	var e *azcore.ResponseError
	if errors.As(err, &e) {
		return e.StatusCode
	}
	return 0
}

func opErr(op, key string, err error) error {
	if err == nil {
		return nil
	}
	switch status(err) {
	case http.StatusNotFound:
		err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	case http.StatusForbidden:
		err = fmt.Errorf("%w: %w", objstore.ErrAccessDenied, err)
	case http.StatusRequestedRangeNotSatisfiable:
		err = fmt.Errorf("%w: %w", objstore.ErrRange, err)
	}
	return objstore.OpErr(op, key, err)
}

func (b *Backend) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return objstore.ObjectInfo{}, opErr("stat", key, err)
	}
	o, err := b.client.NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return objstore.ObjectInfo{}, opErr("stat", key, err)
	}
	metadata := make(map[string]string, len(o.Metadata))
	for k, v := range o.Metadata {
		metadata[strings.ToLower(k)] = value(v)
	}
	return objstore.ObjectInfo{Size: value(o.ContentLength), ETag: string(value(o.ETag)), Metadata: metadata,
		ContentType: value(o.ContentType), CacheControl: value(o.CacheControl), StorageClass: value(o.AccessTier), LastModified: value(o.LastModified)}, nil
}

func (b *Backend) download(ctx context.Context, key string, opts *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
	if err := ctx.Err(); err != nil {
		return azblob.DownloadStreamResponse{}, err
	}
	return b.client.NewBlobClient(key).DownloadStream(ctx, opts)
}

func (b *Backend) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := stream.Range(offset, length); err != nil {
		return nil, opErr("new-reader", key, err)
	}
	o, err := b.download(ctx, key, &azblob.DownloadStreamOptions{Range: azblob.HTTPRange{Offset: offset, Count: length}})
	if status(err) == http.StatusRequestedRangeNotSatisfiable {
		info, herr := b.Stat(ctx, key)
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
	if offset != 0 || length != 0 {
		var first, last, total int64
		n, _ := fmt.Sscanf(value(o.ContentRange), "bytes %d-%d/%d", &first, &last, &total)
		// Azurite returns a zero-length 206 interval at EOF instead of 416.
		if n == 3 && first == offset && first == total && last == first-1 && o.ContentLength != nil && *o.ContentLength == 0 {
			return stream.Download(ctx, o.Body, o.ContentLength), nil
		}
		want := total - offset
		if length > 0 {
			want = min(want, length)
		}
		if n != 3 || first != offset || last < first || total <= last ||
			o.ContentLength == nil || *o.ContentLength != last-first+1 || last-first+1 != want {
			o.Body.Close()
			return nil, opErr("new-reader", key, fmt.Errorf("%w: response %q, length %d", objstore.ErrRange, value(o.ContentRange), value(o.ContentLength)))
		}
	}
	return stream.Download(ctx, o.Body, o.ContentLength), nil
}

func (b *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := b.GetWithETag(ctx, key)
	return data, opErr("get", key, err)
}

func (b *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, tag, _, err := b.GetIfChanged(ctx, key, "")
	return data, tag, opErr("get-with-etag", key, err)
}

func (b *Backend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	opts := &azblob.DownloadStreamOptions{}
	if etag != "" {
		opts.AccessConditions = &azblob.AccessConditions{ModifiedAccessConditions: &azblob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETag(etag))}}
	}
	// The SDK converts 304 into nil error and a nil body, dropping ETag.
	// Capture the wire status rather than mistaking an empty 200 for 304.
	var response *http.Response
	o, err := b.download(policy.WithCaptureResponse(ctx, &response), key, opts)
	if etag != "" && response != nil && response.StatusCode == http.StatusNotModified {
		return nil, etag, true, nil
	}
	if err != nil {
		return nil, "", false, opErr("get-if-changed", key, err)
	}
	defer o.Body.Close()
	data, err := io.ReadAll(stream.Download(ctx, o.Body, o.ContentLength))
	if err != nil {
		return nil, "", false, opErr("get-if-changed", key, err)
	}
	return data, string(value(o.ETag)), false, nil
}

func (b *Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := stream.Range(offset, length); err != nil || length == 0 {
		return nil, opErr("get-range", key, objstore.ErrRange)
	}
	r, err := b.NewReader(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err == nil && int64(len(data)) != length {
		err = objstore.ErrRange
	}
	if err != nil {
		return nil, opErr("get-range", key, err)
	}
	return data, nil
}

func (b *Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := b.Upload(ctx, key, bytes.NewReader(data), objstore.UploadOptions{Size: to.Ptr(int64(len(data)))})
	return opErr("put", key, err)
}

func (b *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	_, ok, err := b.UploadIfAbsent(ctx, key, bytes.NewReader(data), objstore.UploadOptions{Size: to.Ptr(int64(len(data)))})
	return ok, opErr("put-if-absent", key, err)
}

func (b *Backend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	_, ok, err := b.UploadIfMatch(ctx, key, bytes.NewReader(data), etag, objstore.UploadOptions{Size: to.Ptr(int64(len(data)))})
	return ok, opErr("put-if-match", key, err)
}

func (b *Backend) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	info, _, err := b.upload(ctx, key, body, opts, nil)
	return info, err
}

func (b *Backend) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return b.upload(ctx, key, body, opts, &azblob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)})
}

func (b *Backend) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return objstore.ObjectInfo{}, false, opErr("upload-if-match", key, err)
	}
	if etag == "" {
		return objstore.ObjectInfo{}, false, nil
	}
	return b.upload(ctx, key, body, opts, &azblob.ModifiedAccessConditions{IfMatch: to.Ptr(azcore.ETag(etag))})
}

func (b *Backend) upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions, condition *azblob.ModifiedAccessConditions) (objstore.ObjectInfo, bool, error) {
	if err := stream.HTTPOptions(opts, true); err != nil {
		return objstore.ObjectInfo{}, false, opErr("upload", key, err)
	}
	if objstore.KMSKey(ctx) != "" {
		return objstore.ObjectInfo{}, false, opErr("upload", key, errors.ErrUnsupported)
	}
	r, err := stream.New(ctx, body, opts)
	if err != nil {
		return objstore.ObjectInfo{}, false, opErr("upload", key, err)
	}
	metadata := make(map[string]*string, len(opts.Metadata))
	for k, v := range opts.Metadata {
		metadata[k] = to.Ptr(v)
	}
	u := &blockblob.UploadStreamOptions{BlockSize: 8 << 20, Concurrency: 1, Metadata: metadata,
		HTTPHeaders: &azblob.HTTPHeaders{BlobContentType: to.Ptr(opts.ContentType), BlobCacheControl: to.Ptr(opts.CacheControl)}}
	if opts.StorageClass != "" {
		u.AccessTier = to.Ptr(azblob.AccessTier(opts.StorageClass))
	}
	if condition != nil {
		u.AccessConditions = &azblob.AccessConditions{ModifiedAccessConditions: condition}
	}
	o, err := b.client.NewBlockBlobClient(key).UploadStream(ctx, r, u)
	if condition != nil {
		if condition.IfNoneMatch != nil && bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
			return objstore.ObjectInfo{}, false, nil
		}
		if bloberror.HasCode(err, bloberror.ConditionNotMet, bloberror.BlobNotFound) {
			return objstore.ObjectInfo{}, false, nil
		}
		if status(err) == http.StatusConflict {
			err = fmt.Errorf("%w: %w", objstore.ErrConflict, err)
		}
	}
	if err != nil {
		return objstore.ObjectInfo{}, false, opErr("upload", key, err)
	}
	return objstore.ObjectInfo{Size: r.N, ETag: string(value(o.ETag)), LastModified: value(o.LastModified)}, true, nil
}

// ListPage preserves objstore's lexical cursor. Azure's portable list API has
// opaque markers, so a new call scans from the prefix start to after. Memory
// remains page-bounded; deep pagination can require many service requests.
func (b *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > objstore.MaxListPage {
		return nil, "", opErr("list-page", prefix, errors.New("invalid limit"))
	}
	p := b.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix, MaxResults: to.Ptr(int32(objstore.MaxListPage))})
	keys := []string{}
	for p.More() {
		if err := ctx.Err(); err != nil {
			return nil, "", opErr("list-page", prefix, err)
		}
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, "", opErr("list-page", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			key := value(item.Name)
			if key <= after {
				continue
			}
			if len(keys) == limit {
				return keys, keys[len(keys)-1], nil
			}
			keys = append(keys, key)
		}
	}
	return keys, "", nil
}

func (b *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > objstore.MaxListPage {
		return nil, "", opErr("list-prefixes-page", prefix, errors.New("invalid limit"))
	}
	p := b.client.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: &prefix, MaxResults: to.Ptr(int32(objstore.MaxListPage))})
	keys := []string{}
	for p.More() {
		if err := ctx.Err(); err != nil {
			return nil, "", opErr("list-prefixes-page", prefix, err)
		}
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, "", opErr("list-prefixes-page", prefix, err)
		}
		for _, item := range page.Segment.BlobPrefixes {
			key := value(item.Name)
			if key <= after {
				continue
			}
			if len(keys) == limit {
				return keys, keys[len(keys)-1], nil
			}
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys, "", nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return opErr("delete", key, err)
	}
	_, err := b.client.NewBlobClient(key).Delete(ctx, nil)
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil
	}
	return opErr("delete", key, err)
}

func (b *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	e := &objstore.DeleteError{}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			e.Failures = append(e.Failures, objstore.DeleteFailure{Key: key, Err: err, Unattempted: true})
		} else if err := b.Delete(ctx, key); err != nil {
			e.Failures = append(e.Failures, objstore.DeleteFailure{Key: key, Err: err})
		}
	}
	if len(e.Failures) != 0 {
		return e
	}
	return nil
}

func (b *Backend) EnsureBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return opErr("create-bucket", b.ID(), err)
	}
	_, err := b.client.GetProperties(ctx, nil)
	if err == nil {
		return nil
	}
	if !bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return opErr("head-bucket", b.ID(), err)
	}
	_, err = b.client.Create(ctx, nil)
	if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return nil
	}
	return opErr("create-bucket", b.ID(), err)
}

func (b *Backend) DropBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return opErr("drop-bucket", b.ID(), err)
	}
	_, err := b.client.Delete(ctx, nil)
	if bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return nil
	}
	return opErr("drop-bucket", b.ID(), err)
}

// Sign uses shared-key SAS. SAS cannot bind arbitrary request headers, so
// nonempty Headers are rejected rather than presenting them as authenticated.
func (b *Backend) Sign(ctx context.Context, key string, opts objstore.SignOptions) (objstore.SignedRequest, error) {
	if err := ctx.Err(); err != nil {
		return objstore.SignedRequest{}, opErr("sign", key, err)
	}
	if len(opts.Headers) > 0 || objstore.KMSKey(ctx) != "" {
		return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
	}
	if opts.Expires <= 0 || opts.Expires > 7*24*time.Hour {
		return objstore.SignedRequest{}, opErr("sign", key, errors.New("expiry must be in (0, 7 days]"))
	}
	p := sas.BlobPermissions{}
	headers := http.Header{}
	switch opts.Method {
	case http.MethodGet, http.MethodHead:
		p.Read = true
	case http.MethodDelete:
		p.Delete = true
	case http.MethodPut:
		p.Create, p.Write = true, true
		headers.Set("x-ms-blob-type", "BlockBlob")
	default:
		return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
	}
	u, err := b.client.NewBlobClient(key).GetSASURL(p, time.Now().Add(opts.Expires), nil)
	if err != nil {
		return objstore.SignedRequest{}, opErr("sign", key, fmt.Errorf("shared-key signing required: %w: %w", errors.ErrUnsupported, err))
	}
	return objstore.SignedRequest{URL: u, Method: opts.Method, Headers: headers}, nil
}
