package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/internal/stream"
)

func objectInfo(a *storage.ObjectAttrs) objstore.ObjectInfo {
	return objstore.ObjectInfo{Size: a.Size, ETag: etagOf(a.Generation), Metadata: a.Metadata,
		ContentType: a.ContentType, CacheControl: a.CacheControl, StorageClass: a.StorageClass, LastModified: a.Updated}
}

func (g *Backend) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	if err := done(ctx, "stat", key); err != nil {
		return objstore.ObjectInfo{}, err
	}
	a, err := g.bucket.Object(key).Attrs(ctx)
	if isNotFound(err) {
		err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	}
	if err != nil {
		return objstore.ObjectInfo{}, opErr("stat", key, err)
	}
	return objectInfo(a), nil
}

func (g *Backend) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := stream.Range(offset, length); err != nil {
		return nil, opErr("new-reader", key, err)
	}
	if err := done(ctx, "new-reader", key); err != nil {
		return nil, err
	}
	if length == 0 {
		length = -1
	}
	r, err := g.bucket.Object(key).NewRangeReader(ctx, offset, length)
	if httpCode(err) == http.StatusRequestedRangeNotSatisfiable {
		info, herr := g.Stat(ctx, key)
		if herr != nil {
			return nil, herr
		}
		if offset >= info.Size {
			return io.NopCloser(strings.NewReader("")), nil
		}
		err = fmt.Errorf("%w: %w", objstore.ErrRange, err)
	}
	if isNotFound(err) {
		err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	}
	if err != nil {
		return nil, opErr("new-reader", key, err)
	}
	want := max(int64(0), r.Attrs.Size-offset)
	if length > 0 {
		want = min(want, length)
	}
	if r.Attrs.StartOffset != offset || r.Remain() != want {
		r.Close()
		return nil, opErr("new-reader", key, objstore.ErrRange)
	}
	return stream.Download(ctx, r, &want), nil
}

func (g *Backend) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	return g.upload(ctx, key, g.bucket.Object(key), body, opts)
}

func (g *Backend) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	info, err := g.upload(ctx, key, g.bucket.Object(key).If(storage.Conditions{DoesNotExist: true}), body, opts)
	if httpCode(err) == http.StatusPreconditionFailed {
		return objstore.ObjectInfo{}, false, nil
	}
	return info, err == nil, err
}

func (g *Backend) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	if err := done(ctx, "upload-if-match", key); err != nil {
		return objstore.ObjectInfo{}, false, err
	}
	gen, ok := generation(etag)
	if !ok {
		return objstore.ObjectInfo{}, false, nil
	}
	info, err := g.upload(ctx, key, g.bucket.Object(key).If(storage.Conditions{GenerationMatch: gen}), body, opts)
	if httpCode(err) == http.StatusPreconditionFailed {
		return objstore.ObjectInfo{}, false, nil
	}
	if isNotFound(err) {
		return objstore.ObjectInfo{}, false, opErr("upload-if-match", key, g.bucketGone(ctx, err))
	}
	return info, err == nil, err
}

func (g *Backend) upload(ctx context.Context, key string, obj *storage.ObjectHandle, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	r, err := stream.New(ctx, body, opts)
	if err != nil {
		return objstore.ObjectInfo{}, opErr("upload", key, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := obj.NewWriter(ctx)
	w.ChunkSize = singleShotMax
	w.ContentType = opts.ContentType
	if w.ContentType == "" {
		w.ContentType = "application/octet-stream"
	}
	w.CacheControl, w.Metadata, w.StorageClass = opts.CacheControl, opts.Metadata, opts.StorageClass
	w.KMSKeyName = objstore.KMSKey(ctx)
	if _, err := io.Copy(w, r); err != nil {
		cancel() // never Close a healthy writer after an incomplete source
		_ = w.Close()
		return objstore.ObjectInfo{}, opErr("upload", key, err)
	}
	if err := w.Close(); err != nil {
		return objstore.ObjectInfo{}, opErr("upload", key, err)
	}
	return objectInfo(w.Attrs()), nil
}

func (g *Backend) Sign(ctx context.Context, key string, opts objstore.SignOptions) (objstore.SignedRequest, error) {
	if err := done(ctx, "sign", key); err != nil {
		return objstore.SignedRequest{}, err
	}
	if opts.Expires <= 0 || opts.Expires > 7*24*time.Hour {
		return objstore.SignedRequest{}, opErr("sign", key, errors.New("expiry must be in (0, 7 days]"))
	}
	switch opts.Method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
	default:
		return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
	}
	headers := opts.Headers.Clone()
	if headers == nil {
		headers = http.Header{}
	}
	if opts.Method == http.MethodPut && objstore.KMSKey(ctx) != "" {
		headers.Set("x-goog-encryption-kms-key-name", objstore.KMSKey(ctx))
	}
	in := &storage.SignedURLOptions{Method: opts.Method, Expires: time.Now().Add(opts.Expires), Scheme: storage.SigningSchemeV4}
	for k, vs := range headers {
		if len(vs) != 1 {
			return objstore.SignedRequest{}, opErr("sign", key, errors.ErrUnsupported)
		}
		if strings.EqualFold(k, "Content-Type") {
			in.ContentType = vs[0]
		} else {
			in.Headers = append(in.Headers, k+":"+vs[0])
		}
	}
	u, err := g.bucket.SignedURL(key, in)
	if err != nil {
		return objstore.SignedRequest{}, opErr("sign", key, err)
	}
	return objstore.SignedRequest{URL: u, Method: opts.Method, Headers: headers}, nil
}
