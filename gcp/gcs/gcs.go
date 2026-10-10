package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/axiomhq/objstore"
)

// Config selects the bucket and how to reach it.
type Config struct {
	// Bucket is the bucket every key lives in.
	Bucket string
	// ProjectID is needed only by EnsureBucket, to create a missing bucket.
	ProjectID string
	// Options are passed to storage.NewClient: endpoint, credentials, or
	// option.WithoutAuthentication for an emulator. Close also closes idle
	// connections on an HTTP client supplied through these options.
	Options []option.ClientOption
	// Client supplies a caller-owned SDK client, including its retry policy.
	// Mutually exclusive with Options. Close never closes this client.
	// Use storage.WithJSONReads when constructing it for conditional reads.
	Client *storage.Client
}

// Backend is the Google Cloud Storage backend. Close releases clients created
// by New, but not a caller-owned Config.Client.
type Backend struct {
	client  *storage.Client
	bucket  *storage.BucketHandle
	name    string
	project string
	owned   bool
}

// singleShotMax is the largest object uploaded in one request. Anything
// smaller is buffered whole, so the client can retry the request; larger
// objects fall back to the client's resumable upload in 16 MiB chunks.
const singleShotMax = 16 << 20

// deleteParallelism bounds DeleteMany's concurrent requests.
const deleteParallelism = 16

// New connects to cfg.Bucket. Credentials come from cfg.Options or, by
// default, Application Default Credentials.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("gcs: empty bucket name")
	}
	if cfg.Client != nil {
		if len(cfg.Options) != 0 {
			return nil, errors.New("gcs: Client and Options are mutually exclusive")
		}
		return &Backend{client: cfg.Client, bucket: cfg.Client.Bucket(cfg.Bucket), name: cfg.Bucket, project: cfg.ProjectID}, nil
	}
	// JSON reads: the XML read path silently drops GenerationNotMatch, so
	// GetIfChanged would never see a 304.
	opts := append([]option.ClientOption{storage.WithJSONReads()}, cfg.Options...)
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: %w", err)
	}
	return &Backend{client: client, bucket: client.Bucket(cfg.Bucket), name: cfg.Bucket, project: cfg.ProjectID, owned: true}, nil
}

// Close releases the storage client New created. It must not run concurrently
// with backend operations; the backend must not be used afterwards.
func (b *Backend) Close() error {
	if b.owned {
		return b.client.Close()
	}
	return nil
}

// ID is gs://bucket.
func (b *Backend) ID() string { return "gs://" + b.name }

// Open returns a Store over New(ctx, cfg) with ocfg's pacing and write bound.
func Open(ctx context.Context, cfg Config, ocfg objstore.Config) (*objstore.Store, error) {
	b, err := New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return objstore.Open(b, ocfg), nil
}

func httpCode(err error) int {
	var e *googleapi.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return 0
}

// SupportsKMS: GCS applies a per-object Cloud KMS key
// (objstore.Store.KMS).
func (g *Backend) SupportsKMS() bool { return true }

// opErr is objstore.OpErr, adding objstore.ErrAccessDenied for a 403: the
// caller lacks access, or the object's Cloud KMS key is disabled or its
// grant is gone. Missing buckets also wrap ErrNotFound.
func opErr(op, key string, err error) error {
	if httpCode(err) == http.StatusForbidden {
		err = fmt.Errorf("%w: %w", objstore.ErrAccessDenied, err)
	}
	if bucketMissing(err) && !errors.Is(err, objstore.ErrNotFound) {
		err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	}
	return objstore.OpErr(op, key, err)
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist) || httpCode(err) == http.StatusNotFound
}

func bucketMissing(err error) bool {
	return errors.Is(err, storage.ErrBucketNotExist) || httpCode(err) == http.StatusNotFound
}

func generation(etag string) (int64, bool) {
	gen, err := strconv.ParseInt(etag, 10, 64)
	return gen, err == nil && gen > 0
}

func etagOf(gen int64) string { return strconv.FormatInt(gen, 10) }

// done returns ctx's error, wrapped with op and key, once ctx is done. The
// client does not check ctx before every request: against fake-gcs-server
// a List and a Delete under a cancelled ctx succeeded.
func done(ctx context.Context, op, key string) error {
	return opErr(op, key, ctx.Err())
}

// write uploads data through obj. The upload error surfaces on Close.
func write(ctx context.Context, obj *storage.ObjectHandle, data []byte) error {
	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.KMSKeyName = objstore.KMSKey(ctx)
	if len(data) < singleShotMax {
		// One request instead of a resumable session, with the object
		// buffered so a transient failure can be retried. ChunkSize 0 would
		// also be one request but disables retries.
		w.ChunkSize = len(data) + 1
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// Put writes key unconditionally.
func (g *Backend) Put(ctx context.Context, key string, data []byte) error {
	if err := done(ctx, "put", key); err != nil {
		return err
	}
	return opErr("put", key, write(ctx, g.bucket.Object(key), data))
}

// PutIfAbsent writes key only if no live object exists (ifGenerationMatch=0).
// Returns false when the key existed.
func (g *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if err := done(ctx, "put-if-absent", key); err != nil {
		return false, err
	}
	err := write(ctx, g.bucket.Object(key).If(storage.Conditions{DoesNotExist: true}), data)
	if err != nil {
		if httpCode(err) == http.StatusPreconditionFailed {
			return false, nil
		}
		return false, opErr("put-if-absent", key, err)
	}
	return true, nil
}

// PutIfMatch replaces key only if its generation equals etag. (false, nil)
// = precondition failed: the object changed, no longer exists, or etag is
// not a generation this backend issued.
func (g *Backend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	if err := done(ctx, "put-if-match", key); err != nil {
		return false, err
	}
	gen, ok := generation(etag)
	if !ok {
		return false, nil
	}
	err := write(ctx, g.bucket.Object(key).If(storage.Conditions{GenerationMatch: gen}), data)
	switch {
	case err == nil:
		return true, nil
	case httpCode(err) == http.StatusPreconditionFailed:
		return false, nil
	case isNotFound(err):
		return false, opErr("put-if-match", key, g.bucketGone(ctx, err))
	}
	return false, opErr("put-if-match", key, err)
}

// bucketGone tells a missing object from a missing bucket after a 404,
// which GCS reports alike for both: the bucket's error if a read of the
// bucket proves it gone, else nil, the object was missing. Only this rare
// path pays the extra request. Any other answer, such as a 403 for a role
// that may read objects but not buckets, leaves the object missing.
func (g *Backend) bucketGone(ctx context.Context, err error) error {
	if _, aerr := g.bucket.Attrs(ctx); errors.Is(aerr, storage.ErrBucketNotExist) {
		return fmt.Errorf("%w: %w", aerr, err)
	}
	return nil
}

// readAll drains r and proves the object arrived whole: a truncated body
// must be a named error, never a silent prefix. Like s3's readBody it reads
// into one exact-size buffer, so no slack stays pinned on the result.
func readAll(op, key string, r *storage.Reader) ([]byte, error) {
	defer r.Close()
	size := r.Attrs.Size
	if size < 0 || size > 64<<20 {
		// Cap speculative allocation: a bogus size must not cause an
		// enormous make.
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, opErr(op, key, err)
		}
		if int64(len(data)) != size {
			return nil, opErr(op, key, fmt.Errorf("short read: got %d bytes, object size is %d", len(data), size))
		}
		return data, nil
	}
	data := make([]byte, size)
	n, err := io.ReadFull(r, data)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return nil, opErr(op, key, fmt.Errorf("short read: got %d bytes, object size is %d", n, size))
	}
	if err != nil {
		return nil, opErr(op, key, err)
	}
	// Read on to EOF: the client validates its checksum there, and a body
	// longer than the size is as wrong as a short one.
	var extra [1]byte
	if _, err := io.ReadFull(r, extra[:]); err == nil {
		return nil, opErr(op, key, fmt.Errorf("long read: body exceeds object size %d", size))
	} else if !errors.Is(err, io.EOF) {
		return nil, opErr(op, key, err)
	}
	return data, nil
}

func (g *Backend) get(ctx context.Context, op, key string) ([]byte, string, error) {
	if err := done(ctx, op, key); err != nil {
		return nil, "", err
	}
	r, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		if isNotFound(err) {
			err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
		}
		return nil, "", opErr(op, key, err)
	}
	gen := r.Attrs.Generation
	data, err := readAll(op, key, r)
	if err != nil {
		return nil, "", err
	}
	return data, etagOf(gen), nil
}

// Get reads the whole object at key.
func (g *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := g.get(ctx, "get", key)
	return data, err
}

// GetWithETag returns the object and its generation for conditional
// replacement.
func (g *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	return g.get(ctx, "get-with-etag", key)
}

// GetIfChanged is one conditional GET (ifGenerationNotMatch, sent only on
// the JSON read path New selects): GCS answers 304, which the client
// surfaces as a *googleapi.Error with Code 304. A server that ignores the
// precondition (fake-gcs-server, or a caller forcing XML reads) and sends
// the same generation back is also read as unchanged, before the body is
// read. Any error is an error, never "unchanged".
func (g *Backend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := done(ctx, "get-if-changed", key); err != nil {
		return nil, "", false, err
	}
	// An empty or foreign etag is no generation: read unconditionally.
	gen, ok := generation(etag)
	obj := g.bucket.Object(key)
	if ok {
		obj = obj.If(storage.Conditions{GenerationNotMatch: gen})
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		if ok && httpCode(err) == http.StatusNotModified {
			return nil, etag, true, nil
		}
		if isNotFound(err) {
			err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
		}
		return nil, "", false, opErr("get-if-changed", key, err)
	}
	if ok && r.Attrs.Generation == gen {
		r.Close()
		return nil, etag, true, nil
	}
	cur := r.Attrs.Generation
	data, err := readAll("get-if-changed", key, r)
	if err != nil {
		return nil, "", false, err
	}
	return data, etagOf(cur), false, nil
}

// GetRange reads exactly length bytes at offset. A range starting past the
// end (416) or running past it (a short body) is ErrRange.
func (g *Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := done(ctx, "get-range", key); err != nil {
		return nil, err
	}
	r, err := g.bucket.Object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		switch {
		case isNotFound(err):
			err = fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
		case httpCode(err) == http.StatusRequestedRangeNotSatisfiable:
			err = fmt.Errorf("%w: %w", objstore.ErrRange, err)
		}
		return nil, opErr("get-range", key, err)
	}
	defer r.Close()
	// A server ignoring Range must not turn a block read into an unbounded
	// whole-object download.
	if r.Remain() != length || r.Attrs.StartOffset != offset {
		return nil, opErr("get-range", key, objstore.ErrRange)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			err = fmt.Errorf("%w: %w", objstore.ErrRange, err)
		}
		return nil, opErr("get-range", key, err)
	}
	return data, nil
}

// ListPage lists up to limit keys under prefix after after.
func (g *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	q := &storage.Query{Prefix: prefix, StartOffset: after}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, "", opErr("list-page", prefix, err)
	}
	it := g.bucket.Objects(ctx, q)
	// StartOffset is inclusive: room for after itself plus one extra key
	// that tells whether more remain.
	it.PageInfo().MaxSize = min(limit+2, objstore.MaxListPage)
	keys := make([]string, 0, limit)
	for {
		// Next may fetch a page; the client does not check ctx first.
		if err := done(ctx, "list-page", prefix); err != nil {
			return nil, "", err
		}
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return keys, "", nil
		}
		if err != nil {
			return nil, "", opErr("list-page", prefix, err)
		}
		if attrs.Name <= after {
			continue
		}
		keys = append(keys, attrs.Name)
		if len(keys) == limit {
			// Buffered items all sort after after, and a page token means
			// the server has more: either proves another page without
			// fetching one more item, which at limit == MaxListPage
			// (the server's page cap) would cost a second request.
			pi := it.PageInfo()
			if pi.Remaining() > 0 || pi.Token != "" {
				return keys, keys[len(keys)-1], nil
			}
			return keys, "", nil
		}
	}
}

// ListPrefixesPage lists the child prefixes one level under prefix. A
// delimited page may carry only objects at the prefix level; the iterator
// keeps fetching until limit prefixes are found or the listing ends.
func (g *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	q := &storage.Query{Prefix: prefix, Delimiter: "/", StartOffset: after}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, "", opErr("list-prefixes-page", prefix, err)
	}
	it := g.bucket.Objects(ctx, q)
	it.PageInfo().MaxSize = min(limit+2, objstore.MaxListPage)
	out := make([]string, 0, limit)
	for {
		if err := done(ctx, "list-prefixes-page", prefix); err != nil {
			return nil, "", err
		}
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, "", nil
		}
		if err != nil {
			return nil, "", opErr("list-prefixes-page", prefix, err)
		}
		// Objects at the prefix level are skipped; StartOffset = after
		// brings after's own prefix back, so it is skipped too.
		if attrs.Prefix == "" || attrs.Prefix <= after {
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1], nil
		}
		out = append(out, attrs.Prefix)
	}
}

func (g *Backend) delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := g.bucket.Object(key).Delete(ctx)
	if isNotFound(err) {
		return g.bucketGone(ctx, err)
	}
	return err
}

// Delete removes key. A missing key is not an error; a missing bucket is.
func (g *Backend) Delete(ctx context.Context, key string) error {
	return opErr("delete", key, g.delete(ctx, key))
}

// DeleteMany removes keys with bounded parallelism (GCS has no batch delete
// in the JSON client). Missing keys are not an error. Empty input is a
// no-op.
func (g *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	var eg errgroup.Group
	eg.SetLimit(deleteParallelism)
	failures := make([]objstore.DeleteFailure, len(keys))
	for i, key := range keys {
		eg.Go(func() error {
			if err := ctx.Err(); err != nil {
				failures[i] = objstore.DeleteFailure{Key: key, Err: err, Unattempted: true}
			} else if err := g.delete(ctx, key); err != nil {
				failures[i] = objstore.DeleteFailure{Key: key, Err: opErr("delete-many", key, err)}
			}
			return nil
		})
	}
	_ = eg.Wait()
	e := &objstore.DeleteError{}
	for _, f := range failures {
		if f.Err != nil {
			e.Failures = append(e.Failures, f)
		}
	}
	if len(e.Failures) > 0 {
		return e
	}
	return nil
}

// EnsureBucket creates the bucket in Config.ProjectID if it is missing.
func (g *Backend) EnsureBucket(ctx context.Context) error {
	if err := done(ctx, "head-bucket", g.name); err != nil {
		return err
	}
	_, err := g.bucket.Attrs(ctx)
	if err == nil {
		return nil
	}
	if !bucketMissing(err) {
		return opErr("head-bucket", g.name, err)
	}
	if g.project == "" {
		return opErr("create-bucket", g.name, fmt.Errorf("Config.ProjectID is empty: %w", err))
	}
	err = g.bucket.Create(ctx, g.project, nil)
	if httpCode(err) == http.StatusConflict {
		return nil
	}
	return opErr("create-bucket", g.name, err)
}

// DropBucket empties the bucket and deletes it. A bucket that is already
// gone is not an error.
func (g *Backend) DropBucket(ctx context.Context) error {
	if err := done(ctx, "drop-bucket", g.name); err != nil {
		return err
	}
	for {
		keys, _, err := g.ListPage(ctx, "", "", objstore.MaxListPage)
		if err != nil {
			if bucketMissing(err) {
				return nil
			}
			return opErr("drop-bucket", g.name, err)
		}
		if len(keys) == 0 {
			break
		}
		if err := g.DeleteMany(ctx, keys...); err != nil {
			return opErr("drop-bucket", g.name, err)
		}
	}
	err := g.bucket.Delete(ctx)
	if bucketMissing(err) {
		return nil
	}
	return opErr("drop-bucket", g.name, err)
}
