package storetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"

	"github.com/axiomhq/objstore"
)

const (
	OpStat Op = "Stat"
	OpSign Op = "Sign"
)

// Streaming uploads use the corresponding OpPut plan and ledgers without
// retaining payloads. Shaping applies latency, bandwidth and error checks per
// read chunk (including unknown-length streams).
func (f *Fault) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	info, _, err := f.upload(ctx, OpPut, key, body, "", opts)
	return info, err
}

func (f *Fault) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return f.upload(ctx, OpPutIfAbsent, key, body, "", opts)
}

func (f *Fault) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return f.upload(ctx, OpPutIfMatch, key, body, etag, opts)
}

type shapedReader struct {
	io.Reader
	f    *Fault
	ctx  context.Context
	read bool
}

func (r *shapedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if shapeErr := r.f.shapeCall(r.ctx, n); shapeErr != nil {
		return 0, shapeErr
	}
	if r.read {
		r.f.read(p[:n])
	}
	return n, err
}

func (f *Fault) upload(ctx context.Context, op Op, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	m, hit := f.hit(ctx, op, key)
	if hit && m != Ambiguous {
		return objstore.ObjectInfo{}, false, f.pre(ctx, m)
	}
	f.mu.Lock()
	watching := f.digest != nil
	f.mu.Unlock()
	h := sha256.New()
	if watching {
		body = io.TeeReader(body, h)
	}
	if !hit {
		body = &shapedReader{Reader: body, f: f, ctx: ctx}
	}
	var info objstore.ObjectInfo
	var ok bool
	err := error(errors.ErrUnsupported)
	if op == OpPut {
		if b, capable := f.b.(objstore.UploadBackend); capable {
			info, err = b.Upload(ctx, key, body, opts)
			ok = err == nil
		}
	} else if b, capable := f.b.(objstore.ConditionalUploadBackend); capable {
		if op == OpPutIfAbsent {
			info, ok, err = b.UploadIfAbsent(ctx, key, body, opts)
		} else {
			info, ok, err = b.UploadIfMatch(ctx, key, body, etag, opts)
		}
	}
	if ok && err == nil {
		f.wroteDigest(key, info.Size, [32]byte(h.Sum(nil)), watching)
	}
	if hit && err == nil {
		err = ErrFault
	}
	return info, ok, err
}

type trackedReader struct {
	io.Reader
	io.Closer
	f    *Fault
	once sync.Once
	err  error
}

func (r *trackedReader) Close() error {
	r.once.Do(func() { r.err = r.Closer.Close(); r.f.inflight.Add(-1) })
	return r.err
}

// NewReader is counted/planned as OpGetRange; the caller must close it.
func (f *Fault) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	f.inflight.Add(1)
	f.noteRead(key)
	if m, hit := f.hitAt(ctx, OpGetRange, offset, key); hit {
		defer f.inflight.Add(-1)
		return nil, f.pre(ctx, m)
	}
	b, ok := f.b.(objstore.ReaderBackend)
	if !ok {
		f.inflight.Add(-1)
		return nil, errors.ErrUnsupported
	}
	r, err := b.NewReader(ctx, key, offset, length)
	if err != nil {
		f.inflight.Add(-1)
		return nil, err
	}
	return &trackedReader{Reader: &shapedReader{Reader: r, f: f, ctx: ctx, read: true}, Closer: r, f: f}, nil
}

func (f *Fault) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if m, hit := f.hit(ctx, OpStat, key); hit {
		return objstore.ObjectInfo{}, f.pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return objstore.ObjectInfo{}, err
	}
	if b, ok := f.b.(objstore.StatBackend); ok {
		return b.Stat(ctx, key)
	}
	return objstore.ObjectInfo{}, errors.ErrUnsupported
}

func (f *Fault) Sign(ctx context.Context, key string, opts objstore.SignOptions) (objstore.SignedRequest, error) {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	if m, hit := f.hit(ctx, OpSign, key); hit {
		return objstore.SignedRequest{}, f.pre(ctx, m)
	}
	if err := f.shapeCall(ctx, 0); err != nil {
		return objstore.SignedRequest{}, err
	}
	if b, ok := f.b.(objstore.SignBackend); ok {
		return b.Sign(ctx, key, opts)
	}
	return objstore.SignedRequest{}, errors.ErrUnsupported
}

func (k *KMS) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	info, _, err := k.upload(ctx, OpPut, key, body, "", opts)
	return info, err
}

func (k *KMS) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return k.upload(ctx, OpPutIfAbsent, key, body, "", opts)
}

func (k *KMS) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return k.upload(ctx, OpPutIfMatch, key, body, etag, opts)
}

func (k *KMS) upload(ctx context.Context, op Op, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	k.mutation.Lock()
	defer k.mutation.Unlock()
	if err := k.denied(objstore.KMSKey(ctx)); err != nil {
		return objstore.ObjectInfo{}, false, err
	}
	var info objstore.ObjectInfo
	var ok bool
	err := error(errors.ErrUnsupported)
	if op == OpPut {
		if b, capable := k.Backend.(objstore.UploadBackend); capable {
			info, err = b.Upload(plain(ctx), key, body, opts)
			ok = err == nil
		}
	} else if b, capable := k.Backend.(objstore.ConditionalUploadBackend); capable {
		if op == OpPutIfAbsent {
			info, ok, err = b.UploadIfAbsent(plain(ctx), key, body, opts)
		} else {
			info, ok, err = b.UploadIfMatch(plain(ctx), key, body, etag, opts)
		}
	}
	if ok && err == nil {
		k.wrote(ctx, key)
	}
	return info, ok, err
}

func (k *KMS) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := k.readable(key); err != nil {
		return nil, err
	}
	if b, ok := k.Backend.(objstore.ReaderBackend); ok {
		return b.NewReader(ctx, key, offset, length)
	}
	return nil, errors.ErrUnsupported
}

func (k *KMS) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	if err := k.readable(key); err != nil {
		return objstore.ObjectInfo{}, err
	}
	if b, ok := k.Backend.(objstore.StatBackend); ok {
		return b.Stat(ctx, key)
	}
	return objstore.ObjectInfo{}, errors.ErrUnsupported
}

// Sign cannot simulate revocation after credentials leave the wrapper.
func (k *KMS) Sign(context.Context, string, objstore.SignOptions) (objstore.SignedRequest, error) {
	return objstore.SignedRequest{}, errors.ErrUnsupported
}
