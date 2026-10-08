//go:build unix && !aix && (!solaris || illumos)

package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/internal/stream"
)

func (f *Backend) openStream(ctx context.Context, key string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(p)
	if errors.Is(err, iofs.ErrNotExist) {
		err = objstore.ErrNotFound
	}
	return file, err
}

func (f *Backend) Stat(ctx context.Context, key string) (objstore.ObjectInfo, error) {
	file, err := f.openStream(ctx, key)
	if err != nil {
		return objstore.ObjectInfo{}, objstore.OpErr("stat", key, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return objstore.ObjectInfo{}, objstore.OpErr("stat", key, err)
	}
	h := sha256.New()
	n, err := io.Copy(h, &stream.Reader{Ctx: ctx, Body: file})
	if err != nil {
		return objstore.ObjectInfo{}, objstore.OpErr("stat", key, err)
	}
	return objstore.ObjectInfo{Size: n, ETag: hex.EncodeToString(h.Sum(nil)), LastModified: info.ModTime()}, nil
}

func (f *Backend) NewReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := stream.Range(offset, length); err != nil {
		return nil, objstore.OpErr("new-reader", key, err)
	}
	file, err := f.openStream(ctx, key)
	if err != nil {
		return nil, objstore.OpErr("new-reader", key, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, objstore.OpErr("new-reader", key, err)
	}
	n := max(int64(0), info.Size()-offset)
	if length > 0 {
		n = min(n, length)
	}
	r := &stream.Reader{Ctx: ctx, Body: io.NewSectionReader(file, offset, n), Size: &n}
	return &stream.ReadCloser{Reader: r, Closer: file}, nil
}

func (f *Backend) Upload(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, error) {
	info, _, err := f.upload(ctx, key, body, "", false, false, opts)
	return info, err
}

func (f *Backend) UploadIfAbsent(ctx context.Context, key string, body io.Reader, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return f.upload(ctx, key, body, "", true, false, opts)
}

func (f *Backend) UploadIfMatch(ctx context.Context, key string, body io.Reader, etag string, opts objstore.UploadOptions) (objstore.ObjectInfo, bool, error) {
	return f.upload(ctx, key, body, etag, false, true, opts)
}

func (f *Backend) upload(ctx context.Context, key string, body io.Reader, etag string, absent, match bool, opts objstore.UploadOptions) (info objstore.ObjectInfo, ok bool, err error) {
	defer func() { err = objstore.OpErr("upload", key, err) }()
	if len(opts.Metadata) != 0 || opts.ContentType != "" || opts.CacheControl != "" || opts.StorageClass != "" {
		return info, false, errors.ErrUnsupported
	}
	r, err := stream.New(ctx, body, opts)
	if err != nil {
		return info, false, err
	}
	dst, err := f.path(key)
	if err != nil {
		return info, false, err
	}
	if _, err = os.Stat(f.root); err != nil {
		return info, false, f.bucketErr(err)
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return info, false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return info, false, err
	}
	defer func() { tmp.Close(); os.Remove(tmp.Name()) }()
	h := sha256.New()
	buf := make([]byte, writeBackChunk)
	t := objstore.TimingsOf(ctx)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			start := time.Now()
			_, err = tmp.Write(buf[:n])
			t.Since(objstore.CallWrite, start)
			if err != nil {
				return info, false, err
			}
			h.Write(buf[:n])
			if !objstore.IsUrgent(ctx) {
				start = time.Now()
				err = writeBack(tmp, r.N-int64(n), int64(n))
				t.Since(objstore.CallWriteBack, start)
				if err != nil {
					return info, false, err
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return info, false, readErr
		}
	}
	if err = ctx.Err(); err != nil {
		return info, false, err
	}
	if err = tmp.Sync(); err != nil {
		return info, false, err
	}
	if err = tmp.Close(); err != nil {
		return info, false, err
	}
	unlock, err := f.lockKey(ctx, key)
	if err != nil {
		return info, false, err
	}
	defer unlock()
	if match {
		current, err := f.Stat(ctx, key)
		if errors.Is(err, objstore.ErrNotFound) {
			return info, false, nil
		}
		if err != nil {
			return info, false, err
		}
		if current.ETag != etag {
			return info, false, nil
		}
	}
	if err = ctx.Err(); err != nil {
		return info, false, err
	}
	if absent {
		err = os.Link(tmp.Name(), dst)
		if errors.Is(err, iofs.ErrExist) {
			return info, false, f.syncPublishedDir(dst, t)
		}
	} else {
		err = os.Rename(tmp.Name(), dst)
	}
	if err != nil {
		return info, false, err
	}
	info = objstore.ObjectInfo{Size: r.N, ETag: hex.EncodeToString(h.Sum(nil))}
	return info, true, f.syncPublishedDir(dst, t)
}
