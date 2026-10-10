// Package stream holds body validation shared by provider implementations.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/axiomhq/objstore"
)

// Reader checks cancellation between reads and limits known-size bodies to
// their declared prefix. It deliberately exposes no WriterTo or Seek bypass.
type Reader struct {
	Ctx  context.Context
	Body io.Reader
	Size *int64
	N    int64
	Err  error // sticky source error; io.ReadFull may otherwise hide it
}

func (r *Reader) Read(p []byte) (int, error) {
	if err := r.Ctx.Err(); err != nil {
		return 0, err
	}
	if r.Err != nil {
		return 0, r.Err
	}
	if r.Size != nil {
		left := *r.Size - r.N
		if left == 0 {
			return 0, io.EOF
		}
		p = p[:min(int64(len(p)), left)]
	}
	n, err := r.Body.Read(p)
	r.N += int64(n)
	if err == io.EOF && r.Size != nil && r.N < *r.Size {
		err = io.ErrUnexpectedEOF
	}
	if err == io.ErrUnexpectedEOF {
		// Multipart SDKs treat bare UnexpectedEOF as a successful last
		// partial block. A truncated source is not a valid end of upload.
		err = fmt.Errorf("incomplete source: %w", err)
	}
	if err != nil && err != io.EOF {
		r.Err = err
	}
	return n, err
}

func New(ctx context.Context, body io.Reader, opts objstore.UploadOptions) (*Reader, error) {
	if body == nil || opts.Size != nil && *opts.Size < 0 {
		return nil, errors.New("nil body or negative size")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Reader{Ctx: ctx, Body: body, Size: opts.Size}, nil
}

func Range(offset, length int64) error {
	if offset < 0 || length < 0 || offset > math.MaxInt64-length {
		return objstore.ErrRange
	}
	return nil
}

// HTTPOptions rejects metadata/header values that an HTTP provider would
// normalize or drop. Lowercase names give stable round trips across SDKs.
func HTTPOptions(opts objstore.UploadOptions, azure bool) error {
	for k, v := range opts.Metadata {
		if k == "" {
			return fmt.Errorf("empty metadata name: %w", errors.ErrUnsupported)
		}
		for i, c := range k {
			valid := c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9' || !azure && c == '-'
			if !valid {
				return fmt.Errorf("metadata name %q must be lowercase ASCII: %w", k, errors.ErrUnsupported)
			}
		}
		if err := headerValue(v); err != nil {
			return err
		}
	}
	for _, v := range []string{opts.ContentType, opts.CacheControl, opts.StorageClass} {
		if err := headerValue(v); err != nil {
			return err
		}
	}
	return nil
}

func headerValue(v string) error {
	if v != strings.TrimSpace(v) {
		return fmt.Errorf("header value has surrounding whitespace: %w", errors.ErrUnsupported)
	}
	for _, c := range v {
		if c < 32 || c > 126 {
			return fmt.Errorf("header value must be printable ASCII: %w", errors.ErrUnsupported)
		}
	}
	return nil
}

// ReadCloser preserves the underlying Close while checking cancellation.
type ReadCloser struct {
	io.Reader
	io.Closer
}

// Download verifies declared length AND the terminal provider error. Unlike
// upload prefixes it must read through EOF so SDK checksum validation runs.
func Download(ctx context.Context, body io.ReadCloser, size *int64) io.ReadCloser {
	return &ReadCloser{Reader: &downloadReader{Reader: Reader{Ctx: ctx, Body: body}, size: size}, Closer: body}
}

type downloadReader struct {
	Reader
	size *int64
}

func (r *downloadReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.size != nil {
		if r.N > *r.size {
			return 0, fmt.Errorf("body exceeds size %d", *r.size)
		}
		if err == io.EOF && r.N < *r.size {
			return n, io.ErrUnexpectedEOF
		}
	}
	return n, err
}
