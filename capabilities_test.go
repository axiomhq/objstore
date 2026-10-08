package objstore_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

type streamingBackend struct {
	objstore.Backend
	calls atomic.Int32
	key   string
}

func (b *streamingBackend) Upload(ctx context.Context, key string, body io.Reader, _ objstore.UploadOptions) (objstore.ObjectInfo, error) {
	b.calls.Add(1)
	b.key = objstore.KMSKey(ctx)
	n, err := io.Copy(io.Discard, body)
	return objstore.ObjectInfo{Size: n, ETag: "commit"}, err
}
func (b *streamingBackend) Stat(context.Context, string) (objstore.ObjectInfo, error) {
	b.calls.Add(1)
	return objstore.ObjectInfo{ETag: "stat"}, nil
}
func (b *streamingBackend) NewReader(context.Context, string, int64, int64) (io.ReadCloser, error) {
	b.calls.Add(1)
	return io.NopCloser(strings.NewReader("read")), nil
}
func (b *streamingBackend) Sign(context.Context, string, objstore.SignOptions) (objstore.SignedRequest, error) {
	b.calls.Add(1)
	return objstore.SignedRequest{Method: "GET"}, nil
}

func TestOptionalCapabilitiesAndPacing(t *testing.T) {
	// Embedding only Backend must not silently call buffered Get or Put.
	base := objstore.Open(struct{ objstore.Backend }{}, objstore.Config{RequestsPerSecond: 1e6})
	if _, err := base.NewReader(t.Context(), "k", 0, 0); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := base.Stat(t.Context(), "k"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := base.Upload(t.Context(), "k", strings.NewReader("x"), objstore.UploadOptions{}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, _, err := base.UploadIfAbsent(t.Context(), "k", strings.NewReader("x"), objstore.UploadOptions{}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := base.Sign(t.Context(), "k", objstore.SignOptions{Method: "GET", Expires: time.Minute}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	b := &streamingBackend{}
	s := objstore.Open(b, objstore.Config{RequestsPerSecond: 0.001})
	if _, err := s.Stat(t.Context(), "k"); err != nil {
		t.Fatal(err)
	} // consumes burst
	short, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if _, err := s.NewReader(short, "k", 0, 0); err == nil {
		t.Fatal("reader bypassed pacer")
	}
	if _, err := s.Upload(short, "k", strings.NewReader("x"), objstore.UploadOptions{}); err == nil {
		t.Fatal("upload bypassed pacer")
	}
	ctx := objstore.Urgent(t.Context())
	r, err := s.NewReader(ctx, "k", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if _, err := s.Sign(ctx, "k", objstore.SignOptions{Method: "GET", Expires: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != 3 {
		t.Fatalf("calls=%d", b.calls.Load())
	}
}

type startedReader struct {
	io.Reader
	started chan struct{}
	once    atomic.Bool
}

func (r *startedReader) Read(p []byte) (int, error) {
	if r.once.CompareAndSwap(false, true) {
		close(r.started)
	}
	return r.Reader.Read(p)
}

func TestUploadHoldsGateForBodyLifetime(t *testing.T) {
	b := &streamingBackend{}
	s := objstore.Open(b, objstore.Config{MaxInflightWrites: 1}).WithKMSKeys(func(context.Context, string) (string, error) { return "chosen", nil })
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Upload(t.Context(), "held", &startedReader{Reader: pr, started: started}, objstore.UploadOptions{})
		done <- err
	}()
	<-started
	short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.Upload(short, "blocked", strings.NewReader("x"), objstore.UploadOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gate: %v", err)
	}
	var de *objstore.DeleteError
	if err := s.DeleteMany(short, "a", "b"); !errors.As(err, &de) || len(de.Failures) != 2 || !de.Failures[0].Unattempted {
		t.Fatalf("delete gate: %v", err)
	}
	// Use a different backend field-free operation to avoid a test data race.
	if _, err := s.Stat(objstore.Urgent(t.Context()), "held"); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.key != "chosen" {
		t.Fatalf("KMS key=%q", b.key)
	}
	if _, err := s.Upload(t.Context(), "released", strings.NewReader("x"), objstore.UploadOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingWrappers(t *testing.T) {
	base := openFS(t, objstore.Config{RequestsPerSecond: 1e6})
	s, k := storetest.NewKMS(base)
	s, f := storetest.Faulty(t, s)
	f.WatchRewrites()
	ctx := objstore.WithKMSKey(t.Context(), "kms")
	size := int64(3)
	opts := objstore.UploadOptions{Size: &size}
	info, ok, err := s.UploadIfAbsent(ctx, "key", strings.NewReader("one"), opts)
	if err != nil || !ok || k.KeyOf("key") != "kms" {
		t.Fatalf("%+v %v %v", info, ok, err)
	}
	f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Mode: storetest.Ambiguous})
	_, ok, err = s.UploadIfMatch(ctx, "key", strings.NewReader("two"), info.ETag, opts)
	if !ok || !errors.Is(err, storetest.ErrFault) || f.WriteBytes() != 6 || len(f.Rewrites()) != 1 {
		t.Fatalf("ambiguous: %v %v bytes=%d rewrites=%v", ok, err, f.WriteBytes(), f.Rewrites())
	}
	r, err := s.NewReader(ctx, "key", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(got) != "wo" || f.ReadBytes() != 2 {
		t.Fatalf("read: %q %v %d", got, err, f.ReadBytes())
	}
	k.Revoke("kms")
	if _, err := s.NewReader(ctx, "key", 0, 0); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatal(err)
	}
	if _, err := s.Stat(ctx, "key"); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatal(err)
	}
	if _, err := s.Sign(ctx, "key", objstore.SignOptions{Method: http.MethodGet, Expires: time.Minute}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := s.Upload(ctx, "key", strings.NewReader("bad"), opts); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatal(err)
	}
}
