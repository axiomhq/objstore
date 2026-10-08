package storetest

import (
	"context"
	"errors"
	"io"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
)

type sourceError struct{ err error }

func (r sourceError) Read([]byte) (int, error) { return 0, r.err }

// StreamingConformance exercises optional reads, uploads and Stat. Unlike the
// base Conformance it requires these capabilities; legacy backends can continue
// using Conformance alone. Each provider chooses a supported storage class.
func StreamingConformance(t *testing.T, s *objstore.Store, storageClass string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	t.Run("StreamRoundTrip", func(t *testing.T) {
		const data = "aZ13-contents"
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() { _, err := io.WriteString(pw, data); pw.CloseWithError(err); done <- err }()
		info, err := s.Upload(ctx, "stream/pipe", pr, objstore.UploadOptions{})
		pr.CloseWithError(err)
		if e := <-done; e != nil {
			t.Fatal(e)
		}
		if err != nil || info.Size != int64(len(data)) || info.ETag == "" {
			t.Fatalf("upload: %+v %v", info, err)
		}
		stat, err := s.Stat(ctx, "stream/pipe")
		if err != nil || stat.Size != info.Size || stat.ETag != info.ETag {
			t.Fatalf("stat: %+v %v", stat, err)
		}
		_, tag, err := s.GetWithETag(ctx, "stream/pipe")
		if err != nil || tag != info.ETag {
			t.Fatalf("byte token: %q %v", tag, err)
		}
		for _, tc := range []struct {
			name        string
			off, length int64
			want        string
		}{
			{"whole", 0, 0, data}, {"middle", 2, 3, "13-"}, {"to end", 5, 0, "contents"},
			{"past end", 8, 100, "tents"}, {"at eof", 13, 1, ""}, {"beyond eof", 33, 0, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r, err := s.NewReader(ctx, "stream/pipe", tc.off, tc.length)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				cerr := r.Close()
				if err != nil || cerr != nil || string(got) != tc.want {
					t.Fatalf("got %q %v close=%v, want %q", got, err, cerr, tc.want)
				}
			})
		}
		if _, err := s.GetRange(ctx, "stream/pipe", 8, 100); !errors.Is(err, objstore.ErrRange) {
			t.Fatalf("strict range: %v", err)
		}
		if _, err := s.NewReader(ctx, "stream/pipe", -1, 1); !errors.Is(err, objstore.ErrRange) {
			t.Fatalf("invalid range: %v", err)
		}
		if _, err := s.NewReader(ctx, "stream/missing", 0, 0); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("missing reader: %v", err)
		}
		if _, err := s.Stat(ctx, "stream/missing"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("missing stat: %v", err)
		}
	})
	t.Run("StreamSize", func(t *testing.T) {
		for _, size := range []int64{0, 3} {
			body := strings.NewReader("abcdef")
			info, err := s.Upload(ctx, "stream/sized", body, objstore.UploadOptions{Size: &size})
			if err != nil || info.Size != size || body.Len() != 6-int(size) {
				t.Fatalf("size %d: %+v %v remaining=%d", size, info, err, body.Len())
			}
			r, err := s.NewReader(ctx, "stream/sized", 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil || string(got) != "abcdef"[:size] {
				t.Fatalf("sized read %q %v", got, err)
			}
		}
		size := int64(9)
		if _, err := s.Upload(ctx, "stream/short", strings.NewReader("short"), objstore.UploadOptions{Size: &size}); err == nil {
			t.Fatal("short input succeeded")
		}
		body := io.MultiReader(strings.NewReader("prefix"), sourceError{io.ErrUnexpectedEOF})
		if _, err := s.Upload(ctx, "stream/source-error", body, objstore.UploadOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("source error lost: %v", err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Upload(cancelled, "stream/cancelled", strings.NewReader("x"), objstore.UploadOptions{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled: %v", err)
		}
	})
	t.Run("StreamCAS", func(t *testing.T) {
		size := int64(3)
		opts := objstore.UploadOptions{Size: &size}
		first, ok, err := s.UploadIfAbsent(ctx, "stream/cas", strings.NewReader("one"), opts)
		if err != nil || !ok || first.ETag == "" || first.Size != 3 {
			t.Fatalf("create: %+v %v %v", first, ok, err)
		}
		if _, ok, err := s.UploadIfAbsent(ctx, "stream/cas", strings.NewReader("two"), opts); err != nil || ok {
			t.Fatalf("lost create: %v %v", ok, err)
		}
		second, ok, err := s.UploadIfMatch(ctx, "stream/cas", strings.NewReader("two"), first.ETag, opts)
		if err != nil || !ok || second.ETag == first.ETag || second.ETag == "" {
			t.Fatalf("replace: %+v %v %v", second, ok, err)
		}
		if _, ok, err := s.UploadIfMatch(ctx, "stream/cas", strings.NewReader("bad"), first.ETag, opts); err != nil || ok {
			t.Fatalf("lost replace: %v %v", ok, err)
		}
		if ok, err := s.PutIfMatch(ctx, "stream/cas", []byte("byte"), second.ETag); err != nil || !ok {
			t.Fatalf("byte CAS: %v %v", ok, err)
		}
	})
	t.Run("StreamMetadata", func(t *testing.T) {
		opts := objstore.UploadOptions{Metadata: map[string]string{"owner": "test", "revision": "seven"}, ContentType: "text/plain", CacheControl: "no-store", StorageClass: storageClass}
		_, err := s.Upload(ctx, "stream/options", strings.NewReader("v"), opts)
		if errors.Is(err, errors.ErrUnsupported) {
			if _, err := s.Get(ctx, "stream/options"); !errors.Is(err, objstore.ErrNotFound) {
				t.Fatalf("unsupported options wrote object: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Stat(ctx, "stream/options")
		if err != nil || !maps.Equal(info.Metadata, opts.Metadata) || info.ContentType != opts.ContentType ||
			info.CacheControl != opts.CacheControl || storageClass != "" && info.StorageClass != storageClass {
			t.Fatalf("options roundtrip: %+v %v", info, err)
		}
	})
	t.Run("StreamMetadataNoNormalization", func(t *testing.T) {
		opts := objstore.UploadOptions{Metadata: map[string]string{"Owner": " café "}}
		_, err := s.Upload(ctx, "stream/metadata-exact", strings.NewReader("v"), opts)
		if err != nil {
			if _, getErr := s.Get(ctx, "stream/metadata-exact"); !errors.Is(getErr, objstore.ErrNotFound) {
				t.Fatalf("rejected metadata wrote an object: %v (upload %v)", getErr, err)
			}
			return
		}
		info, err := s.Stat(ctx, "stream/metadata-exact")
		if err != nil || !maps.Equal(info.Metadata, opts.Metadata) {
			t.Fatalf("metadata normalized: %+v %v", info, err)
		}
	})
}
