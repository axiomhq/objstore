package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/axiomhq/objstore"
)

type httpDoFunc func(*http.Request) (*http.Response, error)

func (f httpDoFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestStatStorageClass(t *testing.T) {
	for _, class := range []string{"", "STANDARD_IA"} {
		t.Run(class, func(t *testing.T) {
			b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					t.Errorf("method %s", r.Method)
				}
				if class != "" {
					w.Header().Set("x-amz-storage-class", class)
				}
				w.Header().Set("Content-Length", "3")
				w.Header().Set("ETag", `"revision"`)
			})
			info, err := b.Stat(t.Context(), "key")
			want := class
			if want == "" {
				want = "STANDARD"
			}
			if err != nil || info.StorageClass != want || info.Size != 3 || info.ETag != `"revision"` {
				t.Fatalf("%+v %v", info, err)
			}
		})
	}
}

func TestStreamingDownloadIntegrity(t *testing.T) {
	late := errors.New("terminal checksum error")
	for _, tc := range []struct {
		name, body, span      string
		off, length, declared int64
		terminal, want        error
	}{
		{"late error", "abcde", "", 0, 0, 5, late, late},
		{"short body", "abc", "", 0, 0, 5, io.EOF, io.ErrUnexpectedEOF},
		{"incomplete range", "cd", "bytes 2-3/10", 2, 4, 2, io.EOF, objstore.ErrRange},
		{"incomplete open range", "cd", "bytes 2-3/10", 2, 0, 2, io.EOF, objstore.ErrRange},
		{"EOF shortened range", "ij", "bytes 8-9/10", 8, 4, 2, io.EOF, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &objectBody{data: []byte(tc.body), finalErr: tc.terminal}
			client := awss3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: httpDoFunc(func(r *http.Request) (*http.Response, error) {
				h := http.Header{"Content-Length": {fmt.Sprint(tc.declared)}}
				if tc.span != "" {
					h.Set("Content-Range", tc.span)
				}
				return &http.Response{StatusCode: 200, Header: h, Body: body, ContentLength: tc.declared, Request: r}, nil
			})})
			b := &Backend{client: client, bucket: "b"}
			r, err := b.NewReader(t.Context(), "key", tc.off, tc.length)
			if err == nil {
				_, err = io.ReadAll(r)
				r.Close()
			}
			if !errors.Is(err, tc.want) || body.closed != 1 {
				t.Fatalf("error=%v want=%v closes=%d", err, tc.want, body.closed)
			}
		})
	}
}

func TestConditionalStreamCommitAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		status     int
		lost       bool
		want       error
	}{
		{"commit", "", 200, false, nil}, {"lost", "PreconditionFailed", 412, true, nil},
		{"conflict", "ConditionalRequestConflict", 409, false, objstore.ErrConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var heads atomic.Int32
			b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads.Add(1)
					w.Header().Set("ETag", `"overwritten"`)
					return
				}
				if r.Method != http.MethodPut || r.URL.RawQuery != "x-id=PutObject" {
					t.Errorf("not a single PUT: %s %s", r.Method, r.URL.RawQuery)
				}
				if r.Header.Get("If-Match") != "old" || r.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "key-id" {
					t.Errorf("missing condition/KMS: %v", r.Header)
				}
				got, err := io.ReadAll(r.Body)
				if err != nil || string(got) != "value" {
					t.Errorf("body: %q %v", got, err)
				}
				if tc.status != 200 {
					s3Error(w, tc.status, tc.code)
					return
				}
				w.Header().Set("ETag", `"committed"`)
			})
			size := int64(5)
			info, ok, err := b.UploadIfMatch(objstore.WithKMSKey(t.Context(), "key-id"), "key", strings.NewReader("value-surplus"), `"old"`, objstore.UploadOptions{Size: &size})
			if !errors.Is(err, tc.want) || ok != (tc.want == nil && !tc.lost) || heads.Load() != 0 {
				t.Fatalf("info=%+v ok=%v err=%v heads=%d", info, ok, err, heads.Load())
			}
			if ok && (info.Size != 5 || info.ETag != `"committed"`) {
				t.Fatalf("wrong commit info: %+v", info)
			}
		})
	}
}

func TestMultipartStreamingAbortAndSourceErrors(t *testing.T) {
	for _, name := range []string{"success", "upstream short", "upstream boundary", "declared short", "completion failure", "cancel"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var parts, aborts, completes atomic.Int32
			var sourceBytes atomic.Int64
			b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				switch {
				case r.Method == "POST" && q.Has("uploads"):
					fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == "PUT" && q.Has("partNumber"):
					n, err := io.Copy(io.Discard, r.Body)
					if err != nil {
						t.Error(err)
					}
					if parts.Add(1) == 1 && name == "success" && (n != uploadPartSize || sourceBytes.Load() > uploadPartSize) {
						t.Errorf("read ahead/part size: n=%d read=%d", n, sourceBytes.Load())
					}
					w.Header().Set("ETag", `"part"`)
					if name == "cancel" {
						cancel()
					}
				case r.Method == "POST" && q.Has("uploadId"):
					completes.Add(1)
					if name == "completion failure" {
						s3Error(w, 400, "InvalidPart")
						return
					}
					fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"multipart-2"</ETag></CompleteMultipartUploadResult>`)
				case r.Method == "DELETE":
					aborts.Add(1)
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
			})
			data := bytes.Repeat([]byte("q"), uploadPartSize+19)
			var source io.Reader = bytes.NewReader(data)
			opts := objstore.UploadOptions{}
			switch name {
			case "upstream short":
				source = io.MultiReader(strings.NewReader("prefix"), errorReader{io.ErrUnexpectedEOF})
			case "upstream boundary":
				source = io.MultiReader(bytes.NewReader(data[:uploadPartSize]), errorReader{io.ErrUnexpectedEOF})
			case "declared short":
				size := int64(len(data) + 1)
				opts.Size = &size
			}
			info, err := b.Upload(ctx, "key", &countSource{Reader: source, n: &sourceBytes}, opts)
			if name == "success" {
				if err != nil || info.Size != int64(len(data)) || info.ETag != `"multipart-2"` || parts.Load() != 2 || aborts.Load() != 0 {
					t.Fatalf("%+v err=%v parts=%d aborts=%d", info, err, parts.Load(), aborts.Load())
				}
			} else {
				if err == nil || aborts.Load() != 1 {
					t.Fatalf("err=%v aborts=%d", err, aborts.Load())
				}
				if name != "completion failure" && completes.Load() != 0 {
					t.Fatal("committed a failed source")
				}
			}
		})
	}
}

type countSource struct {
	io.Reader
	n *atomic.Int64
}

func (r *countSource) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	r.n.Add(int64(n))
	return n, e
}

func TestStreamLimitsAndSigning(t *testing.T) {
	b := fakeS3(t, func(http.ResponseWriter, *http.Request) { t.Error("invalid upload or local signing sent request") })
	for _, size := range []*int64{nil, aws.Int64(MaxConditionalUploadSize + 1)} {
		if _, _, err := b.UploadIfAbsent(t.Context(), "key", strings.NewReader("x"), objstore.UploadOptions{Size: size}); !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("size: %v", err)
		}
	}
	if _, err := b.Upload(t.Context(), "key", strings.NewReader("x"), objstore.UploadOptions{Size: aws.Int64(MaxUploadSize + 1)}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("limit: %v", err)
	}
	req, err := b.Sign(objstore.WithKMSKey(t.Context(), "kms"), "key", objstore.SignOptions{Method: "PUT", Expires: time.Minute, Headers: http.Header{"Content-Type": {"text/plain"}, "Cache-Control": {"no-cache"}}})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(req.URL)
	if req.Method != "PUT" || req.Headers.Get("Content-Type") != "text/plain" || !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "content-type") || req.Headers.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "kms" {
		t.Fatalf("missing signed headers: %v signed=%q", req.Headers, u.Query().Get("X-Amz-SignedHeaders"))
	}
}

func TestDeleteManyAllOutcomes(t *testing.T) {
	b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<DeleteResult><Error><Key>k3</Key><Code>AccessDenied</Code><Message>denied</Message></Error><Error><Key>k8</Key><Code>InternalError</Code><Message>failed</Message></Error></DeleteResult>`)
	})
	keys := make([]string, 1003)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	err := b.DeleteMany(t.Context(), keys...)
	var de *objstore.DeleteError
	if !errors.As(err, &de) || !errors.Is(err, objstore.ErrAccessDenied) || len(de.Failures) != 5 {
		t.Fatalf("outcomes: %v", err)
	}
	for i, want := range []string{"k3", "k8", "k1000", "k1001", "k1002"} {
		if de.Failures[i].Key != want || de.Failures[i].Unattempted != (i >= 2) {
			t.Fatalf("failure %d: %+v", i, de.Failures[i])
		}
	}
}
