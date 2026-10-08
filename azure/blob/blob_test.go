package blob

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

// This credential is only for the disposable local emulator, never Azure.
func emulatorCredential(t *testing.T) *container.SharedKeyCredential {
	t.Helper()
	c, err := container.NewSharedKeyCredential("objstore", base64.StdEncoding.EncodeToString([]byte("objstore-emulator-not-secret")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func fakeBlob(t *testing.T, handler http.HandlerFunc) *Backend {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := container.NewClientWithSharedKeyCredential(srv.URL+"/objstore/b", emulatorCredential(t), &container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: srv.Client(), Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Client: c})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func azureError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
	fmt.Fprintf(w, `<Error><Code>%s</Code><Message>fixture</Message></Error>`, code)
}

func TestUploadCommitConditionAndKMS(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
		want   error
		ok     bool
	}{
		{"commit", 201, "", nil, true}, {"lost", 412, "ConditionNotMet", nil, false},
		{"conflict", 409, "BlobBeingRehydrated", objstore.ErrConflict, false}, {"denied", 403, "AuthorizationFailure", objstore.ErrAccessDenied, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var heads, commits atomic.Int32
			b := fakeBlob(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "HEAD" {
					heads.Add(1)
					w.Header().Set("ETag", `"newer"`)
					return
				}
				io.Copy(io.Discard, r.Body)
				if r.URL.Query().Get("comp") == "block" {
					w.WriteHeader(201)
					return
				}
				commits.Add(1)
				if r.Header.Get("If-Match") != `"old"` || r.Header.Get("x-ms-meta-owner") != "test" || r.Header.Get("x-ms-blob-cache-control") != "no-store" {
					t.Errorf("commit headers: %v", r.Header)
				}
				if tc.status != 201 {
					azureError(w, tc.status, tc.code)
					return
				}
				w.Header().Set("ETag", `"commit"`)
				w.WriteHeader(201)
			})
			info, ok, err := b.UploadIfMatch(t.Context(), "key", strings.NewReader("abc"), `"old"`, objstore.UploadOptions{Metadata: map[string]string{"owner": "test"}, CacheControl: "no-store"})
			if !errors.Is(err, tc.want) || ok != tc.ok || heads.Load() != 0 || commits.Load() != 1 {
				t.Fatalf("%+v %v %v heads=%d commits=%d", info, ok, err, heads.Load(), commits.Load())
			}
			if ok && (info.Size != 3 || info.ETag != `"commit"`) {
				t.Fatalf("wrong commit info: %+v", info)
			}
			if _, err := b.Upload(objstore.WithKMSKey(t.Context(), "unsupported"), "key", strings.NewReader("x"), objstore.UploadOptions{}); !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("KMS silently ignored: %v", err)
			}
		})
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestLeasePreconditionIsNotCASMiss(t *testing.T) {
	for _, code := range []string{"LeaseIdMissing", "LeaseLost", "LeaseIdMismatchWithBlobOperation"} {
		t.Run(code, func(t *testing.T) {
			b := fakeBlob(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				azureError(w, 412, code)
			})
			_, ok, err := b.UploadIfMatch(t.Context(), "key", strings.NewReader("data"), `"old"`, objstore.UploadOptions{})
			var response *azcore.ResponseError
			if ok || !errors.As(err, &response) || response.StatusCode != 412 || response.ErrorCode != code {
				t.Fatalf("lease failure treated as CAS miss: %v %v", ok, err)
			}
		})
	}
}

type downloadTransport struct{ body io.ReadCloser }

func (d downloadTransport) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": {"5"}}, ContentLength: 5, Body: d.body, Request: req}, nil
}

func TestDownloadTerminalError(t *testing.T) {
	terminal := errors.New("terminal checksum failure")
	c, err := container.NewClientWithNoCredential("https://fixture.invalid/b", &container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: downloadTransport{io.NopCloser(io.MultiReader(strings.NewReader("abcde"), failingReader{terminal}))}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Client: c})
	if err != nil {
		t.Fatal(err)
	}
	r, err := b.NewReader(t.Context(), "key", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, terminal) {
		t.Fatalf("lost terminal error: %v", err)
	}
}

func TestUploadFailedSourceDoesNotCommit(t *testing.T) {
	for _, boundary := range []int{7, 8 << 20} {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			var commits atomic.Int32
			b := fakeBlob(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if r.URL.Query().Get("comp") != "block" {
					commits.Add(1)
				}
				w.WriteHeader(201)
			})
			source := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("x"), boundary)), failingReader{io.ErrUnexpectedEOF})
			_, err := b.Upload(t.Context(), "key", source, objstore.UploadOptions{})
			if !errors.Is(err, io.ErrUnexpectedEOF) || commits.Load() != 0 {
				t.Fatalf("err=%v commits=%d", err, commits.Load())
			}
		})
	}
}

func TestRangeAndDeleteOutcomes(t *testing.T) {
	b := fakeBlob(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Range", "bytes 2-3/10")
			w.Header().Set("Content-Length", "2")
			w.WriteHeader(206)
			fmt.Fprint(w, "cd")
			return
		}
		azureError(w, 403, "AuthorizationFailure")
	})
	if _, err := b.NewReader(t.Context(), "key", 2, 4); !errors.Is(err, objstore.ErrRange) {
		t.Fatalf("accepted incomplete range: %v", err)
	}
	err := b.DeleteMany(t.Context(), "one", "two", "three")
	var de *objstore.DeleteError
	if !errors.As(err, &de) || len(de.Failures) != 3 || !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatalf("delete: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = b.DeleteMany(ctx, "one", "two")
	if !errors.As(err, &de) || len(de.Failures) != 2 || !de.Failures[0].Unattempted || !de.Failures[1].Unattempted {
		t.Fatalf("cancel: %v", err)
	}
}

func TestAzuriteConformance(t *testing.T) {
	endpoint := os.Getenv("OBJSTORE_TEST_AZURE")
	if endpoint == "" {
		t.Skip("OBJSTORE_TEST_AZURE not set (Azurite URL, e.g. http://127.0.0.1:10000)")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	name := fmt.Sprintf("objstore-%d", time.Now().UnixNano())
	c, err := container.NewClientWithSharedKeyCredential(endpoint+"/objstore/"+name, emulatorCredential(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Client: c}, objstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := s.DropBucket(ctx); err != nil {
			t.Error(err)
		}
	})
	storetest.Conformance(t, s)
	storetest.StreamingConformance(t, s, "Hot")
	req, err := s.Sign(ctx, "signed", objstore.SignOptions{Method: "PUT", Expires: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := http.NewRequestWithContext(ctx, req.Method, req.URL, strings.NewReader("signed body"))
	r.Header = req.Headers
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("signed PUT: %s", resp.Status)
	}
	req, err = s.Sign(ctx, "signed", objstore.SignOptions{Method: "GET", Expires: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	r, _ = http.NewRequestWithContext(ctx, req.Method, req.URL, nil)
	resp, err = http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || string(data) != "signed body" {
		t.Fatalf("signed GET: %q %s %v", data, resp.Status, err)
	}
}
