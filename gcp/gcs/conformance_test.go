// fake-gcs-server builds only on these platforms.
//go:build linux || windows || darwin || freebsd || netbsd || openbsd

package gcs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/gcp/gcs"
	"github.com/axiomhq/objstore/storetest"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/option"
)

// TestConformance runs the shared suite against an in-process
// fake-gcs-server. The fake ignores read preconditions, so the plain run
// covers GetIfChanged's same-generation fallback and the "304" run puts a
// transport in front that answers ifGenerationNotMatch the way GCS does.
func TestConformance(t *testing.T) {
	for _, notModified := range []bool{false, true} {
		name := "fake"
		if notModified {
			name = "fake-304"
		}
		t.Run(name, func(t *testing.T) {
			server := fakestorage.NewServer(nil)
			t.Cleanup(server.Stop)
			hc := server.HTTPClient()
			if notModified {
				hc = &http.Client{Transport: notModifiedTransport{hc.Transport}}
			}
			s, err := gcs.Open(context.Background(), gcs.Config{
				Bucket:    "conformance",
				ProjectID: "test",
				Options: []option.ClientOption{
					option.WithEndpoint(server.URL() + "/storage/v1/"),
					option.WithoutAuthentication(),
					option.WithHTTPClient(hc),
				},
			}, objstore.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.EnsureBucket(context.Background()); err != nil {
				t.Fatal(err)
			}
			storetest.Conformance(t, s)
		})
	}
}

// notModifiedTransport turns a media download whose generation equals
// ifGenerationNotMatch into GCS's 304.
type notModifiedTransport struct{ next http.RoundTripper }

func (n notModifiedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := n.next.RoundTrip(req)
	q := req.URL.Query()
	want := q.Get("ifGenerationNotMatch")
	if err != nil || want == "" || q.Get("alt") != "media" ||
		res.StatusCode/100 != 2 || res.Header.Get("X-Goog-Generation") != want {
		return res, err
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return &http.Response{
		Status: "304 Not Modified", StatusCode: http.StatusNotModified,
		Proto: res.Proto, ProtoMajor: res.ProtoMajor, ProtoMinor: res.ProtoMinor,
		Header: http.Header{}, Body: http.NoBody, Request: req,
	}, nil
}

// TestConformanceReal runs the suite against real GCS in a fresh bucket
// objstore-test-<nanos>, created in OBJSTORE_TEST_GCS_PROJECT with
// Application Default Credentials and dropped afterwards.
func TestConformanceReal(t *testing.T) {
	project := os.Getenv("OBJSTORE_TEST_GCS_PROJECT")
	if project == "" {
		t.Skip("OBJSTORE_TEST_GCS_PROJECT not set")
	}
	ctx := context.Background()
	s, err := gcs.Open(ctx, gcs.Config{
		Bucket:    fmt.Sprintf("objstore-test-%d", time.Now().UnixNano()),
		ProjectID: project,
	}, objstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.DropBucket(context.Background()); err != nil {
			t.Error(err)
		}
	})
	storetest.Conformance(t, s)
}

// kmsTransport records the kmsKeyName of each upload and, while deny is
// set, answers every request 403 as GCS does for a disabled key.
type kmsTransport struct {
	next http.RoundTripper
	mu   sync.Mutex
	keys []string
	deny bool
}

func (k *kmsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	k.mu.Lock()
	deny := k.deny
	if strings.Contains(req.URL.Path, "/upload/") {
		k.keys = append(k.keys, req.URL.Query().Get("kmsKeyName"))
	}
	k.mu.Unlock()
	if deny {
		return &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:  http.Header{"Content-Type": []string{"application/json"}},
			Body:    io.NopCloser(strings.NewReader(`{"error":{"code":403,"message":"Permission denied on Cloud KMS key"}}`)),
			Request: req}, nil
	}
	return k.next.RoundTrip(req)
}

// TestKMSKeyPerObject: the Store's WithKMSKeys rides on the upload as
// kmsKeyName, an unkeyed object carries none, and a 403 is ErrAccessDenied.
func TestKMSKeyPerObject(t *testing.T) {
	server := fakestorage.NewServer(nil)
	t.Cleanup(server.Stop)
	tr := &kmsTransport{next: server.HTTPClient().Transport}
	s, err := gcs.Open(context.Background(), gcs.Config{
		Bucket: "kms", ProjectID: "test",
		Options: []option.ClientOption{
			option.WithEndpoint(server.URL() + "/storage/v1/"),
			option.WithoutAuthentication(),
			option.WithHTTPClient(&http.Client{Transport: tr}),
		},
	}, objstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.KMS() {
		t.Fatal("GCS store must report per-object keys")
	}
	const key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
	s = s.WithKMSKeys(func(_ context.Context, object string) (string, error) {
		if object == "secret" {
			return key, nil
		}
		return "", nil
	})
	for _, object := range []string{"secret", "plain"} {
		if err := s.Put(ctx, object, []byte(object)); err != nil {
			t.Fatal(err)
		}
	}
	if len(tr.keys) != 2 || tr.keys[0] != key || tr.keys[1] != "" {
		t.Fatalf("upload kmsKeyName: %q", tr.keys)
	}
	tr.mu.Lock()
	tr.deny = true
	tr.mu.Unlock()
	if _, err := s.Get(ctx, "secret"); !errors.Is(err, objstore.ErrAccessDenied) {
		t.Fatalf("GET under a denied key: %v", err)
	}
}
