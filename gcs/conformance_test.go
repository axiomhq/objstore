// fake-gcs-server builds only on these platforms.
//go:build linux || windows || darwin || freebsd || netbsd || openbsd

package gcs_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/gcs"
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
