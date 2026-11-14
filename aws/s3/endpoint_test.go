package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"

	"github.com/axiomhq/objstore"
)

// checkedEndpoint resolves through the backend's cache and through the SDK's
// rules, and fails the test when they differ. It records each operation.
type checkedEndpoint struct {
	t      *testing.T
	cached *staticEndpoint
	rules  awss3.EndpointResolverV2
	mu     sync.Mutex
	ops    map[string]bool
}

func (c *checkedEndpoint) ResolveEndpoint(ctx context.Context, p awss3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	got, err := c.cached.ResolveEndpoint(ctx, p)
	want, wantErr := c.rules.ResolveEndpoint(ctx, p)
	op := awsmiddleware.GetOperationName(ctx)
	if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
		c.t.Errorf("%s: cached endpoint %+v, %v; the rules give %+v, %v", op, got, err, want, wantErr)
	}
	c.mu.Lock()
	c.ops[op] = true
	c.mu.Unlock()
	return got, err
}

// Over every operation the backend sends, concurrently, the cached endpoint
// equals the rules' own, and the signer's writes into each request's copy
// race with nothing (run under -race). An SDK upgrade that makes another
// part of the endpoint mutable fails here.
func TestEndpointCacheMatchesRules(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("ETag", `"e"`)
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/b":
			w.WriteHeader(http.StatusNotFound) // EnsureBucket then creates it
		case q.Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>`)
		case q.Has("uploadId") && r.Method == http.MethodPost:
			fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"e"</ETag></CompleteMultipartUploadResult>`)
		case q.Get("list-type") == "2":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
		case q.Has("delete"):
			fmt.Fprint(w, `<DeleteResult></DeleteResult>`)
		case r.Header.Get("Range") != "":
			w.Header().Set("Content-Range", "bytes 0-0/1")
			w.WriteHeader(http.StatusPartialContent)
			w.Write([]byte("x"))
		case r.Method == http.MethodGet:
			w.Write([]byte("x"))
		}
	}))
	defer srv.Close()
	b, err := New(t.Context(), Config{Endpoint: srv.URL, Bucket: "b", AWS: &aws.Config{Region: "auto",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, nil
		})}})
	if err != nil {
		t.Fatal(err)
	}
	opts := b.client.Options()
	check := &checkedEndpoint{t: t, cached: opts.EndpointResolverV2.(*staticEndpoint), rules: awss3.NewDefaultEndpointResolverV2(), ops: map[string]bool{}}
	opts.EndpointResolverV2 = check
	b.client = awss3.New(opts)
	ctx := t.Context()
	small, large := int64(1), int64(uploadPartSize+1)
	calls := []func(){
		func() { b.Put(ctx, "k", []byte("x")) },
		func() { b.PutIfAbsent(ctx, "k", []byte("x")) },
		func() { b.PutIfMatch(ctx, "k", []byte("x"), "e") },
		func() { b.Get(ctx, "k") },
		func() { b.GetRange(ctx, "k", 0, 1) },
		func() { b.GetWithETag(ctx, "k") },
		func() { b.GetIfChanged(ctx, "k", "e") },
		func() { b.ListPage(ctx, "p/", "", 1) },
		func() { b.ListPrefixesPage(ctx, "p/", "", 1) },
		func() { b.List(ctx, "p/") },
		func() { b.ListPrefixes(ctx, "p/") },
		func() { b.Delete(ctx, "k") },
		func() { b.DeleteMany(ctx, "k", "l") },
		func() { b.EnsureBucket(ctx) },
		func() { b.DropBucket(ctx) },
		func() { b.Stat(ctx, "k") },
		func() {
			if rc, err := b.NewReader(ctx, "k", 0, 1); err == nil {
				rc.Close()
			}
		},
		func() { b.Upload(ctx, "k", bytes.NewReader([]byte("x")), objstore.UploadOptions{Size: &small}) },
		func() { b.Upload(ctx, "k", bytes.NewReader(make([]byte, large)), objstore.UploadOptions{Size: &large}) },
		func() { b.UploadIfAbsent(ctx, "k", bytes.NewReader([]byte("x")), objstore.UploadOptions{Size: &small}) },
		func() {
			b.UploadIfMatch(ctx, "k", bytes.NewReader([]byte("x")), "e", objstore.UploadOptions{Size: &small})
		},
		func() { b.Sign(ctx, "k", objstore.SignOptions{Method: http.MethodGet, Expires: time.Minute}) },
		func() { b.Sign(ctx, "k", objstore.SignOptions{Method: http.MethodPut, Expires: time.Minute}) },
	}
	var wg sync.WaitGroup
	for range 16 {
		for _, call := range calls {
			wg.Go(call)
		}
	}
	wg.Wait()
	ops := slices.Sorted(func(yield func(string) bool) {
		for op := range check.ops {
			if !yield(op) {
				return
			}
		}
	})
	want := "CompleteMultipartUpload,CreateBucket,CreateMultipartUpload,DeleteBucket,DeleteObject,DeleteObjects,GetObject,HeadBucket,HeadObject,ListObjectsV2,PutObject,UploadPart"
	if got := strings.Join(ops, ","); got != want {
		t.Errorf("operations resolved %s, want %s", got, want)
	}
}
