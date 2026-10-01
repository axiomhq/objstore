package bucket_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestNewS3DoesNotChangeEnvironment(t *testing.T) {
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_SESSION_TOKEN"} {
		t.Setenv(key, "")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "Credential=minioadmin/") || !strings.Contains(auth, "/us-east-1/s3/") {
			t.Error("request did not use MinIO defaults")
		}
		if r.Method == http.MethodGet {
			fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
		}
	}))
	t.Cleanup(srv.Close)
	bucket.NewS3(t, srv.URL)
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"} {
		if os.Getenv(key) != "" {
			t.Errorf("NewS3 changed %s", key)
		}
	}
}
