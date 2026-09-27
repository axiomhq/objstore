package storetest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
)

// New returns a Store on a fresh bucket. OBJSTORE_TEST_S3 names an S3
// endpoint (for MinIO, export OBJSTORE_TEST_S3=http://localhost:9000);
// unset, a per-test file backend under t.TempDir() runs the same suite.
func New(t testing.TB) *objstore.Store {
	t.Helper()
	endpoint := os.Getenv("OBJSTORE_TEST_S3")
	if endpoint == "" {
		endpoint = "file://" + t.TempDir()
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	t.Setenv("AWS_REGION", "us-east-1")
	ctx := context.Background()
	bucket := fmt.Sprintf("objstore-test-%d", time.Now().UnixNano())
	s, err := objstore.New(ctx, endpoint, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	// Tear the bucket down with the test: a dev MinIO carrying thousands of
	// leftover buckets got slow enough to miss lease renewals (2026-09-05).
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := s.DropBucket(ctx); err != nil {
			t.Logf("storetest: drop bucket %s: %v", bucket, err)
		}
	})
	return s
}

// NewFaulty returns New(t)'s Store wrapped in a fault injector, for
// crash-point tests. Disarmed until the caller sets a Plan.
func NewFaulty(t testing.TB) (*objstore.Store, *Fault) {
	t.Helper()
	return NewFault(New(t))
}
