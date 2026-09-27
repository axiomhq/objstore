package storetest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/s3"
)

// New returns a Store on a fresh bucket. OBJSTORE_TEST_S3 names an S3
// endpoint (for MinIO, export OBJSTORE_TEST_S3=http://localhost:9000) and
// New is NewS3 on it; unset, New is NewFS and the same suite runs on a
// per-test file backend.
func New(t testing.TB) *objstore.Store {
	t.Helper()
	if endpoint := os.Getenv("OBJSTORE_TEST_S3"); endpoint != "" {
		return NewS3(t, endpoint)
	}
	return NewFS(t)
}

// NewFS returns a Store on a file bucket under t.TempDir().
func NewFS(t testing.TB) *objstore.Store {
	t.Helper()
	s := fs.Open(t.TempDir(), "bucket", objstore.Config{})
	if err := s.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// NewS3 returns a Store on a fresh bucket objstore-test-<nanos> at the S3
// endpoint, dropped at cleanup. Without AWS_ACCESS_KEY_ID it uses MinIO's
// default credentials; set, the environment's credentials (R2, AWS) pass
// through.
func NewS3(t testing.TB, endpoint string) *objstore.Store {
	t.Helper()
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
		t.Setenv("AWS_REGION", "us-east-1")
	}
	ctx := context.Background()
	bucket := fmt.Sprintf("objstore-test-%d", time.Now().UnixNano())
	s, err := s3.Open(ctx, s3.Config{Endpoint: endpoint, Bucket: bucket}, objstore.Config{})
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
