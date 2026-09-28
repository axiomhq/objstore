package storetest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
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

// NewFS returns a Store on a file bucket under t.TempDir(). It skips t on
// a platform the file backend does not support (no flock).
func NewFS(t testing.TB) *objstore.Store {
	t.Helper()
	s := fs.Open(t.TempDir(), "bucket", objstore.Config{})
	if err := s.EnsureBucket(context.Background()); errors.Is(err, errors.ErrUnsupported) {
		t.Skip(err)
	} else if err != nil {
		t.Fatal(err)
	}
	return s
}

// NewS3 returns a Store on a fresh bucket objstore-test-<nanos>-<random>
// at the S3 endpoint, dropped at cleanup. Each of AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY and AWS_REGION left unset defaults to MinIO's
// (minioadmin, minioadmin, us-east-1), set with t.Setenv (so such a test cannot
// call t.Parallel); set, the environment's credentials (R2, AWS) pass
// through untouched. Opening and creating the bucket is bounded by a
// minute.
func NewS3(t testing.TB, endpoint string) *objstore.Store {
	t.Helper()
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The random suffix keeps two test binaries started in the same
	// nanosecond (go test ./... runs packages in parallel) apart.
	bucket := fmt.Sprintf("objstore-test-%d-%s", time.Now().UnixNano(), strings.ToLower(rand.Text()[:8]))
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
// crash-point tests. Disarmed until the caller sets a Plan. Cleanup
// resumes any call still held by a Pause plan and waits (up to 5s) for
// every paused or hung call to return, so a failed test neither leaks a
// blocked goroutine nor lets one write into a removed TempDir. A Hang call
// ends only with its context: use t.Context() (cancelled before cleanup),
// never context.Background().
func NewFaulty(t testing.TB) (*objstore.Store, *Fault) {
	t.Helper()
	s, f := NewFault(New(t))
	t.Cleanup(func() {
		f.Resume()
		if !f.drain(5 * time.Second) {
			t.Logf("storetest: %d paused or hung calls still blocked at cleanup", f.blocked.Load())
		}
	})
	return s, f
}
