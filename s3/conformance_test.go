package s3_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/s3"
	"github.com/axiomhq/objstore/storetest"
)

// TestConformance runs the shared suite against a real S3 endpoint; it
// skips without OBJSTORE_TEST_S3 (the fake-server tests cover the mapping).
func TestConformance(t *testing.T) {
	endpoint := os.Getenv("OBJSTORE_TEST_S3")
	if endpoint == "" {
		t.Skip("OBJSTORE_TEST_S3 not set")
	}
	storetest.Conformance(t, openBucket(t, endpoint))
}

// TestR2 runs the shared suite against Cloudflare R2; it skips without
// OBJSTORE_TEST_R2_ENDPOINT (https://<account-id>.r2.cloudflarestorage.com).
// Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (an R2 API
// token) with AWS_REGION=auto.
func TestR2(t *testing.T) {
	endpoint := os.Getenv("OBJSTORE_TEST_R2_ENDPOINT")
	if endpoint == "" {
		t.Skip("OBJSTORE_TEST_R2_ENDPOINT not set")
	}
	storetest.Conformance(t, openBucket(t, endpoint))
}

// openBucket returns a Store on a fresh bucket objstore-test-<nanos>-<random>
// at endpoint, dropped at cleanup. Unset AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY and AWS_REGION default to MinIO's (minioadmin,
// minioadmin, us-east-1); set, they pass through untouched.
func openBucket(t *testing.T, endpoint string) *objstore.Store {
	t.Helper()
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	bucket := fmt.Sprintf("objstore-test-%d-%s", time.Now().UnixNano(), strings.ToLower(rand.Text()[:8]))
	s, err := s3.Open(ctx, s3.Config{Endpoint: endpoint, Bucket: bucket}, objstore.Config{})
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
			t.Logf("drop bucket %s: %v", bucket, err)
		}
	})
	return s
}
