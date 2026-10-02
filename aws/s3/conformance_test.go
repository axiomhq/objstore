package s3_test

import (
	"os"
	"testing"

	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

// TestConformance runs the shared suite against a real S3 endpoint; it
// skips without OBJSTORE_TEST_S3 (the fake-server tests cover the mapping).
func TestConformance(t *testing.T) {
	endpoint := os.Getenv("OBJSTORE_TEST_S3")
	if endpoint == "" {
		t.Skip("OBJSTORE_TEST_S3 not set")
	}
	storetest.Conformance(t, bucket.NewS3(t, endpoint))
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
	storetest.Conformance(t, bucket.NewS3(t, endpoint))
}
