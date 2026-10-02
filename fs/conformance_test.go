//go:build unix && !aix && (!solaris || illumos)

package fs_test

import (
	"context"
	"slices"
	"testing"

	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

func TestConformance(t *testing.T) {
	storetest.Conformance(t, bucket.NewFS(t))
}

// TestFSRejectsTraversal: keys become filesystem paths — a trust boundary
// the S3 backend never had. Terminal .lock/.tmp-* names are reserved for
// backend internals, traversal and empty elements are rejected, and other
// nested dot names remain available to namespace keys.
func TestFSRejectsTraversal(t *testing.T) {
	s := bucket.NewFS(t)
	for _, key := range []string{"../evil", "a/../../evil", "a/./b", "/abs", "", "a//b", "a/", ".lock", ".tmp-1", "a/.lock", "a/.tmp-1"} {
		if err := s.Put(context.Background(), key, []byte("x")); err == nil {
			t.Fatalf("traversal key %q accepted", key)
		}
	}
	if err := s.Put(context.Background(), "a/b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "a/.namespace", []byte("x")); err != nil {
		t.Fatalf("nested dot name: %v", err)
	}
	keys, _, err := s.ListPage(context.Background(), "a/", "", 10)
	if err != nil || !slices.Contains(keys, "a/.namespace") {
		t.Fatalf("nested dot name missing from list: %v, %v", keys, err)
	}
	if b, err := s.Get(context.Background(), "a//b"); err == nil {
		t.Fatalf("a//b aliased onto a/b: %q", b)
	}
}
