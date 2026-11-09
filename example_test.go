package objstore_test

import (
	"context"
	"fmt"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/mem"
)

// Compare-and-swap: PutIfAbsent claims a key once, and PutIfMatch replaces
// the object only while its ETag is still the one read.
func Example() {
	ctx := context.Background()
	s := mem.Open(objstore.Config{})
	defer s.Close()

	ok, _ := s.PutIfAbsent(ctx, "config", []byte("v1"))
	again, _ := s.PutIfAbsent(ctx, "config", []byte("other"))
	fmt.Println(ok, again)

	body, etag, _ := s.GetWithETag(ctx, "config")
	swapped, _ := s.PutIfMatch(ctx, "config", []byte("v2"), etag)
	stale, _ := s.PutIfMatch(ctx, "config", []byte("v3"), etag)
	fmt.Println(string(body), swapped, stale)
	// Output:
	// true false
	// v1 true false
}
