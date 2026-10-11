package mem_test

import (
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/mem"
	"github.com/axiomhq/objstore/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Conformance(t, mem.Open(objstore.Config{}))
}

// TestBuffersAreCopied: a caller reusing its buffer, or editing what Get
// returned, does not change the stored object.
func TestBuffersAreCopied(t *testing.T) {
	s := mem.Open(objstore.Config{})
	b := []byte("v1")
	if err := s.Put(t.Context(), "k", b); err != nil {
		t.Fatal(err)
	}
	b[0] = 'X'
	got, _ := s.Get(t.Context(), "k")
	got[1] = 'X'
	if again, _ := s.Get(t.Context(), "k"); string(again) != "v1" {
		t.Fatalf("stored object = %q, want v1", again)
	}
}
