package rangeread

import (
	"bytes"
	"testing"

	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

// TestReadCoalescesAndSplits: nearby and overlapping extents of one object
// share a GET, an extent longer than MaxRangeBytes is assembled from
// several, and every extent gets exactly its own bytes.
func TestReadCoalescesAndSplits(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
	a := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	b := bytes.Repeat([]byte("ABCDEFGH"), 4) // 32 bytes
	for k, v := range map[string][]byte{"a": a, "b": b} {
		if err := s.Put(t.Context(), k, v); err != nil {
			t.Fatal(err)
		}
	}
	extents := []Extent{
		{"a", 2, 4},  // 2-6
		{"a", 4, 6},  // overlaps the first
		{"a", 12, 3}, // 2-byte gap after 4-10: coalesced
		{"b", 1, 30}, // longer than MaxRangeBytes: split
		{"a", 2, 4},  // duplicate
	}
	fault.ResetOps()
	got, err := Read(t.Context(), s, extents, Config{MaxGapBytes: 2, MaxRangeBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	src := map[string][]byte{"a": a, "b": b}
	for i, x := range extents {
		if want := src[x.Object][x.Offset : x.Offset+x.Length]; !bytes.Equal(got[i], want) {
			t.Fatalf("extent %d %+v = %q, want %q", i, x, got[i], want)
		}
	}
	if n := fault.Ops()[storetest.OpGetRange]; n != 3 {
		t.Fatalf("range GETs = %d, want 3: one for a, two for b", n)
	}
}
