package cache

import (
	"fmt"
	"strings"
	"testing"
)

// TestLRUEviction pins exact LRU order, so it runs on one stripe.
func TestLRUEviction(t *testing.T) {
	c := NewByteCacheStripes(10, 1)
	c.Put("a", []byte("1234"), c.Generation.Load())
	c.Put("b", []byte("1234"), c.Generation.Load())
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a evicted early")
	} // a is now most-recent
	c.Put("c", []byte("1234"), c.Generation.Load()) // 12 bytes total: evicts LRU, which is b
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should be present")
	}
	c.Put("huge", make([]byte, 11), c.Generation.Load()) // larger than the whole cache: ignored
	if _, ok := c.Get("huge"); ok {
		t.Fatal("oversized value cached")
	}
	hits, misses := c.Stats()
	if hits != 3 || misses != 2 {
		t.Fatalf("stats: hits=%d misses=%d, want 3/2", hits, misses)
	}
	// Immutable values: a second Put on an existing key is a no-op.
	c.Put("a", []byte("XXXX"), c.Generation.Load())
	if v, _ := c.Get("a"); string(v) != "1234" {
		t.Fatalf("immutable value overwritten: %q", v)
	}
}

// text is a decoded value that charges its own size.
type text string

func (t text) CacheBytes() int { return 16 + len(t) }

func TestDecodedRidesOnBytes(t *testing.T) {
	c := NewByteCacheStripes(100, 1)
	c.Put("ids", []byte("raw"), c.Generation.Load())
	c.PutDecoded("ids", text("a,b"), 0, c.Generation.Load())
	got, ok := Decoded[text](c, "ids")
	if !ok || got != "a,b" {
		t.Fatalf("decoded: %+v ok=%v", got, ok)
	}
	// A zero size on a value that is not a Sizer is not retained.
	c.PutDecoded("ids", []string{"a", "b"}, 0, c.Generation.Load())
	if _, ok := Decoded[[]string](c, "ids"); ok {
		t.Fatal("unsized decode retained")
	}
	// Unknown key: no decoded-only insert (would leak past the byte cap).
	c.PutDecoded("missing", 1, 0, c.Generation.Load())
	if _, ok := Decoded[int](c, "missing"); ok {
		t.Fatal("decoded-only insert")
	}
	// Evicting the bytes drops the decoded value with them.
	c.Put("big", make([]byte, 100), c.Generation.Load())
	if _, ok := Decoded[text](c, "ids"); ok {
		t.Fatal("decoded survived byte eviction")
	}
}

// TestDecodedCountsAgainstTheCap: a decode charged to the cache is inside
// the budget, not beside it. A decode can be several times its object
// (float32 vectors against packed bytes), so an uncharged one would make
// "256 MiB" mean an unbounded amount of memory.
func TestDecodedCountsAgainstTheCap(t *testing.T) {
	c := NewByteCacheStripes(100, 1)
	c.Put("a", make([]byte, 20), c.Generation.Load())
	c.PutDecoded("a", "decode", 60, c.Generation.Load()) // 80 of 100 used
	c.Put("b", make([]byte, 30), c.Generation.Load())    // would be 110: evicts a, decode and all
	if _, ok := c.Get("a"); ok {
		t.Fatal("a survived: its decode was not charged")
	}
	// A decode that cannot fit beside its own bytes is refused outright,
	// rather than evicting the whole cache to seat itself.
	c.Put("c", make([]byte, 10), c.Generation.Load())
	c.PutDecoded("c", "huge", 95, c.Generation.Load())
	if _, ok := Decoded[string](c, "c"); ok {
		t.Fatal("oversized decode attached")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("an oversized decode evicted the rest of the cache")
	}
	// Re-decoding the same key replaces the charge instead of doubling it.
	c.PutDecoded("c", "x", 40, c.Generation.Load())
	c.PutDecoded("c", "y", 40, c.Generation.Load())
	if value, ok := Decoded[string](c, "c"); !ok || value != "y" {
		t.Fatalf("repeated decoding evicted a value that fits: %q, %v", value, ok)
	}
}

func TestDecodedRetainedBytesStayWithinCap(t *testing.T) {
	const capBytes = 4096
	c := NewByteCache(capBytes)
	for i := range 100 {
		key := fmt.Sprintf("object-%d", i)
		c.Put(key, make([]byte, 64), c.Generation.Load())
		c.PutDecoded(key, text(strings.Repeat("x", 64)), 0, c.Generation.Load())
	}
	if charge := c.Charge(); charge > capBytes {
		t.Fatalf("retained bytes %d exceed cap %d", charge, capBytes)
	}
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		for e := s.lists[0].head; e != nil; e = e.next {
			if e.decoded != nil && e.dsize != e.decoded.retainedBytes() {
				t.Fatalf("decoded charge %d != concrete size %d", e.dsize, e.decoded.retainedBytes())
			}
		}
		s.mu.Unlock()
	}
}

func TestInvalidationRetiresRawAndDerivedLoads(t *testing.T) {
	c := NewByteCache(4096)
	removed := []string{"ns/x/seg/a", "ns/x/\x00view/a"}
	kept := []string{"ns/xy/seg/a", "ns/xy/\x00view/a", "other/x/seg/a"}
	generation := c.Generation.Load()
	for _, keys := range [][]string{removed, kept} {
		for _, key := range keys {
			c.Put(key, []byte("old"), generation)
			c.PutDecoded(key, text("old"), 0, generation)
		}
	}
	c.InvalidateNamespace("x")
	for _, key := range removed {
		c.Put(key, []byte("retired"), generation)
		c.PutDecoded(key, text("retired"), 0, generation)
		if _, ok := c.Get(key); ok {
			t.Fatalf("retired load resurrected %q", key)
		}
		c.Put(key, []byte("new"), c.Generation.Load())
		c.PutDecoded(key, text("new"), 0, c.Generation.Load())
		c.PutDecoded(key, text("retired"), 0, generation)
		if value, ok := Decoded[text](c, key); !ok || value != "new" {
			t.Fatalf("retired decode replaced %q: %q, %v", key, value, ok)
		}
	}
	for _, key := range kept {
		if value, ok := Decoded[text](c, key); !ok || value != "old" {
			t.Fatalf("invalidation touched another namespace %q", key)
		}
	}
}

func TestDecodedHitsKeepActiveObjectsResident(t *testing.T) {
	c := NewByteCacheStripes(100, 1) // pins LRU order
	generation := c.Generation.Load()
	for _, key := range []string{"active", "idle"} {
		c.Put(key, make([]byte, 20), generation)
		c.PutDecoded(key, key, 20, generation)
	}
	if value, ok := Decoded[string](c, "active"); !ok || value != "active" {
		t.Fatal("active object not readable")
	}
	c.Put("incoming", make([]byte, 40), generation)
	if _, ok := Decoded[string](c, "active"); !ok {
		t.Fatal("decoded hit did not keep the active object resident")
	}
	if _, ok := c.Get("idle"); ok {
		t.Fatal("idle object retained instead of active object")
	}
}

// TestLowEntriesYieldToRegular: a low-priority object fills only what the
// regular entries leave free; it never evicts a decoded view.
func TestLowEntriesYieldToRegular(t *testing.T) {
	isLow := func(key string) bool { return strings.HasPrefix(key, "ns/a/table/") }
	c := NewByteCacheStripes(1000, 1)
	c.SetLow(isLow, 100)
	for i := range 8 {
		c.PutValue(fmt.Sprintf("ns/a/\x00view/%d", i), i, 100, c.Generation.Load())
	}
	c.Put("ns/a/table/1", make([]byte, 600), c.Generation.Load())
	for i := range 8 {
		if _, ok := Decoded[any](c, fmt.Sprintf("ns/a/\x00view/%d", i)); !ok {
			t.Fatalf("view %d evicted by a low object", i)
		}
	}
	c.Put("ns/a/table/2", make([]byte, 150), c.Generation.Load())
	if _, ok := c.Peek("ns/a/table/2"); !ok {
		t.Fatal("a low object that fits the free room was refused")
	}
}
