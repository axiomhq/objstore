package cache

import (
	"fmt"
	"strings"
	"testing"
)

// TestLRUEviction pins exact LRU order, so it runs on one stripe.
func TestLRUEviction(t *testing.T) {
	c := NewByteCacheStripes(2*(4+1+EntryOverhead)+10, 1) // two 4-byte entries fit, three do not
	c.Put("a", []byte("1234"), c.GenerationOf("a"))
	c.Put("b", []byte("1234"), c.GenerationOf("b"))
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a evicted early")
	} // a is now most-recent
	c.Put("c", []byte("1234"), c.GenerationOf("c")) // 12 bytes total: evicts LRU, which is b
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should be present")
	}
	c.Put("huge", make([]byte, c.Cap), c.GenerationOf("huge")) // larger than the whole cache: ignored
	if _, ok := c.Get("huge"); ok {
		t.Fatal("oversized value cached")
	}
	hits, misses := c.Stats()
	if hits != 3 || misses != 2 {
		t.Fatalf("stats: hits=%d misses=%d, want 3/2", hits, misses)
	}
	// Immutable values: a second Put on an existing key is a no-op.
	c.Put("a", []byte("XXXX"), c.GenerationOf("a"))
	if v, _ := c.Get("a"); string(v) != "1234" {
		t.Fatalf("immutable value overwritten: %q", v)
	}
}

// text is a decoded value that charges its own size.
type text string

func (t text) CacheBytes() int { return 16 + len(t) }

func TestDecodedRidesOnBytes(t *testing.T) {
	c := NewByteCacheStripes(200, 1)
	c.Put("ids", []byte("raw"), c.GenerationOf("ids"))
	c.PutDecoded("ids", text("a,b"), 0, c.GenerationOf("ids"))
	got, ok := Decoded[text](c, "ids")
	if !ok || got != "a,b" {
		t.Fatalf("decoded: %+v ok=%v", got, ok)
	}
	// A zero size on a value that is not a Sizer is not retained.
	c.PutDecoded("ids", []string{"a", "b"}, 0, c.GenerationOf("ids"))
	if _, ok := Decoded[[]string](c, "ids"); ok {
		t.Fatal("unsized decode retained")
	}
	// Unknown key: no decoded-only insert (would leak past the byte cap).
	c.PutDecoded("missing", 1, 0, c.GenerationOf("missing"))
	if _, ok := Decoded[int](c, "missing"); ok {
		t.Fatal("decoded-only insert")
	}
	// Evicting the bytes drops the decoded value with them.
	c.Put("big", make([]byte, 100), c.GenerationOf("big"))
	if _, ok := Decoded[text](c, "ids"); ok {
		t.Fatal("decoded survived byte eviction")
	}
}

// TestDecodedCountsAgainstTheCap: a decode charged to the cache is inside
// the budget, not beside it. A decode can be several times its object
// (float32 vectors against packed bytes), so an uncharged one would make
// "256 MiB" mean an unbounded amount of memory.
func TestDecodedCountsAgainstTheCap(t *testing.T) {
	const o = 1 + EntryOverhead // every key here is one byte
	c := NewByteCacheStripes(100+2*o, 1)
	c.Put("a", make([]byte, 20), c.GenerationOf("a"))
	c.PutDecoded("a", "decode", 60, c.GenerationOf("a")) // 80+o of 100+2o used
	c.Put("b", make([]byte, 30), c.GenerationOf("b"))    // would be 110+2o: evicts a, decode and all
	if _, ok := c.Get("a"); ok {
		t.Fatal("a survived: its decode was not charged")
	}
	// A decode that cannot fit beside its own bytes is refused outright,
	// rather than evicting the whole cache to seat itself.
	c.Put("c", make([]byte, 10), c.GenerationOf("c"))
	c.PutDecoded("c", "huge", 95+o, c.GenerationOf("c"))
	if _, ok := Decoded[string](c, "c"); ok {
		t.Fatal("oversized decode attached")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("an oversized decode evicted the rest of the cache")
	}
	// Re-decoding the same key replaces the charge instead of doubling it.
	c.PutDecoded("c", "x", 40, c.GenerationOf("c"))
	c.PutDecoded("c", "y", 40, c.GenerationOf("c"))
	if value, ok := Decoded[string](c, "c"); !ok || value != "y" {
		t.Fatalf("repeated decoding evicted a value that fits: %q, %v", value, ok)
	}
}

func TestDecodedRetainedBytesStayWithinCap(t *testing.T) {
	const capBytes = 4096
	c := NewByteCache(capBytes)
	for i := range 100 {
		key := fmt.Sprintf("object-%d", i)
		c.Put(key, make([]byte, 64), c.GenerationOf(key))
		c.PutDecoded(key, text(strings.Repeat("x", 64)), 0, c.GenerationOf(key))
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
	c := NewByteCache(1 << 16)
	removed := []string{"ns/x/seg/a", "ns/x/\x00view/a"}
	kept := []string{"ns/xy/seg/a", "ns/xy/\x00view/a", "other/x/seg/a"}
	generation := c.GenerationOf("ns/x/seg/a") // every namespace starts at the same generation
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
		c.Put(key, []byte("new"), c.GenerationOf(key))
		c.PutDecoded(key, text("new"), 0, c.GenerationOf(key))
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
	c := NewByteCacheStripes(300, 1) // pins LRU order; active and idle fit, not a third
	generation := c.GenerationOf("active")
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
	c := NewByteCacheStripes(1700, 1) // eight views and table/2 fit, not table/1
	c.SetLow(isLow, 100)
	for i := range 8 {
		key := fmt.Sprintf("ns/a/\x00view/%d", i)
		c.PutValue(key, i, 100, c.GenerationOf(key))
	}
	c.Put("ns/a/table/1", make([]byte, 600), c.GenerationOf("ns/a/table/1"))
	for i := range 8 {
		if _, ok := Decoded[any](c, fmt.Sprintf("ns/a/\x00view/%d", i)); !ok {
			t.Fatalf("view %d evicted by a low object", i)
		}
	}
	c.Put("ns/a/table/2", make([]byte, 150), c.GenerationOf("ns/a/table/2"))
	if _, ok := c.Peek("ns/a/table/2"); !ok {
		t.Fatal("a low object that fits the free room was refused")
	}
}

// TestEntryOverheadBoundsEmptyValues: an empty value still costs its key and
// bookkeeping, so a flood of them cannot grow the cache without bound.
func TestEntryOverheadBoundsEmptyValues(t *testing.T) {
	c := NewByteCacheStripes(10*(8+EntryOverhead), 1)
	for i := range 1000 {
		key := fmt.Sprintf("k%07d", i)
		c.Put(key, nil, c.GenerationOf(key))
	}
	if n := len(c.stripes[0].items); n != 10 {
		t.Fatalf("%d empty entries resident, want 10", n)
	}
	if got := c.Charge(); got > c.Cap {
		t.Fatalf("charge %d over cap %d", got, c.Cap)
	}
}

func TestNewByteCacheStripesRoundsUp(t *testing.T) {
	for in, want := range map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 16: 16, 17: 32} {
		if got := len(NewByteCacheStripes(1<<10, in).stripes); got != want {
			t.Errorf("stripes(%d) = %d, want %d", in, got, want)
		}
	}
	defer func() {
		if p := recover(); p == nil || !strings.Contains(fmt.Sprint(p), "stripes must be positive") {
			t.Fatalf("zero stripes: panic %v", p)
		}
	}()
	NewByteCacheStripes(1<<10, 0)
}

// TestInvalidateNamespaceIsScoped: invalidating one namespace retires its
// generation alone. Another namespace's in-flight loads still publish and
// its disk entries stay reachable.
func TestInvalidateNamespaceIsScoped(t *testing.T) {
	c := New(nil, 1<<20, nil, Keys{})
	a, b := "ns/a/obj", "ns/b/obj"
	ga, gb := c.Memory.GenerationOf(a), c.Memory.GenerationOf(b)
	c.InvalidateNamespace("a")
	if c.Memory.GenerationOf(a) == ga {
		t.Fatal("invalidated namespace kept its generation")
	}
	if c.Memory.GenerationOf(b) != gb || c.Memory.GenerationOf("other/obj") != 0 {
		t.Fatal("invalidation moved another namespace's generation")
	}
	c.Memory.Put(a, []byte("stale"), ga)
	c.Memory.Put(b, []byte("fresh"), gb)
	if _, ok := c.Memory.Peek(a); ok {
		t.Fatal("a load from before the invalidation published")
	}
	if _, ok := c.Memory.Peek(b); !ok {
		t.Fatal("another namespace's load was retired")
	}
	if DiskKey(b, c.Memory.GenerationOf(b)) != DiskKey(b, gb) {
		t.Fatal("another namespace's disk key moved")
	}
}
