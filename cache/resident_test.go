package cache

import "testing"

// Resident refuses what does not fit instead of evicting, refuses truncated
// pages, and forgets a namespace's pages and floors on invalidation.
func TestResidentBudgetFloorAndInvalidation(t *testing.T) {
	r := NewResident(100)
	const a, b = "ns/a/wal/", "ns/b/wal/"
	gen := r.Generation.Load()
	if !r.Put(a, 1, a+"1", 1, 60, gen) || r.Put(a, 2, a+"2", 2, 60, gen) {
		t.Fatal("a page past the budget evicted instead of being refused")
	}
	if !r.Put(b, 5, b+"5", 5, 40, gen) || r.Charge() != 100 {
		t.Fatalf("charge %d, want 100", r.Charge())
	}
	r.DropThrough(a, 1)
	if _, ok := r.Get(a + "1"); ok || r.Charge() != 40 {
		t.Fatalf("truncated page kept (charge %d)", r.Charge())
	}
	if r.Put(a, 1, a+"1", 1, 10, gen) {
		t.Fatal("a truncated page was admitted again")
	}
	if !r.Put(a, 2, a+"2", 2, 10, gen) {
		t.Fatal("a live page was refused")
	}
	r.InvalidateNamespace("a")
	if _, ok := r.Get(a + "2"); ok || r.Charge() != 40 {
		t.Fatalf("invalidated page kept (charge %d)", r.Charge())
	}
	if r.Put(a, 1, a+"1", 1, 10, gen) {
		t.Fatal("a publication from before the invalidation was admitted")
	}
	// A recreated namespace starts over: its floor went with it.
	if !r.Put(a, 1, a+"1", 1, 10, r.Generation.Load()) {
		t.Fatal("a recreated namespace's first page was refused")
	}
	if _, ok := r.Get(b + "5"); !ok {
		t.Fatal("another namespace's page was dropped")
	}
	if NewResident(-1).Put(a, 1, a+"1", 1, 1, 0) {
		t.Fatal("a negative budget held a page")
	}
}
