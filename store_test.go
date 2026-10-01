package objstore_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

// TestFaultyPassthrough: a disarmed Fault is the backend it wraps — it runs
// the same conformance suite every backend runs.
func TestFaultyPassthrough(t *testing.T) {
	s, _ := bucket.NewFaulty(t)
	storetest.Conformance(t, s)
}

// TestTimingsAttrs: a nil *Timings has no attributes; a live one reports
// every Call by name.
func TestTimingsAttrs(t *testing.T) {
	var none *objstore.Timings
	if attrs := none.Attrs(); attrs != nil {
		t.Fatalf("nil Timings: %v", attrs)
	}
	var tm objstore.Timings
	tm.Since(objstore.CallFsync, time.Now().Add(-2*time.Millisecond))
	attrs := tm.Attrs()
	if len(attrs) != 22 {
		t.Fatalf("%d attrs, want 2 per Call", len(attrs))
	}
	got := map[string]int64{}
	for _, a := range attrs {
		got[a.Key] = a.Value.Int64()
	}
	if got["sys_fsync_n"] != 1 || got["sys_fsync_ms"] < 2 || got["sys_gate_n"] != 0 {
		t.Fatalf("attrs: %v", got)
	}
	if objstore.CallDirSync.String() != "dirsync" || objstore.Call(99).String() != "Call(99)" {
		t.Fatalf("Call.String: %q %q", objstore.CallDirSync, objstore.Call(99))
	}
}

// ignoresIfNoneMatch is a backend that drops the If-None-Match header:
// every PutIfAbsent overwrites and reports that it created the key.
type ignoresIfNoneMatch struct{ objstore.Backend }

func (b ignoresIfNoneMatch) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	return true, b.Put(ctx, key, data)
}

func TestCheckConditionalWrites(t *testing.T) {
	ctx := context.Background()
	s := bucket.New(t)
	if err := s.CheckConditionalWrites(ctx); err != nil {
		t.Fatalf("honest store: %v", err)
	}
	if keys, err := s.List(ctx, "_probe/"); err != nil || len(keys) != 0 {
		t.Fatalf("probe left %v (%v)", keys, err)
	}
	broken := s.WithBackend(func(b objstore.Backend) objstore.Backend { return ignoresIfNoneMatch{b} })
	if err := broken.CheckConditionalWrites(ctx); err == nil || !strings.Contains(err.Error(), "does not honour conditional writes") {
		t.Fatalf("store ignoring If-None-Match: %v", err)
	}
}

// TestKMSKeysDecide: a Store's KMSKeyFunc replaces the key a write's
// context carries, and can read it.
func TestKMSKeysDecide(t *testing.T) {
	ctx := objstore.WithKMSKey(context.Background(), "from-ctx")
	s, k := storetest.NewKMS(bucket.New(t))
	s = s.WithKMSKeys(func(ctx context.Context, key string) (string, error) {
		if key == "plain" {
			return "", nil
		}
		return objstore.KMSKey(ctx), nil
	})
	for _, key := range []string{"plain", "keyed"} {
		if err := s.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if k.KeyOf("plain") != "" || k.KeyOf("keyed") != "from-ctx" {
		t.Fatalf("plain=%q keyed=%q", k.KeyOf("plain"), k.KeyOf("keyed"))
	}
}

// TestIDNamesTheBucket: a wrapped or paced Store keeps its bucket's ID; a
// backend without an ID method gets a fresh one per Open.
func TestIDNamesTheBucket(t *testing.T) {
	root := t.TempDir()
	s := fs.Open(root, "a", objstore.Config{})
	if w := s.WithBackend(func(b objstore.Backend) objstore.Backend { return ignoresIfNoneMatch{b} }); w.ID() != s.ID() {
		t.Fatalf("WithBackend changed the ID: %q, want %q", w.ID(), s.ID())
	}
	if p := fs.Open(root, "a", objstore.Config{RequestsPerSecond: 10}); p.ID() != s.ID() {
		t.Fatalf("paced store ID = %q, want %q", p.ID(), s.ID())
	}
	anon := func() *objstore.Store {
		return objstore.Open(struct{ objstore.Backend }{fs.New(root, "a")}, objstore.Config{})
	}
	if a := anon().ID(); a == "" || a == s.ID() || a == anon().ID() {
		t.Fatalf("backend without ID: %q, want a fresh ID per Open", a)
	}
}

// closer is a backend that records Close.
type closer struct {
	objstore.Backend
	closed int
}

func (c *closer) Close() error { c.closed++; return nil }

// TestCloseReachesBackend: Close reaches the provider's client through
// pacing, and is a no-op for a backend without one.
func TestCloseReachesBackend(t *testing.T) {
	for _, cfg := range []objstore.Config{{}, {RequestsPerSecond: 100}} {
		c := &closer{Backend: fs.New(t.TempDir(), "b")}
		if err := objstore.Open(c, cfg).Close(); err != nil || c.closed != 1 {
			t.Fatalf("pacing %v: Close = %v, backend closed %d times, want once", cfg.RequestsPerSecond, err, c.closed)
		}
	}
	if err := fs.Open(t.TempDir(), "b", objstore.Config{}).Close(); err != nil {
		t.Fatalf("Close without a closer: %v", err)
	}
}
