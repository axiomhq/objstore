package objstore_test

import (
	"testing"
	"time"

	"github.com/axiomhq/objstore"
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
