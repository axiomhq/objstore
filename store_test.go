package objstore_test

import (
	"testing"

	"github.com/axiomhq/objstore/storetest"
)

// TestFaultyPassthrough: a disarmed Fault is the backend it wraps — it runs
// the same conformance suite every backend runs.
func TestFaultyPassthrough(t *testing.T) {
	s, _ := storetest.NewFaulty(t)
	storetest.Conformance(t, s)
}
