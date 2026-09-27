package objstore

import (
	"context"
	"testing"
	"time"
)

func TestPacerSpacesRequestsInCallOrder(t *testing.T) {
	p := newPacer(200) // 5 ms apart
	ctx := context.Background()
	start := time.Now()
	for range 6 {
		if err := p.wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 25*time.Millisecond {
		t.Fatalf("6 requests at 200/s took %v, want >= 25 ms", el)
	}
	// A cancelled context does not wait out its slot.
	p = newPacer(1)
	if err := p.wait(ctx); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := p.wait(cctx); err == nil {
		t.Fatal("want the context error while waiting for a slot a second away")
	}
	var unpaced *pacer
	if err := unpaced.wait(ctx); err != nil {
		t.Fatal("nil pacer must be a no-op")
	}
}
