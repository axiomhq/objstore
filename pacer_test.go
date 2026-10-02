package objstore

import (
	"context"
	"testing"
	"time"
)

type putOnly struct{ Backend }

func (putOnly) Put(ctx context.Context, key string, _ []byte) error {
	return OpErr("put", key, ctx.Err())
}

func TestPacerCancellationAndUnpaced(t *testing.T) {
	ctx := context.Background()
	// A cancelled context does not wait out its slot.
	s := Open(putOnly{}, Config{RequestsPerSecond: 1})
	if err := s.Put(ctx, "k", nil); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := s.Put(cctx, "k", nil); err == nil {
		t.Fatal("want the context error while waiting for a slot a second away")
	}
	if err := Open(putOnly{}, Config{}).Put(ctx, "k", nil); err != nil {
		t.Fatal("unpaced put must not wait for a slot")
	}
}

// TestOpenPacesUnlessUrgent: Open wraps the backend in the pacer when
// RequestsPerSecond is set, and an Urgent context skips it.
func TestOpenPacesUnlessUrgent(t *testing.T) {
	ctx := context.Background()
	s := Open(putOnly{}, Config{RequestsPerSecond: 200}) // 5 ms apart
	if _, ok := s.b.(*paced); !ok {
		t.Fatalf("backend %T, want *paced", s.b)
	}
	start := time.Now()
	for range 6 {
		if err := s.Put(ctx, "k", nil); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 25*time.Millisecond {
		t.Fatalf("6 paced puts at 200/s took %v, want >= 25 ms", el)
	}
	s = Open(putOnly{}, Config{RequestsPerSecond: 1})
	if err := s.Put(ctx, "k", nil); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for range 5 {
		if err := s.Put(Urgent(ctx), "k", nil); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("urgent puts at 1/s took %v, want no pacing", el)
	}
	if _, ok := Open(putOnly{}, Config{}).b.(*paced); ok {
		t.Fatal("unpaced Open wrapped the backend")
	}
}
