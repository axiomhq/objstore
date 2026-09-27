package storetest_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/fs"
	"github.com/axiomhq/objstore/storetest"
)

func TestFaultModes(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()

	t.Run("NthCallOnly", func(t *testing.T) {
		f.Set(storetest.Plan{Op: storetest.OpPut, N: 2})
		if err := s.Put(ctx, "n/1", []byte("a")); err != nil {
			t.Fatalf("first call must pass: %v", err)
		}
		if err := s.Put(ctx, "n/2", []byte("b")); !errors.Is(err, storetest.ErrFault) {
			t.Fatalf("second call: %v", err)
		}
		if err := s.Put(ctx, "n/3", []byte("c")); err != nil {
			t.Fatalf("third call must pass: %v", err)
		}
		if f.Fired() != 1 {
			t.Fatalf("fired %d times, want 1", f.Fired())
		}
		// Fail mode never touches storage.
		if _, err := s.Get(ctx, "n/2"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("failed Put still landed: %v", err)
		}
	})

	t.Run("KeyFilter", func(t *testing.T) {
		f.Set(storetest.Plan{Op: storetest.OpDelete, N: 1, Key: "/seg/"})
		if err := s.DeleteMany(ctx, "ns/x/wal/1", "ns/x/wal/2"); err != nil {
			t.Fatalf("non-matching keys must pass: %v", err)
		}
		if err := s.DeleteMany(ctx, "ns/x/seg/1/docs.json"); !errors.Is(err, storetest.ErrFault) {
			t.Fatalf("matching keys: %v", err)
		}
	})

	t.Run("Ambiguous", func(t *testing.T) {
		f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Ambiguous})
		ok, err := s.PutIfAbsent(ctx, "amb", []byte("landed"))
		if !errors.Is(err, storetest.ErrFault) {
			t.Fatalf("want ErrFault, got ok=%v err=%v", ok, err)
		}
		// The whole point: the bytes are there, the caller was not told.
		got, err := s.Get(ctx, "amb")
		if err != nil || string(got) != "landed" {
			t.Fatalf("ambiguous write did not land: %q %v", got, err)
		}
	})

	t.Run("Hang", func(t *testing.T) {
		f.Set(storetest.Plan{Op: storetest.OpGet, N: 1, Mode: storetest.Hang})
		hctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		if _, err := s.Get(hctx, "amb"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
		if time.Since(start) < 50*time.Millisecond {
			t.Fatal("Hang returned without blocking")
		}
	})

	t.Run("ConcurrentCountingIsExact", func(t *testing.T) {
		f.Set(storetest.Plan{Op: storetest.OpPut, N: 7, Key: "race/"})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var faults int
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := s.Put(ctx, "race/"+string(rune('a'+i)), []byte("x")); errors.Is(err, storetest.ErrFault) {
					mu.Lock()
					faults++
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		if faults != 1 || f.Fired() != 1 {
			t.Fatalf("32 concurrent puts produced %d faults (Fired=%d), want exactly 1", faults, f.Fired())
		}
	})

	f.Clear()
	if err := s.Put(ctx, "after-clear", []byte("x")); err != nil {
		t.Fatalf("Clear did not disarm: %v", err)
	}
}

// TestFaultReadBytes: ReadBytes is the payload side of Ops — whole reads,
// ranged reads and ETag reads all count what they returned, and ResetOps
// zeroes it with the call counts.
func TestFaultReadBytes(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "rb", make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	f.ResetOps()
	if _, err := s.Get(ctx, "rb"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRange(ctx, "rb", 10, 30); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetWithETag(ctx, "rb"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "rb-missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if got := f.ReadBytes(); got != 230 {
		t.Fatalf("ReadBytes = %d, want 230 (100 + 30 + 100; a miss returns nothing)", got)
	}
	f.ResetOps()
	if got := f.ReadBytes(); got != 0 {
		t.Fatalf("ReadBytes after ResetOps = %d", got)
	}
}

func TestFaultWriteBytes(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "write", make([]byte, 7)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.PutIfAbsent(ctx, "write", make([]byte, 11)); err != nil || ok {
		t.Fatalf("failed conditional put: ok=%v err=%v", ok, err)
	}
	if ok, err := s.PutIfAbsent(ctx, "other", make([]byte, 13)); err != nil || !ok {
		t.Fatalf("successful conditional put: ok=%v err=%v", ok, err)
	}
	if got := f.WriteBytes(); got != 20 {
		t.Fatalf("WriteBytes = %d, want 20", got)
	}
	f.ResetOps()
	if got := f.WriteBytes(); got != 0 {
		t.Fatalf("WriteBytes after ResetOps = %d", got)
	}
}

func TestFaultShapeIsDeterministicAndBounded(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "shape", make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	f.SetShape(storetest.Shape{Latency: time.Millisecond, BytesPerSecond: 1_000_000, ErrorRate: .01, Seed: 7})
	start := time.Now()
	faults := 0
	for range 200 {
		if _, err := s.Get(ctx, "shape"); errors.Is(err, storetest.ErrFault) {
			faults++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if faults != 2 || f.ShapeErrors() != faults {
		t.Fatalf("seeded 1%% profile produced %d faults, counter=%d, want 2", faults, f.ShapeErrors())
	}
	if time.Since(start) < 300*time.Millisecond {
		t.Fatal("latency and bandwidth shaping were not applied")
	}
	f.SetShape(storetest.Shape{})
	start = time.Now()
	if _, err := s.Get(ctx, "shape"); err != nil {
		t.Fatal(err)
	}
	// 1 ms latency plus 1 ms of bandwidth per call when shaped: a disabled
	// shaper must not come near the 300 ms the loop above took. Loose
	// enough for a loaded -race CI box.
	if time.Since(start) > time.Second {
		t.Fatal("disabled shaper retained delay")
	}
}

// TestFaultSetShapeRacesCalls: a test flips the shaper while the store it
// wraps is mid-request. The swap is under the injector's mutex, so under
// -race this is the whole assertion; it runs on file:// so it needs no MinIO.
func TestFaultSetShapeRacesCalls(t *testing.T) {
	ctx := context.Background()
	raw := fs.Open(t.TempDir(), "shape-race", objstore.Config{})
	if err := raw.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	s, f := storetest.NewFault(raw)
	if err := s.Put(ctx, "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if _, err := s.Get(ctx, "k"); err != nil && !errors.Is(err, storetest.ErrFault) {
					t.Error(err)
					return
				}
				f.ShapeErrors()
			}
		}()
	}
	for i := range 200 {
		if i%2 == 0 {
			f.SetShape(storetest.Shape{ErrorRate: .5, Seed: int64(i)})
		} else {
			f.SetShape(storetest.Shape{})
		}
	}
	wg.Wait()
}

func TestConditionalReadFaultAndMetering(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "conditional", []byte("data")); err != nil {
		t.Fatal(err)
	}
	_, etag, _, err := s.GetIfChanged(ctx, "conditional", "")
	if err != nil {
		t.Fatal(err)
	}
	f.ResetOps()
	if _, _, same, err := s.GetIfChanged(ctx, "conditional", etag); err != nil || !same {
		t.Fatalf("same=%v err=%v", same, err)
	}
	if f.Ops()[storetest.OpGet] != 1 || f.ReadBytes() != 0 || f.ReadKeys()["conditional"] != 1 {
		t.Fatalf("ops=%v bytes=%d keys=%v", f.Ops(), f.ReadBytes(), f.ReadKeys())
	}
	for _, mode := range []storetest.Mode{storetest.Fail, storetest.Ambiguous, storetest.Hang} {
		f.Set(storetest.Plan{Op: storetest.OpGet, Key: "conditional", N: 1, Mode: mode})
		cctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		data, current, same, err := s.GetIfChanged(cctx, "conditional", etag)
		cancel()
		if err == nil || same || data != nil || current != "" {
			t.Fatalf("mode=%v: %q %q %v %v", mode, data, current, same, err)
		}
	}
	f.Clear()
	f.SetShape(storetest.Shape{ErrorRate: 1})
	if _, _, same, err := s.GetIfChanged(ctx, "conditional", etag); err == nil || same {
		t.Fatalf("shaped error: same=%v err=%v", same, err)
	}
}

func TestWriteKeysCountsLandedConditionalPages(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	const key = "ns/root/wal/00000000000000000001"
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, Key: "/wal/", N: 1, Mode: storetest.Ambiguous})
	if ok, err := s.PutIfAbsent(ctx, key, []byte("page")); !ok || !errors.Is(err, storetest.ErrFault) {
		t.Fatalf("ambiguous: %v %v", ok, err)
	}
	if ok, err := s.PutIfAbsent(ctx, key, []byte("page")); ok || err != nil {
		t.Fatalf("retry: %v %v", ok, err)
	}
	if got := f.WriteKeys(); len(got) != 1 || got[key] != 1 {
		t.Fatalf("landed pages: %v", got)
	}
	if f.Ops()[storetest.OpPutIfAbsent] != 2 {
		t.Fatal(f.Ops())
	}
	got := f.WriteKeys()
	got[key] = 99
	if f.WriteKeys()[key] != 1 {
		t.Fatal("snapshot aliases internal map")
	}
	f.ResetOps()
	if len(f.WriteKeys()) != 0 {
		t.Fatal("reset retained writes")
	}
	f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Fail})
	if _, err := s.PutIfAbsent(ctx, key+"2", []byte("page")); !errors.Is(err, storetest.ErrFault) {
		t.Fatal(err)
	}
	if len(f.WriteKeys()) != 0 {
		t.Fatal("failed put counted as landed")
	}
}

// TestShapedReadReturnsNothing: a read the shaper fails hands back no data
// and meters no bytes, like a read the backend failed.
func TestShapedReadReturnsNothing(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "shaped", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	f.ResetOps()
	f.SetShape(storetest.Shape{ErrorRate: 1})
	if b, err := s.Get(ctx, "shaped"); !errors.Is(err, storetest.ErrFault) || b != nil {
		t.Fatalf("Get: %q %v", b, err)
	}
	if b, etag, err := s.GetWithETag(ctx, "shaped"); !errors.Is(err, storetest.ErrFault) || b != nil || etag != "" {
		t.Fatalf("GetWithETag: %q %q %v", b, etag, err)
	}
	if b, err := s.GetRange(ctx, "shaped", 0, 3); !errors.Is(err, storetest.ErrFault) || b != nil {
		t.Fatalf("GetRange: %q %v", b, err)
	}
	if n := f.ReadBytes(); n != 0 {
		t.Fatalf("shaped failures metered %d bytes", n)
	}
}

func TestFaultPauseResume(t *testing.T) {
	ctx := context.Background()

	t.Run("Resume", func(t *testing.T) {
		s, f := storetest.NewFaulty(t)
		f.Set(storetest.Plan{Op: storetest.OpPut, N: 1, Mode: storetest.Pause, Key: "paused"})
		done := make(chan error, 1)
		go func() { done <- s.Put(ctx, "paused", []byte("x")) }()
		for f.Fired() == 0 {
			time.Sleep(time.Millisecond)
		}
		select {
		case err := <-done:
			t.Fatalf("paused Put returned before Resume: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if _, err := s.Get(ctx, "paused"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("paused Put reached storage: %v", err)
		}
		f.Resume()
		if err := <-done; err != nil {
			t.Fatalf("resumed Put: %v", err)
		}
		if _, err := s.Get(ctx, "paused"); err != nil {
			t.Fatalf("resumed Put did not land: %v", err)
		}
		f.Resume() // nothing paused: a no-op
	})

	t.Run("ContextEnds", func(t *testing.T) {
		s, f := storetest.NewFaulty(t)
		f.Set(storetest.Plan{Op: storetest.OpPut, N: 1, Mode: storetest.Pause})
		pctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if err := s.Put(pctx, "paused", []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Put paused past its deadline: %v", err)
		}
		if _, err := s.Get(ctx, "paused"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("a Put abandoned while paused landed: %v", err)
		}
	})
}

func TestFaultMatchFrom(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ranged", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	// From: 0 alone is a wildcard; MatchFrom makes it offset 0 exactly.
	f.Set(storetest.Plan{Op: storetest.OpGetRange, N: 1, From: 0, MatchFrom: true})
	if _, err := s.GetRange(ctx, "ranged", 4, 2); err != nil {
		t.Fatalf("offset 4 matched a From 0 plan: %v", err)
	}
	if _, err := s.GetRange(ctx, "ranged", 0, 2); !errors.Is(err, storetest.ErrFault) {
		t.Fatalf("offset 0: %v", err)
	}
	f.Set(storetest.Plan{Op: storetest.OpGetRange, N: 1})
	if _, err := s.GetRange(ctx, "ranged", 4, 2); !errors.Is(err, storetest.ErrFault) {
		t.Fatalf("wildcard plan skipped offset 4: %v", err)
	}
	f.Set(storetest.Plan{Op: storetest.OpGetRange, N: 1, From: 4})
	if _, err := s.GetRange(ctx, "ranged", 0, 2); err != nil {
		t.Fatalf("offset 0 matched a From 4 plan: %v", err)
	}
	if _, err := s.GetRange(ctx, "ranged", 4, 2); !errors.Is(err, storetest.ErrFault) {
		t.Fatalf("offset 4: %v", err)
	}
}

func TestFaultWatchRewrites(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "before", []byte("a")); err != nil {
		t.Fatal(err)
	}
	f.WatchRewrites()
	for _, w := range []struct{ key, data string }{
		{"before", "b"},              // first write seen by the ledger: not a rewrite
		{"same", "x"}, {"same", "x"}, // identical bytes: not a rewrite
		{"changed", "1"}, {"changed", "2"},
		{"deleted", "1"},
	} {
		if err := s.Put(ctx, w.key, []byte(w.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete(ctx, "deleted"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "deleted", []byte("2")); err != nil { // re-minted after delete
		t.Fatal(err)
	}
	if got := f.Rewrites(); len(got) != 1 || got[0] != "changed" {
		t.Fatalf("Rewrites = %v, want [changed]", got)
	}
}

func TestFaultReadKeys(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	for _, k := range []string{"rk/a", "rk/b"} {
		if err := s.Put(ctx, k, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	f.ResetOps()
	s.Get(ctx, "rk/a")            //nolint:errcheck
	s.GetRange(ctx, "rk/a", 0, 1) //nolint:errcheck
	s.GetWithETag(ctx, "rk/b")    //nolint:errcheck
	s.Get(ctx, "rk/missing")      //nolint:errcheck
	got := f.ReadKeys()
	if len(got) != 3 || got["rk/a"] != 2 || got["rk/b"] != 1 || got["rk/missing"] != 1 {
		t.Fatalf("ReadKeys = %v", got)
	}
	got["rk/a"] = 99
	if f.ReadKeys()["rk/a"] != 2 {
		t.Fatal("ReadKeys aliases internal map")
	}
	f.ResetOps()
	if len(f.ReadKeys()) != 0 {
		t.Fatal("ResetOps kept read keys")
	}
}

func TestFaultListPrefixes(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	if err := s.Put(ctx, "lp/one/x", []byte("x")); err != nil {
		t.Fatal(err)
	}
	f.ResetOps()
	if _, err := s.ListPrefixes(ctx, "lp/"); err != nil {
		t.Fatal(err)
	}
	if ops := f.Ops(); ops[storetest.OpListPrefixes] != 1 || ops[storetest.OpList] != 0 {
		t.Fatalf("ops = %v, want one OpListPrefixes and no OpList", ops)
	}
	f.Set(storetest.Plan{Op: storetest.OpListPrefixes, N: 1, Key: "lp/"})
	if _, err := s.ListPrefixes(ctx, "other/"); err != nil {
		t.Fatalf("non-matching prefix: %v", err)
	}
	if _, err := s.ListPrefixes(ctx, "lp/"); !errors.Is(err, storetest.ErrFault) {
		t.Fatalf("planned ListPrefixes: %v", err)
	}
}

// TestFaultZeroValue: the zero Fault's bookkeeping is usable.
func TestFaultZeroValue(t *testing.T) {
	var f storetest.Fault
	f.Set(storetest.Plan{Op: storetest.OpPut, N: 1})
	if len(f.Ops()) != 0 || f.Fired() != 0 || len(f.ReadKeys()) != 0 || len(f.Rewrites()) != 0 {
		t.Fatal("zero Fault is not empty")
	}
	f.ResetOps()
	f.Resume()
	f.Clear()
}
