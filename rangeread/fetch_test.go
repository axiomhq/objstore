package rangeread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/axiomhq/objstore/cache"
	"github.com/axiomhq/objstore/storetest"
)

func TestSingleChildPlanIsNotCachedTwice(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	disk, err := cache.NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<20, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/single/object"
	whole := []byte("prefix-one-exact-child-suffix")
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	load := Load{Extent: Extent{Object: object, Offset: 7, Length: 15}, Key: object + "#probe"}
	r := newReader(t, s, objects, Config{})
	fault.ResetOps()
	ctx, err := r.FetchRanges(t.Context(), []Load{load})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := cache.Scoped(ctx, load.Key)
	if !ok || !bytes.Equal(got, whole[7:22]) || cap(got) != len(got) {
		t.Fatalf("child = %q, present=%v, cap=%d", got, ok, cap(got))
	}
	if _, ok := objects.Memory.Peek(load.Key); !ok {
		t.Fatal("child was not stored in memory")
	}
	if charge, want := objects.Memory.Charge(), charged(load); charge != want {
		t.Fatalf("memory charge = %d, want %d", charge, want)
	}
	generation := objects.Memory.GenerationOf(load.Key)
	if disk.Stats().Entries != 1 || !disk.Has(cache.DiskKey(load.Key, generation)) {
		t.Fatal("disk tier retained parent or missed child")
	}
	if r.IO.Gets.Load() != 1 || fault.Ops()[storetest.OpGetRange] != 1 {
		t.Fatalf("range GETs = %d, store ops = %v", r.IO.Gets.Load(), fault.Ops())
	}
}

// TestCoalescedParentIsNotCached: two children read in one coalesced range
// are cached; the parent range, gap included, is not, in either tier.
func TestCoalescedParentIsNotCached(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	disk, err := cache.NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<20, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/merged/object"
	whole := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	r := newReader(t, s, objects, Config{})
	fault.ResetOps()
	ctx, err := r.FetchRanges(t.Context(), loads)
	if err != nil {
		t.Fatal(err)
	}
	for _, load := range loads {
		got, ok := cache.Scoped(ctx, load.Key)
		want := whole[load.Offset : load.Offset+load.Length]
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("%s = %q, want %q", load.Key, got, want)
		}
	}
	if fault.Ops()[storetest.OpGetRange] != 1 {
		t.Fatalf("store ops = %v, want one coalesced range", fault.Ops())
	}
	if charge, want := objects.Memory.Charge(), charged(loads...); charge != want {
		t.Fatalf("memory charge = %d, want %d: the two 6-byte children only", charge, want)
	}
	if st := disk.Stats(); st.Entries != 2 {
		t.Fatalf("disk entries = %d, want the two children only", st.Entries)
	}
}

// TestCoalescedChildrenOfACachedObjectStayOffDisk: children cut from a
// range the disk tier already holds inside a whole object are cached in
// memory, not copied to disk a second time.
func TestCoalescedChildrenOfACachedObjectStayOffDisk(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	disk, err := cache.NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<20, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/warm/object"
	whole := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	disk.Put(cache.DiskKey(object, objects.Memory.GenerationOf(object)), whole)
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	r := newReader(t, s, objects, Config{})
	fault.ResetOps()
	ctx, err := r.FetchRanges(t.Context(), loads)
	if err != nil {
		t.Fatal(err)
	}
	for _, load := range loads {
		if got, ok := cache.Scoped(ctx, load.Key); !ok || !bytes.Equal(got, whole[load.Offset:load.Offset+load.Length]) {
			t.Fatalf("%s = %q", load.Key, got)
		}
	}
	if n := len(fault.Ops()); n != 0 {
		t.Fatalf("store ops = %v, want none", fault.Ops())
	}
	if st := disk.Stats(); st.Entries != 1 {
		t.Fatalf("disk entries = %d, want the object alone", st.Entries)
	}
}

// TestConcurrentColdParentsShareOneGet: N queries missing the same children
// at once plan the same coalesced parent; they share one store GET, and the
// parent is still not cached.
func TestConcurrentColdParentsShareOneGet(t *testing.T) {
	const queries = 8
	s, fault := storetest.NewFault(storetest.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/shared/object"
	whole := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	r := newReader(t, s, objects, Config{})
	joined := make(chan struct{}, queries)
	r.joined = func() { joined <- struct{}{} }
	fault.ResetOps()
	fault.Set(storetest.Plan{Op: storetest.OpGetRange, Key: object, N: 1, Mode: storetest.Pause})
	errs := make(chan error, queries)
	for range queries {
		go func() {
			ctx, err := r.FetchRanges(t.Context(), loads)
			if err == nil {
				for _, load := range loads {
					if got, ok := cache.Scoped(ctx, load.Key); !ok || !bytes.Equal(got, whole[load.Offset:load.Offset+load.Length]) {
						err = fmt.Errorf("%s = %q", load.Key, got)
					}
				}
			}
			errs <- err
		}()
	}
	for range queries { // every query has joined the one paused GET
		<-joined
	}
	fault.Resume()
	for range queries {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := fault.Ops()[storetest.OpGetRange]; got != 1 {
		t.Fatalf("%d concurrent cold queries made %d range GETs, want 1", queries, got)
	}
	if charge, want := objects.Memory.Charge(), charged(loads...); charge != want {
		t.Fatalf("memory charge = %d, want %d: the two 6-byte children only", charge, want)
	}
}

// TestSharedParentOutlivesItsLeader: a query that joined another query's
// parent GET does not inherit that query's cancellation.
func TestSharedParentOutlivesItsLeader(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/leader/object"
	whole := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	r := newReader(t, s, objects, Config{})
	joined := make(chan struct{}, 4)
	r.joined = func() { joined <- struct{}{} }
	fault.Set(storetest.Plan{Op: storetest.OpGetRange, Key: object, N: 1, Mode: storetest.Hang})
	leaderCtx, cancel := context.WithCancel(t.Context())
	leader, follower := make(chan error, 1), make(chan error, 1)
	go func() { _, err := r.FetchRanges(leaderCtx, loads); leader <- err }()
	<-joined
	go func() {
		ctx, err := r.FetchRanges(t.Context(), loads)
		if got, ok := cache.Scoped(ctx, loads[1].Key); err == nil && (!ok || !bytes.Equal(got, whole[12:18])) {
			err = fmt.Errorf("child = %q", got)
		}
		follower <- err
	}()
	<-joined // the follower joined the hung GET
	cancel()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v, want context.Canceled", err)
	}
	if err := <-follower; err != nil {
		t.Fatalf("follower inherited the leader's cancellation: %v", err)
	}
}

// TestSharedParentFollowerRetriesTheLeadersError: the shared read runs
// under the leader's context, so its error can be the leader's own (a
// per-request budget); a follower retries once for itself, and the leader
// keeps its error.
func TestSharedParentFollowerRetriesTheLeadersError(t *testing.T) {
	var r Reader
	joined := make(chan struct{}, 4)
	r.joined = func() { joined <- struct{}{} }
	x := Extent{Object: "o", Offset: 0, Length: 4}
	started, release := make(chan struct{}), make(chan struct{})
	own := errors.New("the leader's own budget")
	leader := make(chan error, 1)
	go func() {
		_, _, _, err := r.sharedParent(t.Context(), x, func(context.Context) ([]byte, cache.Outcome, error) {
			close(started)
			<-release
			return nil, cache.Load, own
		})
		leader <- err
	}()
	<-started
	<-joined
	follower := make(chan error, 1)
	go func() {
		data, _, _, err := r.sharedParent(t.Context(), x, func(context.Context) ([]byte, cache.Outcome, error) {
			return []byte("data"), cache.Load, nil
		})
		if err == nil && string(data) != "data" {
			err = fmt.Errorf("follower read %q", data)
		}
		follower <- err
	}()
	<-joined // the follower joined the leader's flight
	close(release)
	if err := <-leader; !errors.Is(err, own) {
		t.Fatalf("leader: %v, want its own error", err)
	}
	if err := <-follower; err != nil {
		t.Fatalf("follower inherited the leader's error: %v", err)
	}
}

// TestTransientLoadsAreServedOnlyFromTheScope: a transient child is in the
// returned scope and nowhere else, alone in its plan or coalesced with a
// neighbour, and costs the memory tier no charge.
func TestTransientLoadsAreServedOnlyFromTheScope(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	disk, err := cache.NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<20, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/rows/table.sst"
	whole := bytes.Repeat([]byte("0123456789abcdef"), 64)
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	r := newReader(t, s, objects, Config{})
	for name, loads := range map[string][]Load{
		"alone":     {{Extent: Extent{Object: object, Offset: 16, Length: 8}, Key: object + "#row1", Transient: true}},
		"coalesced": {{Extent: Extent{Object: object, Offset: 100, Length: 8}, Key: object + "#row2", Transient: true}, {Extent: Extent{Object: object, Offset: 300, Length: 8}, Key: object + "#row3", Transient: true}},
	} {
		fault.ResetOps()
		before := r.IO.Gets.Load()
		ctx, err := r.FetchRanges(t.Context(), loads)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, load := range loads {
			got, ok := cache.Scoped(ctx, load.Key)
			if !ok || !bytes.Equal(got, whole[load.Offset:load.Offset+load.Length]) {
				t.Fatalf("%s: %s = %q, present=%v", name, load.Key, got, ok)
			}
			if _, ok := objects.Memory.Peek(load.Key); ok {
				t.Fatalf("%s: %s was cached in memory", name, load.Key)
			}
			if disk.Has(cache.DiskKey(load.Key, objects.Memory.GenerationOf(load.Key))) {
				t.Fatalf("%s: %s was written to disk", name, load.Key)
			}
		}
		if r.IO.Gets.Load()-before != 1 || fault.Ops()[storetest.OpGetRange] != 1 {
			t.Fatalf("%s: range GETs = %d, store ops = %v; want one coalesced read", name, r.IO.Gets.Load()-before, fault.Ops())
		}
	}
	if charge := objects.Memory.Charge(); charge != 0 || disk.Stats().Entries != 0 {
		t.Fatalf("memory charge %d, disk entries %d after transient reads", charge, disk.Stats().Entries)
	}
}

// TestTransientLoadsAreDecoded: R1. A transient child is decoded like any
// other, alone in its plan or coalesced with a neighbour.
func TestTransientLoadsAreDecoded(t *testing.T) {
	s := storetest.New(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/decode/table.sst"
	whole := []byte("abcdefghijklmnopqrstuvwxyz")
	if err := s.Put(t.Context(), object, whole); err != nil {
		t.Fatal(err)
	}
	r := newReader(t, s, objects, Config{})
	upper := func(b []byte) ([]byte, error) { return bytes.ToUpper(b), nil }
	for name, loads := range map[string][]Load{
		"alone":     {{Extent: Extent{Object: object, Offset: 2, Length: 4}, Key: object + "#t1", Decode: upper, Transient: true}},
		"coalesced": {{Extent: Extent{Object: object, Offset: 8, Length: 3}, Key: object + "#t2", Decode: upper, Transient: true}, {Extent: Extent{Object: object, Offset: 14, Length: 3}, Key: object + "#t3", Decode: upper, Transient: true}},
		"mixed":     {{Extent: Extent{Object: object, Offset: 20, Length: 2}, Key: object + "#t4", Decode: upper, Transient: true}, {Extent: Extent{Object: object, Offset: 23, Length: 2}, Key: object + "#c5", Decode: upper}},
	} {
		ctx, err := r.FetchRanges(t.Context(), loads)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, load := range loads {
			want := bytes.ToUpper(whole[load.Offset : load.Offset+load.Length])
			if got, ok := cache.Scoped(ctx, load.Key); !ok || !bytes.Equal(got, want) {
				t.Fatalf("%s: %s = %q, want %q", name, load.Key, got, want)
			}
		}
	}
	// A cacheable child alone in its plan is decoded once, through the cache.
	calls := 0
	count := func(b []byte) ([]byte, error) { calls++; return bytes.ToUpper(b), nil }
	load := Load{Extent: Extent{Object: object, Offset: 0, Length: 2}, Key: object + "#once", Decode: count}
	ctx, err := r.FetchRanges(t.Context(), []Load{load})
	if got, _ := cache.Scoped(ctx, load.Key); err != nil || string(got) != "AB" || calls != 1 {
		t.Fatalf("direct child: %q %v, decoded %d times", got, err, calls)
	}
}

// TestZeroConfigReader: R2. New with a zero Config gets the defaults, so
// FetchRanges does real work instead of skipping every stage.
func TestZeroConfigReader(t *testing.T) {
	s := storetest.New(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/zero/object"
	if err := s.Put(t.Context(), object, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	r, err := New(s, objects, Config{})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := (Config{}).Normalized()
	if r.Config != want {
		t.Fatalf("Config = %+v, want %+v", r.Config, want)
	}
	load := Load{Extent: Extent{Object: object, Offset: 3, Length: 4}, Key: object + "#zero"}
	ctx, err := r.FetchRanges(t.Context(), []Load{load})
	if got, ok := cache.Scoped(ctx, load.Key); err != nil || !ok || string(got) != "3456" {
		t.Fatalf("FetchRanges on a zero Config: %q %v %v", got, ok, err)
	}
	if _, err := New(s, objects, Config{Concurrency: -1}); err == nil {
		t.Fatal("New accepted a negative Concurrency")
	}
}

// TestGetRangeErrorFailsTheStage: a store error surfaces from FetchRanges
// and caches nothing; the next stage reads again.
func TestGetRangeErrorFailsTheStage(t *testing.T) {
	s, fault := storetest.NewFault(storetest.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/fail/object"
	if err := s.Put(t.Context(), object, []byte("0123456789abcdefghij")); err != nil {
		t.Fatal(err)
	}
	r := newReader(t, s, objects, Config{})
	for name, loads := range map[string][]Load{
		"direct":    {{Extent: Extent{Object: object, Offset: 0, Length: 4}, Key: object + "#d"}},
		"coalesced": {{Extent: Extent{Object: object, Offset: 5, Length: 2}, Key: object + "#c1"}, {Extent: Extent{Object: object, Offset: 9, Length: 2}, Key: object + "#c2"}},
	} {
		fault.Set(storetest.Plan{Op: storetest.OpGetRange, Key: object, N: 1})
		if _, err := r.FetchRanges(t.Context(), loads); !errors.Is(err, storetest.ErrFault) {
			t.Fatalf("%s: %v, want ErrFault", name, err)
		}
		for _, load := range loads {
			if _, ok := objects.Memory.Peek(load.Key); ok {
				t.Fatalf("%s: %s cached after a failed read", name, load.Key)
			}
		}
		ctx, err := r.FetchRanges(t.Context(), loads)
		if err != nil {
			t.Fatalf("%s: retry: %v", name, err)
		}
		for _, load := range loads {
			if _, ok := cache.Scoped(ctx, load.Key); !ok {
				t.Fatalf("%s: retry missed %s", name, load.Key)
			}
		}
	}
}

func TestInvalidLoadIsErrInvalidExtent(t *testing.T) {
	s := storetest.New(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	for _, load := range []Load{
		{Extent: Extent{Object: "o", Offset: -1, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 0, Length: 0}, Key: "k"},
		{Extent: Extent{Object: "o#x", Offset: 0, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "", Offset: 0, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 0, Length: 1}, Key: "k", DecodedBytes: -1},
	} {
		_, err := r.FetchRanges(t.Context(), []Load{load})
		if !errors.Is(err, ErrInvalidExtent) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("%+v: %v, want ErrInvalidExtent", load, err)
		}
	}
}

// charged is what the memory tier charges for loads cached as stored.
func charged(loads ...Load) int {
	n := 0
	for _, load := range loads {
		n += int(load.Length) + len(load.Key) + cache.EntryOverhead
	}
	return n
}
