package rangeread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
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
	if charge := objects.Memory.Charge(); charge != len(got) {
		t.Fatalf("memory charge = %d, want %d", charge, len(got))
	}
	generation := objects.Memory.Generation.Load()
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
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
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
	if charge := objects.Memory.Charge(); charge != 12 {
		t.Fatalf("memory charge = %d, want the two 6-byte children only", charge)
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
	disk.Put(cache.DiskKey(object, objects.Memory.Generation.Load()), whole)
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
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
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
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
	for fault.Fired() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // the other queries join the paused GET
	fault.Resume()
	for range queries {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := fault.Ops()[storetest.OpGetRange]; got != 1 {
		t.Fatalf("%d concurrent cold queries made %d range GETs, want 1", queries, got)
	}
	if charge := objects.Memory.Charge(); charge != 12 {
		t.Fatalf("memory charge = %d, want the two 6-byte children only", charge)
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
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
	fault.Set(storetest.Plan{Op: storetest.OpGetRange, Key: object, N: 1, Mode: storetest.Hang})
	leaderCtx, cancel := context.WithCancel(t.Context())
	leader, follower := make(chan error, 1), make(chan error, 1)
	go func() { _, err := r.FetchRanges(leaderCtx, loads); leader <- err }()
	for fault.Fired() == 0 {
		time.Sleep(time.Millisecond)
	}
	go func() {
		ctx, err := r.FetchRanges(t.Context(), loads)
		if got, ok := cache.Scoped(ctx, loads[1].Key); err == nil && (!ok || !bytes.Equal(got, whole[12:18])) {
			err = fmt.Errorf("child = %q", got)
		}
		follower <- err
	}()
	time.Sleep(50 * time.Millisecond) // the follower joins the hung GET
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
	time.Sleep(50 * time.Millisecond) // the follower joins the leader's flight
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
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	r := New(s, objects, cfg)
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
			if disk.Has(cache.DiskKey(load.Key, objects.Memory.Generation.Load())) {
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
