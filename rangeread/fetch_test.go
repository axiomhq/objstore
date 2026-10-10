package rangeread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cache"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

// readBackend supplies channel rendezvous at the actual store operation.
type readBackend struct {
	objstore.Backend
	get      func(context.Context, string) ([]byte, error)
	getRange func(context.Context, string, int64, int64) ([]byte, error)
}

func (b *readBackend) Get(ctx context.Context, key string) ([]byte, error) {
	if b.get != nil {
		return b.get(ctx, key)
	}
	return b.Backend.Get(ctx, key)
}

func (b *readBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if b.getRange != nil {
		return b.getRange(ctx, key, off, n)
	}
	return b.Backend.GetRange(ctx, key, off, n)
}

func TestCancelledRangeWaiterKeepsProducerReservation(t *testing.T) {
	for _, transient := range []bool{true, false} {
		name := "parent"
		if !transient {
			name = "cached-child"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, deadline := context.WithTimeout(t.Context(), 5*time.Second)
				defer deadline()
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				const limit = 1024
				const length = 512
				entered, release := make(chan struct{}), make(chan struct{})
				s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
					return &readBackend{Backend: b, getRange: func(context.Context, string, int64, int64) ([]byte, error) {
						data := make([]byte, length)
						close(entered)
						<-release // cancellation cannot free a buffer the backend still holds
						return data, nil
					}}
				})
				objects := cache.New(s, 1<<20, nil, cache.Keys{})
				t.Cleanup(objects.Close)
				r := newReader(t, s, objects, Config{MaxRangeBytes: limit, MaxInFlightBytes: limit})
				load := Load{Extent: Extent{Object: "o", Length: length}, Key: "child", Transient: transient}
				done := make(chan error, 1)
				go func() { _, err := r.FetchRanges(ctx, []Load{load}); done <- err }()
				<-entered
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("waiter: %v, want context.Canceled", err)
				}
				synctest.Wait()
				var free int64
				for r.memory.TryAcquire(1) {
					free++
				}
				if free > 0 {
					r.memory.Release(free)
				}
				if free != limit-length {
					t.Errorf("free physical bytes = %d, want %d while producer holds %d bytes", free, limit-length, length)
				}
				if got := objects.GateInUse.Load(); got != 1 {
					t.Errorf("GET slots in use = %d, want 1", got)
				}
				close(release)
				synctest.Wait()
				if !r.memory.TryAcquire(limit) {
					t.Fatal("physical reservation survived the producer and its consumers")
				}
				r.memory.Release(limit)
			})
		})
	}
}

func TestSharedParentReservationCoversConsumers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		const limit = 1024
		r := newReader(t, nil, nil, Config{MaxRangeBytes: limit, MaxInFlightBytes: limit})
		x := Extent{Object: "o", Length: limit}
		joined := make(chan struct{}, 2)
		r.joined = func() { joined <- struct{}{} }
		entered, finish := make(chan struct{}), make(chan struct{})
		read := func(context.Context) ([]byte, cache.Outcome, error) {
			close(entered)
			<-finish
			return make([]byte, limit), cache.Load, nil
		}
		type consumer struct {
			data    []byte
			release func()
			err     error
		}
		results := make(chan consumer, 2)
		fetch := func() {
			data, _, _, release, err := r.sharedParentOnce(ctx, x, 0, read, false)
			results <- consumer{data, release, err}
		}
		go fetch()
		<-entered
		<-joined
		go fetch()
		<-joined // even a full-size flight admits a follower without reserving twice
		close(finish)
		first, last := <-results, <-results
		if first.err != nil || last.err != nil {
			t.Fatalf("consumers: %v, %v", first.err, last.err)
		}
		if &first.data[0] != &last.data[0] {
			t.Fatal("consumers did not share the physical buffer")
		}
		synctest.Wait()
		if r.memory.TryAcquire(1) {
			r.memory.Release(1)
			t.Error("producer completion freed bytes still used by consumers")
		}
		first.release()
		if r.memory.TryAcquire(1) {
			r.memory.Release(1)
			t.Error("first consumer freed bytes still used by the last consumer")
		}
		copy(make([]byte, limit), last.data)
		last.release()
		if !r.memory.TryAcquire(limit) {
			t.Fatal("last consumer did not free the physical reservation")
		}
		r.memory.Release(limit)
	})
}

func TestCachedChildCopiesUnderProducerReservation(t *testing.T) {
	for _, decoded := range []bool{false, true} {
		name := "stored"
		if decoded {
			name = "decoded"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const limit = 1024
			physical := bytes.Repeat([]byte("x"), 512)
			s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
				return &readBackend{Backend: b, getRange: func(context.Context, string, int64, int64) ([]byte, error) {
					return physical, nil
				}}
			})
			objects := cache.New(s, 1<<20, nil, cache.Keys{})
			t.Cleanup(objects.Close)
			r := newReader(t, s, objects, Config{MaxRangeBytes: limit, MaxInFlightBytes: limit})
			load := Load{Extent: Extent{Object: "o", Length: int64(len(physical))}, Key: "child"}
			if decoded {
				load.Decode = func(child []byte) ([]byte, error) {
					if &child[0] == &physical[0] {
						t.Error("decoder still uses the physical read buffer")
					}
					if r.memory.TryAcquire(limit) {
						r.memory.Release(limit)
						t.Error("decoder ran without the producer's reservation")
					}
					return child, nil
				}
			}
			gotCtx, err := r.FetchRanges(ctx, []Load{load})
			if err != nil {
				t.Fatal(err)
			}
			child, ok := cache.Scoped(gotCtx, load.Key)
			if !ok || !bytes.Equal(child, physical) {
				t.Fatal("scope missed the child")
			}
			if &child[0] == &physical[0] {
				t.Fatal("scope retained the unreserved physical read buffer")
			}
			if !r.memory.TryAcquire(limit) {
				t.Fatal("owned child kept the physical reservation")
			}
			r.memory.Release(limit)
		})
	}
}

func TestParentFlightKeyDoesNotAliasObjectGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r := newReader(t, nil, nil, Config{})
	joined := make(chan struct{}, 2)
	r.joined = func() { joined <- struct{}{} }
	entered, release := make(chan struct{}), make(chan struct{})
	x := Extent{Object: "ns/a/object", Length: 3}
	if err := x.valid(); err != nil {
		t.Fatal(err)
	}
	leader := make(chan error, 1)
	go func() {
		_, _, _, leave, err := r.sharedParentOnce(ctx, x, 1, func(ctx context.Context) ([]byte, cache.Outcome, error) {
			close(entered)
			select {
			case <-release:
				return []byte("ONE"), cache.Load, nil
			case <-ctx.Done():
				return nil, cache.Load, ctx.Err()
			}
		}, false)
		if err == nil {
			leave()
		}
		leader <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	await(t, joined, "generation 1 join")
	x.Object = "1:ns/a/object"
	if err := x.valid(); err != nil {
		t.Fatal(err)
	}
	follower := make(chan error, 1)
	go func() {
		b, _, _, leave, err := r.sharedParentOnce(ctx, x, 0, func(context.Context) ([]byte, cache.Outcome, error) {
			return []byte("TWO"), cache.Load, nil
		}, false)
		if err == nil {
			defer leave()
		}
		if err == nil && string(b) != "TWO" {
			err = fmt.Errorf("object %q at generation 0 read %q, want TWO", x.Object, b)
		}
		follower <- err
	}()
	await(t, joined, "generation 0 join")
	close(release)
	if err := await(t, leader, "generation 1 read"); err != nil {
		t.Fatal(err)
	}
	if err := await(t, follower, "generation 0 read"); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFetchRangesMemoryHit(b *testing.B) {
	ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
	defer cancel()
	objects := cache.New(nil, 1<<20, nil, cache.Keys{})
	b.Cleanup(objects.Close)
	r, err := New(nil, objects, Config{})
	if err != nil {
		b.Fatal(err)
	}
	load := Load{Extent: Extent{Object: "ns/hit/object", Length: 1}, Key: "ns/hit/child"}
	objects.Memory.Put(load.Key, []byte("x"), 0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		got, err := r.FetchRanges(ctx, []Load{load})
		if data, ok := cache.Scoped(got, load.Key); err != nil || !ok || string(data) != "x" {
			b.Fatalf("memory hit = %q, %v, %v", data, ok, err)
		}
	}
}

func TestSerialDecodeCancellationStopsPublication(t *testing.T) {
	ctx, deadline := context.WithTimeout(t.Context(), 5*time.Second)
	defer deadline()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := bucket.NewFS(t)
	disk, err := cache.NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<20, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/cancel/object"
	if err := s.Put(ctx, object, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	calls := 0
	loads := []Load{
		{Extent: Extent{Object: object, Length: 2}, Key: object + "#one", Decode: func(b []byte) ([]byte, error) { calls++; cancel(); return b, nil }},
		{Extent: Extent{Object: object, Offset: 4, Length: 2}, Key: object + "#two", Decode: func(b []byte) ([]byte, error) { calls++; return b, nil }},
	}
	r := newReader(t, s, objects, Config{})
	_, err = r.FetchRanges(ctx, loads)
	if !errors.Is(err, context.Canceled) || calls != 1 || objects.Memory.Charge() != 0 || disk.Stats().Entries != 0 {
		t.Fatalf("cancelled decode: err=%v calls=%d memory=%d disk=%d", err, calls, objects.Memory.Charge(), disk.Stats().Entries)
	}
}

func TestParentFlightSeparatesNamespaceGenerations(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
		return &readBackend{Backend: b, getRange: func(ctx context.Context, _ string, _, _ int64) ([]byte, error) {
			select {
			case <-entered:
				return []byte("NEW"), nil
			default:
				close(entered)
			}
			select {
			case <-release:
				return []byte("OLD"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
	})
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	joined := make(chan struct{}, 2)
	r.joined = func() { joined <- struct{}{} }
	loads := []Load{
		{Extent: Extent{Object: "ns/g/object", Length: 1}, Key: "ns/g/one"},
		{Extent: Extent{Object: "ns/g/object", Offset: 2, Length: 1}, Key: "ns/g/two"},
	}
	leader := make(chan error, 1)
	go func() { _, err := r.FetchRanges(ctx, loads); leader <- err }()
	await(t, entered, "old read")
	await(t, joined, "old join")
	objects.InvalidateNamespace("g")
	follower := make(chan error, 1)
	go func() {
		got, err := r.FetchRanges(ctx, loads)
		if b, _ := cache.Scoped(got, loads[0].Key); err == nil && string(b) != "N" {
			err = fmt.Errorf("generation 1 cached %q, want N", b)
		}
		follower <- err
	}()
	await(t, joined, "new join")
	// Release the old read only after the new caller has registered.
	close(release)
	released = true
	if err := await(t, follower, "new stage"); err != nil {
		t.Fatal(err)
	}
	if err := await(t, leader, "old stage"); err != nil {
		t.Fatal(err)
	}
	if b, _ := objects.Memory.Peek(loads[0].Key); string(b) != "N" {
		t.Fatalf("generation 1 memory = %q, want N", b)
	}
}

func TestSingleChildPlanIsNotCachedTwice(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
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
	disk.WaitFills() // the fill is asynchronous
	generation := objects.Memory.GenerationOf(load.Key)
	if disk.Stats().Entries != 0 || disk.Has(cache.DiskKey(load.Key, generation)) {
		t.Fatal("disk tier holds the parent, or a child below cache.MinDiskFill")
	}
	if r.IO.Gets.Load() != 1 || fault.Ops()[storetest.OpGetRange] != 1 {
		t.Fatalf("range GETs = %d, store ops = %v", r.IO.Gets.Load(), fault.Ops())
	}
}

// TestCoalescedParentIsNotCached: two children read in one coalesced range
// are cached; the parent range, gap included, is not, in either tier.
func TestCoalescedParentIsNotCached(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
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
	disk.WaitFills() // the fill is asynchronous
	if st := disk.Stats(); st.Entries != 0 {
		t.Fatalf("disk entries = %d, want none: the parent is never cached, the children are below cache.MinDiskFill", st.Entries)
	}
}

// TestCoalescedChildrenOfACachedObjectStayOffDisk: children cut from a
// range the disk tier already holds inside a whole object are cached in
// memory, not copied to disk a second time.
func TestCoalescedChildrenOfACachedObjectStayOffDisk(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
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
	s, fault := storetest.NewFault(bucket.New(t))
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
	r.joined = func() {
		select {
		case joined <- struct{}{}:
		default:
		}
	}
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
		await(t, joined, "join")
	}
	fault.Resume()
	for range queries {
		if err := await(t, errs, "query"); err != nil {
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

func TestSharedParentFollowersShareDeterministicError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"access-denied", objstore.ErrAccessDenied},
		{"not-found", objstore.ErrNotFound},
		{"range", objstore.ErrRange},
		{"corrupt", ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const followers = 8
			entered, release := make(chan struct{}), make(chan struct{})
			var gets atomic.Int32
			s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
				return &readBackend{Backend: b, getRange: func(ctx context.Context, _ string, _, _ int64) ([]byte, error) {
					if gets.Add(1) == 1 {
						close(entered)
					}
					select {
					case <-release:
						return nil, fmt.Errorf("read: %w", tc.err)
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}}
			})
			objects := cache.New(s, 1<<20, nil, cache.Keys{})
			t.Cleanup(objects.Close)
			r := newReader(t, s, objects, Config{})
			joined := make(chan struct{}, 2*(followers+1))
			r.joined = func() { joined <- struct{}{} }
			loads := []Load{
				{Extent: Extent{Object: "o", Offset: 2, Length: 6}, Key: "child1"},
				{Extent: Extent{Object: "o", Offset: 12, Length: 6}, Key: "child2"},
			}
			done := make(chan error, followers+1)
			fetch := func() { _, err := r.FetchRanges(ctx, loads); done <- err }
			go fetch()
			await(t, entered, "leader GET")
			await(t, joined, "leader join")
			for range followers {
				go fetch()
			}
			for range followers {
				await(t, joined, "follower join")
			}
			close(release)
			for range followers + 1 {
				if err := await(t, done, "read"); !errors.Is(err, tc.err) {
					t.Errorf("read: %v, want %v", err, tc.err)
				}
			}
			if got := gets.Load(); got != 1 {
				t.Fatalf("%d followers made %d range GETs, want 1", followers, got)
			}
		})
	}
}

// TestSharedParentOutlivesItsLeader: a query that joined another query's
// parent GET does not inherit that query's cancellation.
func TestSharedParentOutlivesItsLeader(t *testing.T) {
	ctx, deadline := context.WithTimeout(t.Context(), 5*time.Second)
	defer deadline()
	entered := make(chan struct{})
	var reads atomic.Int32
	s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
		return &readBackend{Backend: b, getRange: func(ctx context.Context, key string, off, n int64) ([]byte, error) {
			if reads.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return b.GetRange(ctx, key, off, n)
		}}
	})
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/leader/object"
	whole := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if err := s.Put(ctx, object, whole); err != nil {
		t.Fatal(err)
	}
	loads := []Load{
		{Extent: Extent{Object: object, Offset: 2, Length: 6}, Key: object + "#vector-block#v#0"},
		{Extent: Extent{Object: object, Offset: 12, Length: 6}, Key: object + "#vector-block#v#1"},
	}
	r := newReader(t, s, objects, Config{})
	joined := make(chan struct{}, 4)
	r.joined = func() {
		select {
		case joined <- struct{}{}:
		default:
		}
	}
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leader, follower := make(chan error, 1), make(chan error, 1)
	go func() { _, err := r.FetchRanges(leaderCtx, loads); leader <- err }()
	await(t, joined, "leader join")
	await(t, entered, "leader GET")
	go func() {
		gotCtx, err := r.FetchRanges(ctx, loads)
		if got, ok := cache.Scoped(gotCtx, loads[1].Key); err == nil && (!ok || !bytes.Equal(got, whole[12:18])) {
			err = fmt.Errorf("child = %q", got)
		}
		follower <- err
	}()
	await(t, joined, "follower join") // the follower joined the hung GET
	cancel()
	if err := await(t, leader, "leader"); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v, want context.Canceled", err)
	}
	if err := await(t, follower, "follower"); err != nil {
		t.Fatalf("follower inherited the leader's cancellation: %v", err)
	}
}

// TestSharedParentFollowerRetriesTheLeadersError: the shared read runs
// under the leader's context, so its error can be the leader's own (a
// cancellation or per-request budget); a follower retries once for itself,
// and the leader keeps its error.
func TestSharedParentFollowerRetriesTheLeadersError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"budget", cache.ErrBudget},
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			r := newReader(t, nil, nil, Config{})
			joined := make(chan struct{}, 4)
			r.joined = func() { joined <- struct{}{} }
			x := Extent{Object: "o", Offset: 0, Length: 4}
			started, release := make(chan struct{}), make(chan struct{})
			own := fmt.Errorf("leader: %w", tc.err)
			leader := make(chan error, 1)
			go func() {
				_, _, _, leave, err := r.sharedParentOnce(ctx, x, 0, func(ctx context.Context) ([]byte, cache.Outcome, error) {
					close(started)
					select {
					case <-release:
						return nil, cache.Load, own
					case <-ctx.Done():
						return nil, cache.Load, ctx.Err()
					}
				}, false)
				if err == nil {
					leave()
				}
				leader <- err
			}()
			await(t, started, "leader read")
			await(t, joined, "leader join")
			follower := make(chan error, 1)
			go func() {
				data, _, _, leave, err := r.sharedParentOnce(ctx, x, 0, func(context.Context) ([]byte, cache.Outcome, error) {
					return []byte("data"), cache.Load, nil
				}, false)
				if err == nil {
					defer leave()
				}
				if err == nil && string(data) != "data" {
					err = fmt.Errorf("follower read %q", data)
				}
				follower <- err
			}()
			await(t, joined, "follower join") // the follower joined the leader's flight
			close(release)
			if err := await(t, leader, "leader"); !errors.Is(err, tc.err) {
				t.Fatalf("leader: %v, want its own error", err)
			}
			if err := await(t, follower, "follower"); err != nil {
				t.Fatalf("follower inherited the leader's error: %v", err)
			}
		})
	}
}

// TestTransientLoadsAreServedOnlyFromTheScope: a transient child is in the
// returned scope and nowhere else, alone in its plan or coalesced with a
// neighbour, and costs the memory tier no charge.
func TestTransientLoadsAreServedOnlyFromTheScope(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
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
	s := bucket.New(t)
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
	// A second stage is served from the cache: no second decode.
	ctx, err = r.FetchRanges(t.Context(), []Load{load})
	if got, _ := cache.Scoped(ctx, load.Key); err != nil || string(got) != "AB" || calls != 1 {
		t.Fatalf("cached child: %q %v, decoded %d times", got, err, calls)
	}
}

// TestZeroConfigReader: R2. New with a zero Config gets the defaults, so
// FetchRanges does real work instead of skipping every stage.
func TestZeroConfigReader(t *testing.T) {
	s := bucket.New(t)
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
	if r.Config() != want {
		t.Fatalf("Config = %+v, want %+v", r.Config(), want)
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
	s, fault := storetest.NewFault(bucket.New(t))
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
	s := bucket.New(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{})
	for _, load := range []Load{
		{Extent: Extent{Object: "o", Offset: -1, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 0, Length: 0}, Key: "k"},
		{Extent: Extent{Object: "o#x", Offset: 0, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "", Offset: 0, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 0, Length: 1}, Key: "k", DecodedBytes: -1},
		{Extent: Extent{Object: "o", Offset: 0, Length: 1}},
	} {
		_, err := r.FetchRanges(t.Context(), []Load{load})
		if !errors.Is(err, ErrInvalidExtent) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("%+v: %v, want ErrInvalidExtent", load, err)
		}
	}
	// One Key naming two extents would serve one child's bytes for both.
	reused := []Load{
		{Extent: Extent{Object: "o", Offset: 0, Length: 1}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 4, Length: 1}, Key: "k"},
	}
	if _, err := r.FetchRanges(t.Context(), reused); !errors.Is(err, ErrInvalidExtent) {
		t.Fatalf("Key reused with a different extent: %v, want ErrInvalidExtent", err)
	}
	// Same Key and extent, but a different retention or decoded size.
	base := Load{Extent: Extent{Object: "o", Offset: 0, Length: 1}, Key: "k"}
	transient, decoded := base, base
	transient.Transient = true
	decoded.DecodedBytes = 7
	for name, other := range map[string]Load{"Transient": transient, "DecodedBytes": decoded} {
		if _, err := r.FetchRanges(t.Context(), []Load{base, other}); !errors.Is(err, ErrInvalidExtent) {
			t.Fatalf("Key reused with a different %s: %v, want ErrInvalidExtent", name, err)
		}
	}
}

// TestInvalidLoadAfterOverBudget: An invalid load is an error even
// after an earlier load already put the stage over budget.
func TestInvalidLoadAfterOverBudget(t *testing.T) {
	s := bucket.New(t)
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	r := newReader(t, s, objects, Config{MaxRangeBytes: 1 << 10, MaxInFlightBytes: 1 << 10})
	loads := []Load{
		{Extent: Extent{Object: "o", Offset: 0, Length: 1 << 20}, Key: "big"},
		{Extent: Extent{Object: "o", Offset: -1, Length: 1}, Key: "bad"},
	}
	if _, err := r.FetchRanges(t.Context(), loads); !errors.Is(err, ErrInvalidExtent) {
		t.Fatalf("invalid load behind an over-budget one: %v, want ErrInvalidExtent", err)
	}
	dup := []Load{
		{Extent: Extent{Object: "o", Offset: 0, Length: 1 << 20}, Key: "k"},
		{Extent: Extent{Object: "o", Offset: 4, Length: 1}, Key: "k"},
	}
	if _, err := r.FetchRanges(t.Context(), dup); !errors.Is(err, ErrInvalidExtent) {
		t.Fatalf("reused Key behind an over-budget load: %v, want ErrInvalidExtent", err)
	}
}

// TestDedupSameKeySameExtent: a Key repeated with its own extent is one
// child, read once.
func TestDedupSameKeySameExtent(t *testing.T) {
	s, fault := storetest.NewFault(bucket.New(t))
	objects := cache.New(s, 1<<20, nil, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/dedup/object"
	if err := s.Put(t.Context(), object, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	r := newReader(t, s, objects, Config{})
	load := Load{Extent: Extent{Object: object, Offset: 2, Length: 3}, Key: object + "#d"}
	fault.ResetOps()
	ctx, err := r.FetchRanges(t.Context(), []Load{load, load})
	if got, ok := cache.Scoped(ctx, load.Key); err != nil || !ok || string(got) != "234" {
		t.Fatalf("dedup: %q %v %v", got, ok, err)
	}
	if n := fault.Ops()[storetest.OpGetRange]; n != 1 {
		t.Fatalf("duplicate load made %d range GETs, want 1", n)
	}
}

// await receives from ch, failing the test after 10s.
func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out", what)
		panic("unreachable")
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

// TestSmallChildrenStayOffDisk: a child of cache.MinDiskFill bytes read
// from the store gets a disk entry; one byte smaller does not.
func TestSmallChildrenStayOffDisk(t *testing.T) {
	s := bucket.New(t)
	disk, err := cache.NewDisk(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	objects := cache.New(s, 1<<30, disk, cache.Keys{})
	t.Cleanup(objects.Close)
	const object = "ns/fill/object"
	if err := s.Put(t.Context(), object, make([]byte, 4*cache.MinDiskFill)); err != nil {
		t.Fatal(err)
	}
	big := Load{Extent: Extent{Object: object, Offset: 0, Length: cache.MinDiskFill}, Key: object + "#big"}
	small := Load{Extent: Extent{Object: object, Offset: 2 * cache.MinDiskFill, Length: cache.MinDiskFill - 1}, Key: object + "#small"}
	if _, err := newReader(t, s, objects, Config{}).FetchRanges(t.Context(), []Load{big, small}); err != nil {
		t.Fatal(err)
	}
	disk.WaitFills()
	has := func(l Load) bool { return disk.Has(cache.DiskKey(l.Key, objects.Memory.GenerationOf(l.Key))) }
	if !has(big) || has(small) {
		t.Fatalf("on disk: %d-byte child %v, %d-byte child %v; want true, false", big.Length, has(big), small.Length, has(small))
	}
}
