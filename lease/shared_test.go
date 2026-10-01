package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/axiomhq/objstore/storetest"
)

func TestShared(t *testing.T) {
	ctx := context.Background()
	const ttl = time.Minute

	runUnit(t, "RefCount", func(t *testing.T) {
		s := newMemStore()
		var sh Shared
		minted := 0
		mint := func() (*Lease, error) {
			minted++
			return Acquire(ctx, s, "shared/lease", "owner-a", ttl)
		}
		r1, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		if minted != 1 || r1.Lease() != r2.Lease() || !sh.Held() {
			t.Fatalf("second Join minted a new lease: minted=%d held=%v", minted, sh.Held())
		}
		r1.Release(ctx)
		r1.Release(ctx) // a double release must not drop r2's reference
		if err := r2.Valid(); err != nil || !sh.Held() {
			t.Fatalf("first release dropped the shared lease: %v", err)
		}
		r2.Release(ctx)
		if sh.Held() {
			t.Fatal("last release left the lease held")
		}
		// The last release hands the object back: expiry zero, free at once.
		if cur, _, err := Load(ctx, s, "shared/lease"); err != nil || !cur.Expiry.IsZero() {
			t.Fatalf("last release did not hand the lease back: %+v (%v)", cur, err)
		}

		// A fenced lease is not joined, and mint is not called for it.
		r3, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		defer r3.Release(ctx)
		r3.Lease().Fence()
		if _, err := sh.Join(mint); !errors.Is(err, ErrNotOwner) || minted != 2 {
			t.Fatalf("joined a fenced lease: err=%v minted=%d", err, minted)
		}
		var none *Ref
		if err := none.Valid(); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("nil Ref is valid: %v", err)
		}
		none.Release(ctx)
	})

	runUnit(t, "ConcurrentJoinMintsOnce", func(t *testing.T) {
		s := newMemStore()
		var sh Shared
		var minted atomic.Int32
		mint := func() (*Lease, error) {
			minted.Add(1)
			return Acquire(ctx, s, "shared/concurrent", "owner-a", ttl)
		}
		refs := make([]*Ref, 8)
		var wg sync.WaitGroup
		for i := range refs {
			wg.Go(func() {
				r, err := sh.Join(mint)
				if err != nil {
					t.Error(err)
				}
				refs[i] = r
			})
		}
		wg.Wait()
		if minted.Load() != 1 {
			t.Fatalf("minted %d leases for concurrent Joins, want 1", minted.Load())
		}
		for _, r := range refs {
			r.Release(ctx)
		}
		if sh.Held() {
			t.Fatal("all refs released, lease still held")
		}
	})

	// A mint stuck on the store must not hold Shared's mutex. Held
	// answers at once; a concurrent Join waits for the mint and, when it
	// fails, mints its own.
	runUnit(t, "HungMintDoesNotBlockShared", func(t *testing.T) {
		s, f := storetest.Faulty(t, newMemStore())
		f.Set(storetest.Plan{Op: storetest.OpPutIfAbsent, N: 1, Mode: storetest.Hang, Key: "shared/hang"})
		var sh Shared
		var minted atomic.Int32
		mint := func(timeout time.Duration) func() (*Lease, error) {
			return func() (*Lease, error) {
				minted.Add(1)
				mctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				return Acquire(mctx, s, "shared/hang", "owner-a", ttl)
			}
		}
		release := make(chan struct{})
		firstErr := make(chan error, 1)
		go func() {
			// The hung mint lasts until release closes.
			_, err := sh.Join(func() (*Lease, error) {
				minted.Add(1)
				mctx, cancel := context.WithCancel(ctx)
				go func() { <-release; cancel() }()
				return Acquire(mctx, s, "shared/hang", "owner-a", ttl)
			})
			firstErr <- err
		}()
		synctest.Wait()
		if f.Fired() != 1 {
			t.Fatal("mint did not reach the hung PUT")
		}
		held := make(chan bool, 1)
		go func() { held <- sh.Held() }()
		select {
		case h := <-held:
			if h {
				t.Fatal("Held during a mint")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Held blocked behind a hung mint")
		}
		second := make(chan *Ref, 1)
		go func() {
			r, err := sh.Join(mint(10 * time.Second))
			if err != nil {
				t.Error(err)
			}
			second <- r
		}()
		synctest.Wait()
		select {
		case <-second:
			t.Fatal("second Join did not wait for the in-flight mint")
		default:
		}
		close(release)
		if err := await(t, firstErr, "hung mint"); !errors.Is(err, context.Canceled) {
			t.Fatalf("hung mint: %v, want context.Canceled", err)
		}
		r := await(t, second, "second Join")
		if r == nil || minted.Load() != 2 || !sh.Held() {
			t.Fatalf("second Join: ref=%v minted=%d held=%v", r, minted.Load(), sh.Held())
		}
		r.Release(ctx)
	})

	// The last Ref.Release hands back outside the mutex: Held answers
	// while the handover is stuck on the store.
	runUnit(t, "ReleaseOutsideLock", func(t *testing.T) {
		s, f := storetest.Faulty(t, newMemStore())
		var sh Shared
		r, err := sh.Join(func() (*Lease, error) { return Acquire(ctx, s, "shared/rel", "owner-a", ttl) })
		if err != nil {
			t.Fatal(err)
		}
		f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Mode: storetest.Pause, Key: "shared/rel"})
		done := make(chan struct{})
		go func() {
			r.Release(ctx)
			close(done)
		}()
		synctest.Wait()
		if f.Fired() != 1 {
			t.Fatal("release did not reach the paused PUT")
		}
		held := make(chan bool, 1)
		go func() { held <- sh.Held() }()
		select {
		case h := <-held:
			if h {
				t.Fatal("Held true during the last release")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Held blocked behind a release")
		}
		f.Resume()
		await(t, done, "last Release")
	})
}

func TestSharedRejectsInvalidMint(t *testing.T) {
	for _, state := range []string{"fenced", "stolen", "retired", "expired"} {
		runUnit(t, state, func(t *testing.T) {
			ctx := context.Background()
			s := newMemStore()
			l, err := Acquire(ctx, s, "shared/invalid-mint", "owner-a", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Release(ctx)
			var sh Shared
			entered, unblock, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(unblock) })
			defer resume()
			var r *Ref
			go func() {
				r, err = sh.Join(func() (*Lease, error) {
					close(entered)
					<-unblock
					return l, nil
				})
				close(joined)
			}()
			await(t, entered, "mint")
			switch state {
			case "fenced":
				l.Fence()
			case "stolen":
				if err := Steal(ctx, s, l.Key(), "owner-b", time.Minute); err != nil {
					t.Fatal(err)
				}
				l.Fence()
			case "retired":
				l.Retire()
			case "expired":
				l.Expire()
			}
			resume()
			await(t, joined, "Join after invalidation")
			if r != nil || !errors.Is(err, ErrNotOwner) || sh.Held() {
				t.Fatalf("Join accepted invalid mint: ref=%v err=%v held=%v", r != nil, err, sh.Held())
			}
			await(t, l.Done(), "rejected lease Done")
			cur, _, err := Load(ctx, s, l.Key())
			if err != nil {
				t.Fatal(err)
			}
			if state == "stolen" {
				if cur.Owner != "owner-b" || cur.Expiry.IsZero() {
					t.Fatalf("rejected mint clobbered its new owner: %+v", cur)
				}
				return
			}
			if !cur.Expiry.IsZero() {
				t.Fatalf("rejected mint was not handed back: %+v", cur)
			}
			r, err = sh.Join(func() (*Lease, error) { return Acquire(ctx, s, l.Key(), "owner-b", time.Minute) })
			if err != nil {
				t.Fatalf("Join after rejected mint: %v", err)
			}
			r.Release(ctx)
		})
	}
}

func TestSharedJoinContextCancelsWait(t *testing.T) {
	for _, busy := range []string{"mint", "release"} {
		for _, end := range []string{"cancel", "deadline"} {
			runUnit(t, busy+"/"+end, func(t *testing.T) {
				ctx := context.Background()
				base := newMemStore()
				entered, unblock := make(chan struct{}), make(chan struct{})
				resume := sync.OnceFunc(func() { close(unblock) })
				defer resume()
				var s Store = base
				if busy == "release" {
					var once sync.Once
					s = &stubStore{Store: base, match: func(ctx context.Context, key string, data []byte, etag string) (bool, error) {
						once.Do(func() { close(entered); <-unblock })
						return base.PutIfMatch(ctx, key, data, etag)
					}}
				}
				var sh Shared
				mint := func() (*Lease, error) { return Acquire(ctx, s, "shared/cancel-wait", "owner", time.Minute) }
				var first *Ref
				var firstErr error
				operationDone := make(chan struct{})
				if busy == "mint" {
					go func() {
						first, firstErr = sh.Join(func() (*Lease, error) {
							close(entered)
							<-unblock
							return mint()
						})
						close(operationDone)
					}()
				} else {
					r, err := sh.Join(mint)
					if err != nil {
						t.Fatal(err)
					}
					go func() { r.Release(ctx); close(operationDone) }()
				}
				await(t, entered, "busy operation")
				joinCtx, cancel := context.WithCancel(ctx)
				wantErr := context.Canceled
				if end == "deadline" {
					cancel()
					joinCtx, cancel = context.WithTimeout(ctx, time.Second)
					wantErr = context.DeadlineExceeded
				}
				defer cancel()
				var waiting *Ref
				var waitErr error
				waitDone := make(chan struct{})
				go func() {
					waiting, waitErr = sh.JoinContext(joinCtx, func() (*Lease, error) {
						t.Error("canceled waiter called mint")
						return mint()
					})
					close(waitDone)
				}()
				synctest.Wait()
				select {
				case <-waitDone:
					t.Fatal("JoinContext did not wait for the busy operation")
				default:
				}
				if end == "cancel" {
					cancel()
				}
				await(t, joinCtx.Done(), "context ends")
				synctest.Wait()
				select {
				case <-waitDone:
					if waiting != nil || !errors.Is(waitErr, wantErr) {
						t.Errorf("JoinContext: ref=%v err=%v, want %v", waiting != nil, waitErr, wantErr)
					}
				default:
					t.Errorf("JoinContext blocked after %v while %s is still blocked", wantErr, busy)
				}
				resume()
				await(t, operationDone, "busy operation completes")
				await(t, waitDone, "waiter exits")
				waiting.Release(ctx)
				if firstErr != nil {
					t.Fatalf("blocked mint failed: %v", firstErr)
				}
				if first != nil {
					if err := first.Valid(); err != nil {
						t.Errorf("blocked mint returned invalid ref: %v", err)
					}
					first.Release(ctx)
				}
				r, err := sh.Join(mint)
				if err != nil {
					t.Fatalf("Join after canceled wait: %v", err)
				}
				r.Release(ctx)
			})
		}
	}
}

// TestSharedReleaseFromFence: The last Ref.Release from inside the
// fence callback completes; Shared is usable afterwards.
func TestSharedReleaseFromFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const ttl = 200 * time.Millisecond
		s := newMemStore()
		var sh Shared
		mint := func() (*Lease, error) { return Acquire(ctx, s, "shared/fence", "owner-a", ttl) }
		r, err := sh.Join(mint)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		r.Lease().Start(func() { r.Release(ctx); close(done) })
		if err := Steal(ctx, s, "shared/fence", "owner-b", ttl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Ref.Release from the fence callback hung")
		}
		if sh.Held() {
			t.Fatal("lease still held after its last Release")
		}
		joined := make(chan error, 1)
		go func() {
			_, err := sh.Join(func() (*Lease, error) { return nil, errors.New("mint") })
			joined <- err
		}()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Fatal("Join blocked: Shared stuck busy")
		}
	})
}

// TestSharedMintPanic: A panicking mint does not leave Shared busy.
func TestSharedMintPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sh Shared
		func() {
			defer func() { _ = recover() }()
			sh.Join(func() (*Lease, error) { panic("mint") }) //nolint:errcheck // panics
		}()
		joined := make(chan error, 1)
		go func() {
			_, err := sh.Join(func() (*Lease, error) { return nil, errors.New("mint") })
			joined <- err
		}()
		select {
		case err := <-joined:
			if err == nil || err.Error() != "mint" {
				t.Fatalf("second Join: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Join blocked after a panicking mint")
		}
	})
}
