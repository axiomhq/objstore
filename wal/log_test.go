package wal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/mem"
	"github.com/axiomhq/objstore/storetest/bucket"
	"github.com/axiomhq/objstore/wal"
)

// Faults intercept the actual backend boundary, not a public publisher hook.
type logBackend struct {
	objstore.Backend
	cas    func(context.Context, string, []byte, string) (bool, error)
	absent func(context.Context, string, []byte) (bool, error)
}

func (b *logBackend) PutIfMatch(ctx context.Context, key string, p []byte, tag string) (bool, error) {
	if b.cas != nil {
		return b.cas(ctx, key, p, tag)
	}
	return b.Backend.PutIfMatch(ctx, key, p, tag)
}

func (b *logBackend) PutIfAbsent(ctx context.Context, key string, p []byte) (bool, error) {
	if b.absent != nil {
		return b.absent(ctx, key, p)
	}
	return b.Backend.PutIfAbsent(ctx, key, p)
}

func openOwned(t *testing.T, store *objstore.Store, prefix string) *wal.Log {
	t.Helper()
	l, err := wal.OpenLog(t.Context(), store, prefix, []byte("configuration"))
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func acquireOwned(t *testing.T, l *wal.Log, opts wal.SessionOptions) *wal.Session {
	t.Helper()
	s, err := l.Acquire(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func enqueueOwned(t *testing.T, s *wal.Session, data, metadata string) *wal.Receipt {
	t.Helper()
	r, err := s.Enqueue([]byte(data), []byte(metadata))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func snapshotOwned(t *testing.T, l *wal.Log) wal.Snapshot {
	t.Helper()
	s, err := l.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func replayOwned(t *testing.T, l *wal.Log, snapshot wal.Snapshot) []string {
	t.Helper()
	var got []string
	err := l.Replay(t.Context(), l.Genesis(), snapshot, 64<<20, func(b wal.Batch) error {
		records, err := b.Read(t.Context())
		if err != nil {
			return err
		}
		for _, record := range records {
			got = append(got, string(record))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestLogTakeoverCASOrder(t *testing.T) {
	for _, mode := range []string{
		"takeover-first", "publication-first", "ambiguous-publication-first", "ambiguous-settlement-takeover",
	} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				store := mem.Open(objstore.Config{})
				attempted := make(chan wal.Cursor, 1)
				release := make(chan struct{})
				var releaseOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				lost := errors.New("lost CAS response")
				attempts := 0 // only A's flush goroutine touches this
				aStore := store.WithBackend(func(base objstore.Backend) objstore.Backend {
					return &logBackend{Backend: base, cas: func(ctx context.Context, key string, p []byte, tag string) (bool, error) {
						var head struct{ Tip wal.Cursor }
						if err := json.Unmarshal(p, &head); err != nil {
							return false, err
						}
						if head.Tip.Sequence() == 0 {
							return base.PutIfMatch(ctx, key, p, tag)
						}
						attempts++
						if mode == "ambiguous-settlement-takeover" && attempts == 1 {
							ok, err := base.PutIfMatch(ctx, key, p, tag)
							if err != nil {
								return ok, err
							}
							return ok, lost
						}
						var ok bool
						var err error
						if mode != "takeover-first" && mode != "ambiguous-settlement-takeover" {
							ok, err = base.PutIfMatch(ctx, key, p, tag)
						}
						attempted <- head.Tip
						select {
						case <-release:
						case <-ctx.Done():
							return false, ctx.Err()
						}
						if mode == "takeover-first" || mode == "ambiguous-settlement-takeover" {
							return base.PutIfMatch(ctx, key, p, tag)
						}
						if mode == "ambiguous-publication-first" {
							return ok, lost
						}
						return ok, err
					}}
				})
				la := openOwned(t, aStore, "log/")
				a := acquireOwned(t, la, wal.SessionOptions{})
				x := enqueueOwned(t, a, "X", "mx")
				xCursor := <-attempted // X's pages and descriptor are already staged.
				successor := enqueueOwned(t, a, "must-not-commit", "successor")
				lb := openOwned(t, store, "log/")
				b := acquireOwned(t, lb, wal.SessionOptions{})
				defer b.Close(ctx)
				wantBase := lb.Genesis()
				if mode != "takeover-first" {
					wantBase = xCursor
				}
				if b.Base().Cursor() != wantBase {
					t.Fatal("acquisition did not preserve the exact tip")
				}
				y := enqueueOwned(t, b, "Y", "my")
				if err := y.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				upper := snapshotOwned(t, lb)
				releaseOnce.Do(func() { close(release) })
				wantX := error(nil)
				if mode == "takeover-first" {
					wantX = wal.ErrFenced
				}
				if strings.HasPrefix(mode, "ambiguous-") {
					wantX = wal.ErrUnresolved
				}
				if err := x.Wait(ctx); !errors.Is(err, wantX) {
					t.Fatalf("X: %v, want %v", err, wantX)
				}
				if wantX == wal.ErrUnresolved && errors.Is(x.Wait(ctx), wal.ErrFenced) {
					t.Fatal("ambiguous acceptance classified as definitive fencing")
				}
				wantTerminal := wal.ErrFenced
				if strings.HasPrefix(mode, "ambiguous-") {
					wantTerminal = wal.ErrUnresolved
				}
				if err := successor.Wait(ctx); !errors.Is(err, wantTerminal) {
					t.Fatalf("successor: %v", err)
				}
				if !errors.Is(a.Err(), wantTerminal) {
					t.Fatalf("terminal: %v", a.Err())
				}
				if _, err := a.Enqueue(nil, nil); !errors.Is(err, wantTerminal) {
					t.Fatalf("late admission: %v", err)
				}
				if err := a.Close(ctx); !errors.Is(err, wantTerminal) {
					t.Fatalf("close: %v", err)
				}
				want := []string{"Y"}
				if mode != "takeover-first" {
					want = []string{"X", "Y"}
				}
				if got := replayOwned(t, lb, upper); !reflect.DeepEqual(got, want) {
					t.Fatalf("replay %v, want %v", got, want)
				}
				z := enqueueOwned(t, b, "Z", "mz")
				if err := z.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				later := snapshotOwned(t, lb)
				if got := replayOwned(t, lb, upper); !reflect.DeepEqual(got, want) {
					t.Fatal("fixed snapshot moved", got)
				}
				if mode == "takeover-first" {
					calls := 0
					err := lb.Replay(ctx, xCursor, later, 1<<20, func(wal.Batch) error { calls++; return nil })
					if !errors.Is(err, wal.ErrCorrupt) || calls != 0 {
						t.Fatalf("orphan lower cursor: %v, %d callbacks", err, calls)
					}
				}
				keys, err := store.List(ctx, "log/owners/")
				if err != nil {
					t.Fatal(err)
				}
				if len(keys) != 3 {
					t.Fatalf("successor staged unexpectedly: %v", keys)
				}
			})
		})
	}
}

func TestLogFullWindowAndEarlyClose(t *testing.T) {
	for _, window := range []time.Duration{time.Second, 7 * time.Second} {
		t.Run(window.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := openOwned(t, mem.Open(objstore.Config{}), "log/")
				s := acquireOwned(t, l, wal.SessionOptions{CollectionWindow: window})
				start := time.Now()
				first := enqueueOwned(t, s, "first", "one")
				time.Sleep(700 * time.Millisecond)
				second := enqueueOwned(t, s, "second", "two")
				closed := make(chan error, 1)
				go func() { closed <- s.Close(t.Context()) }()
				synctest.Wait()
				time.Sleep(window - 700*time.Millisecond - time.Nanosecond)
				synctest.Wait()
				select {
				case <-first.Done():
					t.Fatal("committed before full collection window")
				default:
				}
				select {
				case <-closed:
					t.Fatal("Close bypassed collection")
				default:
				}
				if err := first.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := second.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if time.Since(start) != window {
					t.Fatalf("window: %v", time.Since(start))
				}
				upper := snapshotOwned(t, l)
				if upper.Cursor().Sequence() != 1 || string(upper.Metadata()) != "two" {
					t.Fatal("window split into groups")
				}
				if got := replayOwned(t, l, upper); !reflect.DeepEqual(got, []string{"first", "second"}) {
					t.Fatal(got)
				}
			})
		})
	}
}

func TestLogWaitersAndCopies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		store := mem.Open(objstore.Config{})
		l := openOwned(t, store, "log/")
		s := acquireOwned(t, l, wal.SessionOptions{})
		defer s.Close(ctx)
		data, metadata := []byte("payload"), []byte("metadata")
		r, err := s.Enqueue(data, metadata)
		if err != nil {
			t.Fatal(err)
		}
		data[0], metadata[0] = '!', '!'
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := r.Wait(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		waited := make(chan error, 8)
		for range 8 {
			go func() { waited <- r.Wait(ctx) }()
		}
		for range 8 {
			if err := <-waited; err != nil {
				t.Fatal(err)
			}
		}
		if err := r.Wait(canceled); err != nil {
			t.Fatal("completed receipt not definitive", err)
		}
		upper := snapshotOwned(t, l)
		upper.Metadata()[0] = '!'
		if string(upper.Metadata()) != "metadata" {
			t.Fatal("snapshot aliases accessor")
		}
		err = l.Replay(ctx, l.Genesis(), upper, 1<<20, func(b wal.Batch) error {
			if b.Cursor() != upper.Cursor() {
				t.Fatal("wrong group cursor")
			}
			b.Metadata()[0][0] = '!'
			if string(b.Metadata()[0]) != "metadata" {
				t.Fatal("batch metadata aliases accessor")
			}
			got, err := b.Read(ctx)
			if err != nil {
				return err
			}
			if string(got[0]) != "payload" {
				t.Fatal("input payload was not copied")
			}
			got[0][0] = '!'
			again, err := b.Read(ctx)
			if err == nil && string(again[0]) != "payload" {
				t.Fatal("Read retains caller-mutated payload")
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(upper.Cursor())
		if err != nil {
			t.Fatal(err)
		}
		var roundtrip wal.Cursor
		if err := json.Unmarshal(encoded, &roundtrip); err != nil || roundtrip != upper.Cursor() {
			t.Fatal("cursor round trip", err)
		}
		var future map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &future); err != nil {
			t.Fatal(err)
		}
		if _, ok := future["format"]; !ok {
			t.Fatalf("cursor JSON keys are not lowercase: %s", encoded)
		}
		future["format"] = json.RawMessage("2")
		unknown, err := json.Marshal(future)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(unknown, &roundtrip); !errors.Is(err, wal.ErrCorrupt) {
			t.Fatal("unknown cursor version accepted", err)
		}
		var forged wal.Snapshot
		if err := json.Unmarshal(encoded, &forged); err == nil {
			t.Fatal("snapshot deserialized from cursor")
		}
		other := openOwned(t, store, "other/")
		for _, from := range []wal.Cursor{{}, other.Genesis()} {
			if err := l.Replay(ctx, from, upper, 1<<20, func(wal.Batch) error { t.Fatal("invalid bounds called visitor"); return nil }); err == nil {
				t.Fatal("invalid cursor accepted")
			}
		}
		for _, bound := range []wal.Snapshot{forged, snapshotOwned(t, other)} {
			if err := l.Replay(ctx, l.Genesis(), bound, 1<<20, func(wal.Batch) error { t.Fatal("invalid snapshot called visitor"); return nil }); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		}
		calls := 0
		err = l.Replay(ctx, l.Genesis(), upper, 1, func(wal.Batch) error { calls++; return nil })
		if !errors.Is(err, wal.ErrRecordTooLarge) || calls != 0 {
			t.Fatalf("summary budget: %v, %d callbacks", err, calls)
		}
		b := acquireOwned(t, l, wal.SessionOptions{})
		defer b.Close(ctx)
		b.Base().Metadata()[0] = '!'
		if string(b.Base().Metadata()) != "metadata" {
			t.Fatal("base metadata aliases accessor")
		}
	})
}

func TestLogLifetimeAndCloseDeadline(t *testing.T) {
	for _, mode := range []string{"collection", "io", "close-deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				store := mem.Open(objstore.Config{})
				entered := make(chan struct{}, 1)
				if mode == "io" {
					store = store.WithBackend(func(base objstore.Backend) objstore.Backend {
						return &logBackend{Backend: base, absent: func(ctx context.Context, key string, p []byte) (bool, error) {
							if strings.Contains(key, "/owners/") {
								entered <- struct{}{}
								<-ctx.Done()
								return false, ctx.Err()
							}
							return base.PutIfAbsent(ctx, key, p)
						}}
					})
				}
				l := openOwned(t, store, "log/")
				s, err := l.Acquire(ctx, wal.SessionOptions{})
				if err != nil {
					t.Fatal(err)
				}
				r := enqueueOwned(t, s, "data", "meta")
				if mode == "io" {
					<-entered
				}
				if mode == "close-deadline" {
					deadline, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
					defer stop()
					if err := s.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				if err := r.Wait(t.Context()); !errors.Is(err, context.Canceled) {
					t.Fatal("receipt", err)
				}
				if err := s.Close(t.Context()); !errors.Is(err, context.Canceled) {
					t.Fatal("close", err)
				}
				if !errors.Is(s.Err(), context.Canceled) {
					t.Fatal("terminal", s.Err())
				}
				if snapshotOwned(t, l).Cursor() != l.Genesis() {
					t.Fatal("canceled work accepted")
				}
			})
		})
	}
}

func TestLogAdmissionLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts wal.SessionOptions
	}{
		{name: "pending", opts: wal.SessionOptions{MaxPendingBytes: 8000}},
		{name: "batch", opts: wal.SessionOptions{MaxBatchBytes: 150}},
		{name: "metadata-overhead", opts: wal.SessionOptions{MaxMetadataBytes: 1600}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := openOwned(t, mem.Open(objstore.Config{}), "log/")
				s := acquireOwned(t, l, tc.opts)
				defer s.Close(t.Context())
				r := enqueueOwned(t, s, "", "") // even empty records consume descriptor memory
				if _, err := s.Enqueue(nil, nil); !errors.Is(err, wal.ErrOverloaded) {
					t.Fatalf("capacity: %v", err)
				}
				if err := r.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				r = enqueueOwned(t, s, "", "") // released credit is reusable
				if err := r.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Enqueue(nil, make([]byte, 1<<20)); !errors.Is(err, wal.ErrRecordTooLarge) {
					t.Fatal("oversized metadata", err)
				}
			})
		})
	}
}

func TestLogExpiredPendingAndLogicalCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := mem.Open(objstore.Config{})
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		store = store.WithBackend(func(base objstore.Backend) objstore.Backend {
			return &logBackend{Backend: base, absent: func(ctx context.Context, key string, p []byte) (bool, error) {
				if strings.Contains(key, "/owners/") {
					once.Do(func() { close(entered); <-release })
				}
				return base.PutIfAbsent(ctx, key, p)
			}}
		})
		l := openOwned(t, store, "log/")
		s := acquireOwned(t, l, wal.SessionOptions{})
		defer s.Close(t.Context())
		first := enqueueOwned(t, s, "first", "")
		<-entered
		second := enqueueOwned(t, s, "second", "")
		time.Sleep(2 * time.Second) // original I/O stalls longer than another full window
		if _, err := s.Enqueue(nil, nil); !errors.Is(err, wal.ErrOverloaded) {
			t.Fatalf("expired pending accepted: %v", err)
		}
		resumed := time.Now()
		close(release)
		if err := first.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := second.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if time.Now() != resumed {
			t.Fatal("cadence anchored to completion instead of logical start")
		}
	})
}

func TestLogLazyMultiPageValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Real page splitting through the unchanged codec, not a second encoder.
		store := mem.Open(objstore.Config{})
		l := openOwned(t, store, "log/")
		s := acquireOwned(t, l, wal.SessionOptions{MaxBatchBytes: 64 << 20, MaxPendingBytes: 256 << 20})
		defer s.Close(t.Context())
		payload := bytes.Repeat([]byte("a"), 17<<20)
		first, err := s.Enqueue(payload, []byte("first"))
		if err != nil {
			t.Fatal(err)
		}
		second, err := s.Enqueue(payload, []byte("second"))
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := second.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		upper := snapshotOwned(t, l)
		if upper.Cursor().Sequence() != 1 {
			t.Fatal("multi-page batch changed logical sequence")
		}
		keys, err := store.List(t.Context(), "log/owners/")
		if err != nil || len(keys) != 2 {
			t.Fatal("pages", keys, err)
		}
		var batch wal.Batch
		if err := l.Replay(t.Context(), l.Genesis(), upper, 1<<20, func(b wal.Batch) error { batch = b; return nil }); err != nil {
			t.Fatal(err)
		}
		got, err := batch.Read(t.Context())
		if err != nil || len(got) != 2 || !bytes.Equal(got[0], payload) || !bytes.Equal(got[1], payload) {
			t.Fatal("complete read", err)
		}
		if !reflect.DeepEqual(batch.Metadata(), [][]byte{[]byte("first"), []byte("second")}) {
			t.Fatal("metadata order")
		}
		page, err := store.Get(t.Context(), keys[0])
		if err != nil {
			t.Fatal(err)
		}
		page[len(page)-1] ^= 1
		if err := store.Put(t.Context(), keys[0], page); err != nil {
			t.Fatal(err)
		}
		if got, err := batch.Read(t.Context()); got != nil || !errors.Is(err, wal.ErrCorrupt) {
			t.Fatalf("damaged page accepted: %d, %v", len(got), err)
		}
		page[len(page)-1] ^= 1
		if err := store.Put(t.Context(), keys[0], page); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), keys[1]); err != nil {
			t.Fatal(err)
		}
		if got, err := batch.Read(t.Context()); got != nil || !errors.Is(err, wal.ErrCorrupt) || !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("partial payload escaped: %d, %v", len(got), err)
		}
		// HEAD and summaries are still valid, explicitly not a page-existence certificate.
		snapshotOwned(t, l)
		if err := l.Replay(t.Context(), l.Genesis(), upper, 1<<20, func(wal.Batch) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogValidatesWholeChainBeforeCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := mem.Open(objstore.Config{})
		l := openOwned(t, store, "log/")
		s := acquireOwned(t, l, wal.SessionOptions{})
		defer s.Close(t.Context())
		for _, v := range []string{"first", "second", "third"} {
			if err := enqueueOwned(t, s, v, "").Wait(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		upper := snapshotOwned(t, l)
		keys, err := store.List(t.Context(), "log/groups/")
		if err != nil {
			t.Fatal(err)
		}
		// Any broken link must prevent even the otherwise valid first callback.
		if err := store.Put(t.Context(), keys[0], []byte("corrupt")); err != nil {
			t.Fatal(err)
		}
		calls := 0
		err = l.Replay(t.Context(), l.Genesis(), upper, 1<<20, func(wal.Batch) error { calls++; return nil })
		if !errors.Is(err, wal.ErrCorrupt) || calls != 0 {
			t.Fatalf("chain validation: %v, %d callbacks", err, calls)
		}
	})
}

func TestLogFSLostResponses(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent=%v", persistent), func(t *testing.T) {
			base := bucket.NewFS(t)
			lost := errors.New("lost durability response")
			var attempts atomic.Int32
			var mu sync.Mutex
			staged := make(map[string][]byte)
			var tips []wal.Cursor
			var revisions []string
			store := base.WithBackend(func(b objstore.Backend) objstore.Backend {
				return &logBackend{Backend: b,
					absent: func(ctx context.Context, key string, p []byte) (bool, error) {
						ok, err := b.PutIfAbsent(ctx, key, p)
						if err != nil {
							return ok, err
						}
						mu.Lock()
						defer mu.Unlock()
						previous, retry := staged[key]
						if retry {
							if !bytes.Equal(previous, p) {
								t.Error("staging retry changed identity")
							}
							return ok, nil
						}
						staged[key] = bytes.Clone(p)
						return ok, lost
					},
					cas: func(ctx context.Context, key string, p []byte, tag string) (bool, error) {
						ok, err := b.PutIfMatch(ctx, key, p, tag)
						if err != nil || !ok {
							return ok, err
						}
						var h struct {
							Tip      wal.Cursor
							Revision string
						}
						if err := json.Unmarshal(p, &h); err != nil {
							return ok, err
						}
						if h.Tip.Sequence() == 0 {
							return ok, nil
						}
						mu.Lock()
						tips = append(tips, h.Tip)
						revisions = append(revisions, h.Revision)
						mu.Unlock()
						if attempts.Add(1) == 1 || persistent {
							return ok, lost
						}
						return ok, nil
					},
				}
			})
			l := openOwned(t, store, "log/")
			s := acquireOwned(t, l, wal.SessionOptions{CollectionWindow: time.Millisecond})
			r := enqueueOwned(t, s, "payload", "meta")
			err := r.Wait(t.Context())
			if persistent {
				if !errors.Is(err, wal.ErrUnresolved) || !errors.Is(err, lost) {
					t.Fatal("readback falsely settled", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			s.Close(t.Context())
			mu.Lock()
			defer mu.Unlock()
			if len(tips) < 2 {
				t.Fatal("no positive durability settlement attempted")
			}
			for i := 1; i < len(tips); i++ {
				if tips[i] != tips[0] || revisions[i] == revisions[i-1] {
					t.Fatal("settlement changed batch or reused revision")
				}
			}
			if got := replayOwned(t, l, snapshotOwned(t, l)); !reflect.DeepEqual(got, []string{"payload"}) {
				t.Fatal(got)
			}
		})
	}
}

// TestLogHeadSwapTransientFailure: one failed HEAD swap does not end an
// owned session, whether the store refused it (a 503 SlowDown: nothing
// written), committed it and lost the answer (a 500 after commit), or
// committed it late, just before the resend. The record commits once, and
// the session takes the next one.
func TestLogHeadSwapTransientFailure(t *testing.T) {
	for _, mode := range []string{"refused", "committed", "late"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			var first []byte
			var firstTag string
			store := mem.Open(objstore.Config{}).WithBackend(func(b objstore.Backend) objstore.Backend {
				return &logBackend{Backend: b, cas: func(ctx context.Context, key string, p []byte, tag string) (bool, error) {
					var h struct{ Tip wal.Cursor }
					if err := json.Unmarshal(p, &h); err != nil {
						return false, err
					}
					if h.Tip.Sequence() == 0 {
						return b.PutIfMatch(ctx, key, p, tag)
					}
					switch calls.Add(1) {
					case 1:
						if mode == "committed" {
							if ok, err := b.PutIfMatch(ctx, key, p, tag); err != nil || !ok {
								return ok, err
							}
						}
						first, firstTag = p, tag
						return false, errors.New("transient store failure")
					case 2:
						if mode == "refused" && !bytes.Equal(p, first) {
							t.Errorf("resent HEAD differs:\n%s\n%s", first, p)
						}
						if mode == "late" {
							if ok, err := b.PutIfMatch(ctx, key, first, firstTag); err != nil || !ok {
								return ok, err
							}
						}
					}
					return b.PutIfMatch(ctx, key, p, tag)
				}}
			})
			l := openOwned(t, store, "log/")
			s := acquireOwned(t, l, wal.SessionOptions{CollectionWindow: time.Millisecond})
			defer s.Close(t.Context())
			for _, record := range []string{"first", "second"} {
				if err := enqueueOwned(t, s, record, "m").Wait(t.Context()); err != nil {
					t.Fatalf("%s: %v", record, err)
				}
			}
			if got := replayOwned(t, l, snapshotOwned(t, l)); !reflect.DeepEqual(got, []string{"first", "second"}) {
				t.Fatalf("replay %v", got)
			}
		})
	}
}

// TestLogStagingThrottled: a store that refuses page or descriptor PUTs
// for a second (a 503 SlowDown burst) and then recovers does not end an
// owned session.
func TestLogStagingThrottled(t *testing.T) {
	for _, kind := range []string{"owners/", "groups/"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var until time.Time
				store := mem.Open(objstore.Config{}).WithBackend(func(b objstore.Backend) objstore.Backend {
					return &logBackend{Backend: b, absent: func(ctx context.Context, key string, p []byte) (bool, error) {
						if strings.Contains(key, kind) {
							if until.IsZero() {
								until = time.Now().Add(time.Second)
							}
							if time.Now().Before(until) {
								return false, errors.New("SlowDown")
							}
						}
						return b.PutIfAbsent(ctx, key, p)
					}}
				})
				l := openOwned(t, store, "log/")
				s := acquireOwned(t, l, wal.SessionOptions{CollectionWindow: time.Millisecond})
				defer s.Close(t.Context())
				if err := enqueueOwned(t, s, "payload", "m").Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if got := replayOwned(t, l, snapshotOwned(t, l)); !reflect.DeepEqual(got, []string{"payload"}) {
					t.Fatalf("replay %v", got)
				}
			})
		})
	}
}

func TestLogOpenValidation(t *testing.T) {
	for _, factory := range []struct {
		name string
		open func(*testing.T) *objstore.Store
	}{
		{name: "mem", open: func(*testing.T) *objstore.Store { return mem.Open(objstore.Config{}) }},
		{name: "fs", open: func(t *testing.T) *objstore.Store { return bucket.NewFS(t) }},
	} {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.open(t)
			config := []byte("config")
			l, err := wal.OpenLog(t.Context(), store, ".dotwerk/owned/", config)
			if err != nil {
				t.Fatal(err)
			}
			config[0] = '!'
			if _, err := wal.OpenLog(t.Context(), store, ".dotwerk/owned/", config); err == nil {
				t.Fatal("configuration mismatch accepted")
			}
			reopened, err := wal.OpenLog(t.Context(), store, ".dotwerk/owned/", []byte("config"))
			if err != nil || reopened.Genesis() != l.Genesis() {
				t.Fatal("reopen", err)
			}
			if len(snapshotOwned(t, l).Metadata()) != 0 {
				t.Fatal("genesis has metadata")
			}
			for _, prefix := range []string{"", "/", "a", "/a/", "a//b/", "a/../b/", "a/./", "a\x00b/", "\xff/", strings.Repeat("a", 513) + "/"} {
				if _, err := wal.OpenLog(t.Context(), store, prefix, nil); !errors.Is(err, objstore.ErrInvalidKey) {
					t.Fatalf("invalid prefix %q: %v", prefix, err)
				}
			}
			prefix := strings.Repeat("é", 64) + "/" + strings.Repeat("é", 64) + "/log/"
			if factory.name == "mem" {
				// Dotwerk accepts a 256-byte namespace. Linux filesystems impose
				// NAME_MAX=255 per component; object stores have no such limit.
				prefix = strings.Repeat("é", 128) + "/log/"
			}
			if _, err := wal.OpenLog(t.Context(), store, prefix, nil); err != nil {
				t.Fatalf("canonical UTF-8 prefix: %v", err)
			}
			if _, err := wal.OpenLog(t.Context(), store, "big/", make([]byte, (64<<10)+1)); !errors.Is(err, wal.ErrRecordTooLarge) {
				t.Fatal(err)
			}
		})
	}
}

func TestLogStagingFailureTerminatesSuccessors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := mem.Open(objstore.Config{})
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		var puts atomic.Int32
		store := base.WithBackend(func(b objstore.Backend) objstore.Backend {
			return &logBackend{Backend: b, absent: func(ctx context.Context, key string, p []byte) (bool, error) {
				if strings.Contains(key, "/owners/") {
					once.Do(func() { close(entered); <-release })
					puts.Add(1)
					return false, objstore.ErrAccessDenied
				}
				return b.PutIfAbsent(ctx, key, p)
			}}
		})
		l := openOwned(t, store, "log/")
		s := acquireOwned(t, l, wal.SessionOptions{})
		first := enqueueOwned(t, s, "first", "")
		<-entered
		second := enqueueOwned(t, s, "dependent", "")
		close(release)
		for _, r := range []*wal.Receipt{first, second} {
			if err := r.Wait(t.Context()); !errors.Is(err, objstore.ErrAccessDenied) {
				t.Fatal(err)
			}
		}
		if err := s.Close(t.Context()); !errors.Is(err, objstore.ErrAccessDenied) {
			t.Fatal(err)
		}
		if puts.Load() != 6 || snapshotOwned(t, l).Cursor() != l.Genesis() {
			t.Fatal("failed stream advanced")
		}
		if _, err := s.Enqueue(nil, nil); !errors.Is(err, objstore.ErrAccessDenied) {
			t.Fatal(err)
		}
	})
}

type getRangeFault struct {
	objstore.Backend
	err error
}

func (b *getRangeFault) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if strings.Contains(key, "/groups/") {
		return nil, b.err
	}
	return b.Backend.GetRange(ctx, key, off, n)
}

// A failed read of the accepted descriptor is ErrCorrupt only when the object
// is missing; a transient or access error must not look like a corrupt log.
func TestLogReadErrorIsCorruptOnlyWhenObjectMissing(t *testing.T) {
	base := mem.Open(objstore.Config{})
	s := acquireOwned(t, openOwned(t, base, "log/"), wal.SessionOptions{})
	if err := enqueueOwned(t, s, "x", "").Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		err     error
		corrupt bool
	}{
		"missing":   {objstore.ErrNotFound, true},
		"transient": {errors.New("connection reset"), false},
		"denied":    {objstore.ErrAccessDenied, false},
	} {
		t.Run(name, func(t *testing.T) {
			faulty := base.WithBackend(func(b objstore.Backend) objstore.Backend {
				return &getRangeFault{b, tc.err}
			})
			_, err := openOwned(t, faulty, "log/").Snapshot(t.Context())
			if !errors.Is(err, tc.err) || errors.Is(err, wal.ErrCorrupt) != tc.corrupt {
				t.Fatalf("Snapshot: %v; want %v, ErrCorrupt=%v", err, tc.err, tc.corrupt)
			}
		})
	}
}

// TestLogHeadListsRecentTips: HEAD lists its newest tips and their pages,
// across a takeover, so a snapshot reads every listed descriptor after a
// cursor and their pages in one round beside the tip's, and a replay from
// there reads none of them again; only groups older than the list are
// walked one round each, and their pages read as each batch is. A list that
// fails a check costs rounds, never an error or a wrong record.
func TestLogHeadListsRecentTips(t *testing.T) {
	for _, tc := range []struct {
		groups, rounds int
		tamper         func(h map[string]any)
	}{
		// HEAD, then the listed descriptors and their pages; a group older
		// than the list costs a round for its descriptor and one for its
		// pages, read in the callback.
		{groups: 1, rounds: 2},
		{groups: 5, rounds: 2},
		{groups: 70, rounds: 2 + 2*(70-64)},
		// A wrong page size: that page is read again from its descriptor.
		{groups: 5, rounds: 3, tamper: func(h map[string]any) {
			b := h["pages"].([]any)[2].(map[string]any)["bytes"].([]any)
			b[0] = b[0].(float64) + 1
		}},
		// A list whose newest entry is not Tip is dropped: HEAD, the tip's
		// descriptor, the walk back over all 5 and their pages.
		{groups: 5, rounds: 12, tamper: func(h map[string]any) {
			r := h["recent"].([]any)
			h["recent"], h["pages"] = r[:len(r)-1], h["pages"].([]any)[:len(r)-1]
		}},
	} {
		groups := tc.groups
		synctest.Test(t, func(t *testing.T) {
			var slow atomic.Bool
			store := mem.Open(objstore.Config{}).WithBackend(func(b objstore.Backend) objstore.Backend {
				return &slowReads{Backend: b, slow: &slow}
			})
			l := openOwned(t, store, "log/")
			s := acquireOwned(t, l, wal.SessionOptions{CollectionWindow: time.Millisecond})
			var want []string
			for i := range groups {
				if i == groups/2 {
					// A takeover keeps the list.
					s.Close(t.Context())
					s = acquireOwned(t, l, wal.SessionOptions{CollectionWindow: time.Millisecond})
				}
				want = append(want, fmt.Sprint(i))
				if err := enqueueOwned(t, s, want[i], "m").Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			s.Close(t.Context())
			p, err := store.Get(t.Context(), "log/HEAD")
			var head map[string]any
			if err != nil || json.Unmarshal(p, &head) != nil || head["format"] != float64(2) {
				t.Fatalf("HEAD %s, %v; want format 2", p, err)
			}
			if tc.tamper != nil {
				tc.tamper(head)
				if p, err = json.Marshal(head); err != nil || store.Put(t.Context(), "log/HEAD", p) != nil {
					t.Fatal(err)
				}
			}
			slow.Store(true)
			t0 := time.Now()
			h, err := l.ReadHead(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			snap, err := h.Snapshot(t.Context(), l.Genesis(), 64<<20, 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			err = l.Replay(t.Context(), l.Genesis(), snap, 64<<20, func(b wal.Batch) error {
				records, err := b.Read(t.Context())
				for _, r := range records {
					got = append(got, string(r))
				}
				return err
			})
			rounds := int(time.Since(t0) / time.Second)
			slow.Store(false)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("records %v, %v; want %v", got, err, want)
			}
			if rounds != tc.rounds {
				t.Fatalf("%d groups: %d rounds, want %d", groups, rounds, tc.rounds)
			}
		})
	}
}

type slowReads struct {
	objstore.Backend
	slow *atomic.Bool
}

func (b *slowReads) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if b.slow.Load() {
		time.Sleep(time.Second)
	}
	return b.Backend.GetRange(ctx, key, off, n)
}

func (b *slowReads) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if b.slow.Load() {
		time.Sleep(time.Second)
	}
	return b.Backend.GetWithETag(ctx, key)
}
