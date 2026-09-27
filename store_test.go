package objstore_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

// TestS3 runs the conformance suite against MinIO (skips without
// OBJSTORE_TEST_S3); TestFS runs the identical suite against the file://
// backend — one behavior, verified per implementation.
func TestS3(t *testing.T) { runConformance(t, storetest.New(t)) }

func TestFS(t *testing.T) {
	s, err := objstore.New(context.Background(), "file://"+t.TempDir(), "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	runConformance(t, s)
}

func runConformance(t *testing.T, s *objstore.Store) {
	ctx := context.Background()
	t.Run("ConditionalRead", func(t *testing.T) {
		key := "conditional-read"
		if err := s.Put(ctx, key, []byte("first")); err != nil {
			t.Fatal(err)
		}
		data, etag, unchanged, err := s.GetIfChanged(ctx, key, "")
		if err != nil || unchanged || string(data) != "first" || etag == "" {
			t.Fatalf("initial: %q %q %v %v", data, etag, unchanged, err)
		}
		data, current, unchanged, err := s.GetIfChanged(ctx, key, etag)
		if err != nil || !unchanged || data != nil || current != etag {
			t.Fatalf("unchanged: %q %q %v %v", data, current, unchanged, err)
		}
		if err := s.Put(ctx, key, []byte("second")); err != nil {
			t.Fatal(err)
		}
		data, current, unchanged, err = s.GetIfChanged(ctx, key, etag)
		if err != nil || unchanged || string(data) != "second" || current == "" || current == etag {
			t.Fatalf("changed: %q %q %v %v", data, current, unchanged, err)
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		data, current, unchanged, err = s.GetIfChanged(ctx, key, current)
		if !errors.Is(err, objstore.ErrNotFound) || unchanged || data != nil || current != "" {
			t.Fatalf("absent: %q %q %v %v", data, current, unchanged, err)
		}
		if err := s.Put(ctx, key, nil); err != nil {
			t.Fatal(err)
		}
		data, current, unchanged, err = s.GetIfChanged(ctx, key, "")
		if err != nil || unchanged || len(data) != 0 || current == "" {
			t.Fatalf("empty object: %q %q %v %v", data, current, unchanged, err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, unchanged, err = s.GetIfChanged(cancelled, key, current)
		if !errors.Is(err, context.Canceled) || unchanged {
			t.Fatalf("cancelled: %v %v", unchanged, err)
		}
	})
	t.Run("Range", func(t *testing.T) {
		if err := s.Put(ctx, "range", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		for _, span := range [][2]int64{{0, 1}, {3, 4}, {9, 1}, {0, 10}} {
			got, err := s.GetRange(ctx, "range", span[0], span[1])
			if err != nil || string(got) != "0123456789"[span[0]:span[0]+span[1]] {
				t.Fatalf("range %v: %q %v", span, got, err)
			}
		}
		for _, span := range [][2]int64{{-1, 1}, {0, 0}, {0, -1}, {10, 1}, {9, 2}, {1<<63 - 1, 1}} {
			if _, err := s.GetRange(ctx, "range", span[0], span[1]); !errors.Is(err, objstore.ErrRange) {
				t.Fatalf("invalid range %v: %v", span, err)
			}
		}
		if _, err := s.GetRange(ctx, "missing-range", 0, 1); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal(err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.GetRange(cancelled, "range", 0, 1); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("PutGetListDelete", func(t *testing.T) {
		if err := s.Put(ctx, "a/2", []byte("two")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "a/1", []byte("one")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "a/1")
		if err != nil || string(got) != "one" {
			t.Fatalf("Get = %q, %v", got, err)
		}
		if _, err := s.Get(ctx, "missing"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		keys, err := s.List(ctx, "a/")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 || keys[0] != "a/1" || keys[1] != "a/2" {
			t.Fatalf("bad list: %v", keys)
		}
		if err := s.Delete(ctx, "a/1"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "a/1"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("delete did not remove object")
		}
	})
	// The delimited listing discovery runs on: one entry per child prefix,
	// whatever lives under it, and nothing for an object AT the prefix
	// level. A directory a Delete emptied must be invisible on both
	// backends — S3 has no directories, and the fs backend must not invent
	// any (a namespace that was dropped is gone, not a phantom).
	t.Run("ListPrefixes", func(t *testing.T) {
		for _, k := range []string{"ns/one/meta", "ns/one/wal/0001", "ns/two/meta", "ns/loose"} {
			if err := s.Put(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.ListPrefixes(ctx, "ns/")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != "ns/one/" || got[1] != "ns/two/" {
			t.Fatalf("ListPrefixes = %v, want [ns/one/ ns/two/]", got)
		}
		for _, k := range []string{"ns/two/meta"} {
			if err := s.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		if got, err = s.ListPrefixes(ctx, "ns/"); err != nil {
			t.Fatal(err)
		} else if len(got) != 1 || got[0] != "ns/one/" {
			t.Fatalf("an emptied prefix is still listed: %v", got)
		}
	})
	t.Run("PagedLists", func(t *testing.T) {
		// page/0 sorts before every child prefix: on S3 a limit-1 delimited
		// page then holds one object and no prefix while still truncated.
		for _, k := range []string{"page/0", "page/a/1", "page/a/2", "page/b/1", "page/c/1", "page/z"} {
			if err := s.Put(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		var keys []string
		for after := ""; ; {
			page, next, err := s.ListPage(ctx, "page/", after, 2)
			if err != nil || len(page) > 2 {
				t.Fatalf("ListPage: page=%v next=%q err=%v", page, next, err)
			}
			keys = append(keys, page...)
			if next == "" {
				break
			}
			if next <= after {
				t.Fatalf("cursor did not advance: %q after %q", next, after)
			}
			after = next
		}
		if got, want := strings.Join(keys, ","), "page/0,page/a/1,page/a/2,page/b/1,page/c/1,page/z"; got != want {
			t.Fatalf("paged keys = %q, want %q", got, want)
		}
		var prefixes []string
		for after := ""; ; {
			page, next, err := s.ListPrefixesPage(ctx, "page/", after, 1)
			if err != nil || len(page) > 1 {
				t.Fatalf("ListPrefixesPage: page=%v next=%q err=%v", page, next, err)
			}
			prefixes = append(prefixes, page...)
			if next == "" {
				break
			}
			after = next
		}
		if got, want := strings.Join(prefixes, ","), "page/a/,page/b/,page/c/"; got != want {
			t.Fatalf("paged prefixes = %q, want %q", got, want)
		}
		if _, _, err := s.ListPage(ctx, "page/", "", 0); err == nil {
			t.Fatal("ListPage accepted zero limit")
		}
	})
	// Every error leaving a backend names the operation and the key. A bare
	// SDK/syscall error names neither, so a failure surfaced three layers up
	// — mid-compaction, mid-replay, inside a query's fan-out — said only
	// that storage was unhappy about something. ErrNotFound must survive the
	// wrapping: callers branch on it.
	t.Run("ErrorsNameOpAndKey", func(t *testing.T) {
		_, err := s.Get(ctx, "no/such/key")
		if !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		for _, want := range []string{"store: get ", "no/such/key"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Get error must contain %q: %v", want, err)
			}
		}
		_, _, err = s.GetWithETag(ctx, "no/such/key")
		if !errors.Is(err, objstore.ErrNotFound) || !strings.Contains(err.Error(), "no/such/key") {
			t.Fatalf("GetWithETag error must name the key and stay ErrNotFound: %v", err)
		}
	})
	t.Run("PutIfAbsent", func(t *testing.T) {
		ok, err := s.PutIfAbsent(ctx, "wal/1", []byte("first"))
		if err != nil || !ok {
			t.Fatalf("first claim: ok=%v err=%v", ok, err)
		}
		ok, err = s.PutIfAbsent(ctx, "wal/1", []byte("second"))
		if err != nil || ok {
			t.Fatalf("second claim should lose: ok=%v err=%v", ok, err)
		}
		got, _ := s.Get(ctx, "wal/1")
		if string(got) != "first" {
			t.Fatalf("loser overwrote winner: %q", got)
		}
	})
	t.Run("ETagCAS", func(t *testing.T) {
		if err := s.Put(ctx, "m", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		data, tag1, err := s.GetWithETag(ctx, "m")
		if err != nil || string(data) != "v1" || tag1 == "" {
			t.Fatalf("get: %q %q %v", data, tag1, err)
		}
		ok, err := s.PutIfMatch(ctx, "m", []byte("v2"), tag1)
		if err != nil || !ok {
			t.Fatalf("cas with fresh etag: ok=%v err=%v", ok, err)
		}
		// Stale etag loses and does not clobber.
		ok, err = s.PutIfMatch(ctx, "m", []byte("v3"), tag1)
		if err != nil || ok {
			t.Fatalf("cas with stale etag: ok=%v err=%v", ok, err)
		}
		data, tag2, _ := s.GetWithETag(ctx, "m")
		if string(data) != "v2" || tag2 == tag1 {
			t.Fatalf("state after races: %q %q", data, tag2)
		}
		// CAS on a missing key: precondition cannot hold.
		ok, err = s.PutIfMatch(ctx, "missing", []byte("x"), tag1)
		if err != nil || ok {
			t.Fatalf("cas on missing: ok=%v err=%v", ok, err)
		}
		if _, _, err := s.GetWithETag(ctx, "missing"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})
	for _, conditional := range []string{"absent", "match"} {
		t.Run("ConcurrentClaim/"+conditional, func(t *testing.T) {
			key := "concurrent/" + conditional
			var tag string
			if conditional == "match" {
				if err := s.Put(ctx, key, []byte("initial")); err != nil {
					t.Fatal(err)
				}
				var err error
				_, tag, err = s.GetWithETag(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			winners := make(chan string, 16)
			for i := range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					value := string(rune('a' + i))
					var ok bool
					var err error
					if conditional == "match" {
						ok, err = s.PutIfMatch(ctx, key, []byte(value), tag)
					} else {
						ok, err = s.PutIfAbsent(ctx, key, []byte(value))
					}
					if err != nil {
						// S3 may return 409 for an overlapping conditional PUT;
						// that is an unresolved attempt, never a claimed win.
						var apiErr smithy.APIError
						if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConditionalRequestConflict" && !ok {
							return
						}
						t.Error(err)
					}
					if ok {
						winners <- value
					}
				}()
			}
			close(start)
			wg.Wait()
			if len(winners) != 1 {
				t.Fatalf("got %d winners, want exactly one", len(winners))
			}
			winner := <-winners
			if got, err := s.Get(ctx, key); err != nil || string(got) != winner {
				t.Fatalf("stored %q, winner %q: %v", got, winner, err)
			}
		})
	}
	t.Run("DeleteMany", func(t *testing.T) {
		if err := s.Put(ctx, "d/a", []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "d/b", []byte("2")); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteMany(ctx, "d/a", "d/b", "ghost"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "d/a"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("d/a survived")
		}
		if _, err := s.Get(ctx, "d/b"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("d/b survived")
		}
	})
}

// TestFSRejectsTraversal: keys become filesystem paths — a trust boundary
// the S3 backend never had. Terminal .lock/.tmp-* names are reserved for
// backend internals, traversal and empty elements are rejected, and other
// nested dot names remain available to namespace keys.
func TestFSRejectsTraversal(t *testing.T) {
	s, err := objstore.New(context.Background(), "file://"+t.TempDir(), "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"../evil", "a/../../evil", "a/./b", "/abs", "", "a//b", "a/", ".lock", ".tmp-1", "a/.lock", "a/.tmp-1"} {
		if err := s.Put(context.Background(), key, []byte("x")); err == nil {
			t.Fatalf("traversal key %q accepted", key)
		}
	}
	if err := s.Put(context.Background(), "a/b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "a/.namespace", []byte("x")); err != nil {
		t.Fatalf("nested dot name: %v", err)
	}
	keys, _, err := s.ListPage(context.Background(), "a/", "", 10)
	if err != nil || !slices.Contains(keys, "a/.namespace") {
		t.Fatalf("nested dot name missing from list: %v, %v", keys, err)
	}
	if b, err := s.Get(context.Background(), "a//b"); err == nil {
		t.Fatalf("a//b aliased onto a/b: %q", b)
	}
}

// TestFaultyPassthrough: a disarmed Fault is the backend it wraps — it runs
// the same conformance suite TestS3 and TestFS run.
func TestFaultyPassthrough(t *testing.T) {
	s, _ := storetest.NewFaulty(t)
	runConformance(t, s)
}
