package storetest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
)

// Conformance runs the behavior every Backend must share against s: the
// same suite on the file backend, S3 (MinIO, AWS, R2) and any other
// provider, so one behavior is verified per implementation. Each subtest
// writes under its own key prefix, so they can run in any order (or
// -run one alone) against one bucket. s should start empty.
func Conformance(t *testing.T, s *objstore.Store) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	// raw is the backend itself: the Store's write gate and ctx checks
	// would answer a cancelled call before the backend ever saw it.
	var raw objstore.Backend
	s.WithBackend(func(b objstore.Backend) objstore.Backend { raw = b; return b })
	t.Run("ConditionalRead", func(t *testing.T) {
		key := "conditional-read/obj"
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
	})
	// GetWithETag and GetIfChanged speak one ETag: each accepts the other's.
	t.Run("ETagInterchangeable", func(t *testing.T) {
		key := "etag-interchange/obj"
		if err := s.Put(ctx, key, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		_, tag, err := s.GetWithETag(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, current, unchanged, err := s.GetIfChanged(ctx, key, tag)
		if err != nil || !unchanged || data != nil || current != tag {
			t.Fatalf("GetIfChanged with GetWithETag's etag %q: %q %q %v %v", tag, data, current, unchanged, err)
		}
		_, current, _, err = s.GetIfChanged(ctx, key, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, tag, err = s.GetWithETag(ctx, key); err != nil || tag != current {
			t.Fatalf("GetWithETag etag %q, GetIfChanged etag %q: %v", tag, current, err)
		}
		if ok, err := s.PutIfMatch(ctx, key, []byte("v2"), current); err != nil || !ok {
			t.Fatalf("PutIfMatch with GetIfChanged's etag: ok=%v err=%v", ok, err)
		}
	})
	t.Run("Range", func(t *testing.T) {
		if err := s.Put(ctx, "range/obj", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		for _, span := range [][2]int64{{0, 1}, {3, 4}, {9, 1}, {0, 10}} {
			got, err := s.GetRange(ctx, "range/obj", span[0], span[1])
			if err != nil || string(got) != "0123456789"[span[0]:span[0]+span[1]] {
				t.Fatalf("range %v: %q %v", span, got, err)
			}
		}
		for _, span := range [][2]int64{{-1, 1}, {0, 0}, {0, -1}, {10, 1}, {9, 2}, {1<<63 - 1, 1}} {
			if _, err := s.GetRange(ctx, "range/obj", span[0], span[1]); !errors.Is(err, objstore.ErrRange) {
				t.Fatalf("invalid range %v: %v", span, err)
			}
		}
		if _, err := s.GetRange(ctx, "range/missing", 0, 1); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("PutGetListDelete", func(t *testing.T) {
		if err := s.Put(ctx, "pgld/2", []byte("two")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "pgld/1", []byte("one")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "pgld/1")
		if err != nil || string(got) != "one" {
			t.Fatalf("Get = %q, %v", got, err)
		}
		if _, err := s.Get(ctx, "pgld/missing"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		keys, err := s.List(ctx, "pgld/")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 || keys[0] != "pgld/1" || keys[1] != "pgld/2" {
			t.Fatalf("bad list: %v", keys)
		}
		if err := s.Delete(ctx, "pgld/1"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "pgld/1"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("delete did not remove object")
		}
		// Deleting what is not there is not an error: deletes are retried.
		if err := s.Delete(ctx, "pgld/1"); err != nil {
			t.Fatalf("Delete of a missing key: %v", err)
		}
	})
	// Keys a URL-based backend must escape: space, plus (a space in query
	// encoding), non-ASCII, and a literal escape sequence.
	t.Run("EscapedKeys", func(t *testing.T) {
		keys := []string{"esc/%2F", "esc/a b", "esc/a+b", "esc/ü"} // sorted
		for _, k := range keys {
			if err := s.Put(ctx, k, []byte(k)); err != nil {
				t.Fatalf("Put %q: %v", k, err)
			}
		}
		for _, k := range keys {
			if got, err := s.Get(ctx, k); err != nil || string(got) != k {
				t.Fatalf("Get %q = %q, %v", k, got, err)
			}
		}
		if got, err := s.List(ctx, "esc/"); err != nil || strings.Join(got, "|") != strings.Join(keys, "|") {
			t.Fatalf("List = %q, %v; want %q", got, err, keys)
		}
		for _, k := range keys {
			if err := s.Delete(ctx, k); err != nil {
				t.Fatalf("Delete %q: %v", k, err)
			}
			if _, err := s.Get(ctx, k); !errors.Is(err, objstore.ErrNotFound) {
				t.Fatalf("Get %q after Delete: %v", k, err)
			}
		}
		if got, err := s.List(ctx, "esc/"); err != nil || len(got) != 0 {
			t.Fatalf("List after Delete = %q, %v", got, err)
		}
	})
	t.Run("EnsureBucketIdempotent", func(t *testing.T) {
		for range 2 {
			if err := s.EnsureBucket(ctx); err != nil {
				t.Fatalf("EnsureBucket on an existing bucket: %v", err)
			}
		}
	})
	// The backend itself honours a cancelled context: a read returns no
	// data, a write or delete reports the cancellation and does not land.
	t.Run("CancelledBackend", func(t *testing.T) {
		const obj = "cancelled/obj"
		if err := s.Put(ctx, obj, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		_, etag, err := s.GetWithETag(ctx, obj)
		if err != nil {
			t.Fatal(err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		reads := map[string]func() ([]byte, error){
			"Get":      func() ([]byte, error) { return raw.Get(cancelled, obj) },
			"GetRange": func() ([]byte, error) { return raw.GetRange(cancelled, obj, 0, 1) },
			"GetWithETag": func() ([]byte, error) {
				data, _, err := raw.GetWithETag(cancelled, obj)
				return data, err
			},
			"GetIfChanged": func() ([]byte, error) {
				data, _, _, err := raw.GetIfChanged(cancelled, obj, "")
				return data, err
			},
			"ListPage": func() ([]byte, error) {
				keys, _, err := raw.ListPage(cancelled, "cancelled/", "", 10)
				if keys != nil {
					return []byte(strings.Join(keys, ",")), err
				}
				return nil, err
			},
			"ListPrefixesPage": func() ([]byte, error) {
				prefixes, _, err := raw.ListPrefixesPage(cancelled, "cancelled/", "", 10)
				if prefixes != nil {
					return []byte(strings.Join(prefixes, ",")), err
				}
				return nil, err
			},
		}
		for name, read := range reads {
			if data, err := read(); !errors.Is(err, context.Canceled) || data != nil {
				t.Errorf("%s under a cancelled ctx: %q %v, want no data and context.Canceled", name, data, err)
			}
		}
		writes := map[string]func(key string) error{
			"Put": func(key string) error { return raw.Put(cancelled, key, []byte("x")) },
			"PutIfAbsent": func(key string) error {
				_, err := raw.PutIfAbsent(cancelled, key, []byte("x"))
				return err
			},
			"PutIfMatch": func(key string) error {
				if err := s.Put(ctx, key, []byte("v1")); err != nil {
					return err
				}
				_, etag, err := s.GetWithETag(ctx, key)
				if err != nil {
					return err
				}
				_, err = raw.PutIfMatch(cancelled, key, []byte("x"), etag)
				return err
			},
		}
		for name, write := range writes {
			key := "cancelled/" + name
			if err := write(key); !errors.Is(err, context.Canceled) {
				t.Errorf("%s under a cancelled ctx: %v, want context.Canceled", name, err)
			}
			if got, err := s.Get(ctx, key); err == nil && string(got) == "x" {
				t.Errorf("%s under a cancelled ctx landed", name)
			}
		}
		deletes := map[string]func() error{
			"Delete":     func() error { return raw.Delete(cancelled, obj) },
			"DeleteMany": func() error { return raw.DeleteMany(cancelled, obj) },
		}
		for name, del := range deletes {
			if err := del(); !errors.Is(err, context.Canceled) {
				t.Errorf("%s under a cancelled ctx: %v, want context.Canceled", name, err)
			}
			if _, tag, err := s.GetWithETag(ctx, obj); err != nil || tag != etag {
				t.Errorf("%s under a cancelled ctx landed: %q %v", name, tag, err)
			}
		}
	})
	// The delimited listing discovery runs on: one entry per child prefix,
	// whatever lives under it, and nothing for an object AT the prefix
	// level. A directory a Delete emptied must be invisible on both
	// backends — S3 has no directories, and the fs backend must not invent
	// any (a namespace that was dropped is gone, not a phantom).
	t.Run("ListPrefixes", func(t *testing.T) {
		for _, k := range []string{"lp/one/meta", "lp/one/wal/0001", "lp/two/meta", "lp/loose"} {
			if err := s.Put(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.ListPrefixes(ctx, "lp/")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != "lp/one/" || got[1] != "lp/two/" {
			t.Fatalf("ListPrefixes = %v, want [lp/one/ lp/two/]", got)
		}
		if err := s.Delete(ctx, "lp/two/meta"); err != nil {
			t.Fatal(err)
		}
		if got, err = s.ListPrefixes(ctx, "lp/"); err != nil {
			t.Fatal(err)
		} else if len(got) != 1 || got[0] != "lp/one/" {
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
		// after need not be a key: the page starts at the first key past it.
		for after, want := range map[string]string{
			"page/":     "page/0,page/a/1,page/a/2,page/b/1,page/c/1,page/z",
			"page/a/15": "page/a/2,page/b/1,page/c/1,page/z",
			"page/b":    "page/b/1,page/c/1,page/z",
			"page/y":    "page/z",
			"page/zz":   "",
		} {
			page, next, err := s.ListPage(ctx, "page/", after, objstore.MaxListPage)
			if got := strings.Join(page, ","); err != nil || got != want || next != "" {
				t.Fatalf("ListPage after %q = %q next=%q err=%v, want %q", after, got, next, err, want)
			}
		}
		// Limit 1 splits every page; limit 2 and the maximum let one page
		// of the underlying listing mix objects (page/0, page/z) with
		// prefixes, and only the prefixes may come back.
		for _, limit := range []int{1, 2, objstore.MaxListPage} {
			var prefixes []string
			for after := ""; ; {
				page, next, err := s.ListPrefixesPage(ctx, "page/", after, limit)
				if err != nil || len(page) > limit {
					t.Fatalf("ListPrefixesPage(limit %d): page=%v next=%q err=%v", limit, page, next, err)
				}
				prefixes = append(prefixes, page...)
				if next == "" {
					break
				}
				if next <= after {
					t.Fatalf("ListPrefixesPage(limit %d): cursor did not advance: %q after %q", limit, next, after)
				}
				after = next
			}
			if got, want := strings.Join(prefixes, ","), "page/a/,page/b/,page/c/"; got != want {
				t.Fatalf("paged prefixes (limit %d) = %q, want %q", limit, got, want)
			}
		}
		for _, limit := range []int{0, -1, objstore.MaxListPage + 1} {
			if _, _, err := s.ListPage(ctx, "page/", "", limit); err == nil {
				t.Fatalf("ListPage accepted limit %d", limit)
			}
			if _, _, err := s.ListPrefixesPage(ctx, "page/", "", limit); err == nil {
				t.Fatalf("ListPrefixesPage accepted limit %d", limit)
			}
		}
	})
	// Every error leaving a backend names the operation and the key. A bare
	// SDK/syscall error names neither, so a failure surfaced three layers up
	// — mid-compaction, mid-replay, inside a query's fan-out — said only
	// that storage was unhappy about something. ErrNotFound must survive the
	// wrapping: callers branch on it.
	t.Run("ErrorsNameOpAndKey", func(t *testing.T) {
		_, err := s.Get(ctx, "errors/no/such/key")
		if !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		for _, want := range []string{"store: get ", "errors/no/such/key"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Get error must contain %q: %v", want, err)
			}
		}
		_, _, err = s.GetWithETag(ctx, "errors/no/such/key")
		if !errors.Is(err, objstore.ErrNotFound) || !strings.Contains(err.Error(), "errors/no/such/key") {
			t.Fatalf("GetWithETag error must name the key and stay ErrNotFound: %v", err)
		}
	})
	t.Run("PutIfAbsent", func(t *testing.T) {
		ok, err := s.PutIfAbsent(ctx, "put-if-absent/1", []byte("first"))
		if err != nil || !ok {
			t.Fatalf("first claim: ok=%v err=%v", ok, err)
		}
		ok, err = s.PutIfAbsent(ctx, "put-if-absent/1", []byte("second"))
		if err != nil || ok {
			t.Fatalf("second claim should lose: ok=%v err=%v", ok, err)
		}
		got, _ := s.Get(ctx, "put-if-absent/1")
		if string(got) != "first" {
			t.Fatalf("loser overwrote winner: %q", got)
		}
	})
	t.Run("ETagCAS", func(t *testing.T) {
		if err := s.Put(ctx, "etag-cas/m", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		data, tag1, err := s.GetWithETag(ctx, "etag-cas/m")
		if err != nil || string(data) != "v1" || tag1 == "" {
			t.Fatalf("get: %q %q %v", data, tag1, err)
		}
		ok, err := s.PutIfMatch(ctx, "etag-cas/m", []byte("v2"), tag1)
		if err != nil || !ok {
			t.Fatalf("cas with fresh etag: ok=%v err=%v", ok, err)
		}
		// Stale etag loses and does not clobber.
		ok, err = s.PutIfMatch(ctx, "etag-cas/m", []byte("v3"), tag1)
		if err != nil || ok {
			t.Fatalf("cas with stale etag: ok=%v err=%v", ok, err)
		}
		data, tag2, _ := s.GetWithETag(ctx, "etag-cas/m")
		if string(data) != "v2" || tag2 == tag1 {
			t.Fatalf("state after races: %q %q", data, tag2)
		}
	})
	// CAS on a missing key: the precondition cannot hold. MinIO creates the
	// object on PutIfMatch for a missing key, while AWS S3 and R2 answer
	// 404, so the MinIO CI job skips this subtest by name.
	t.Run("ETagCASMissing", func(t *testing.T) {
		if err := s.Put(ctx, "etag-cas-missing/m", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		_, tag, err := s.GetWithETag(ctx, "etag-cas-missing/m")
		if err != nil {
			t.Fatal(err)
		}
		ok, err := s.PutIfMatch(ctx, "etag-cas-missing/absent", []byte("x"), tag)
		if ok || (err != nil && !errors.Is(err, objstore.ErrNotFound)) {
			t.Fatalf("cas on missing: ok=%v err=%v, want false and nil or ErrNotFound", ok, err)
		}
		if _, _, err := s.GetWithETag(ctx, "etag-cas-missing/absent"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("cas on missing created the object: %v", err)
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
						if errors.Is(err, objstore.ErrConflict) && !ok {
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
		if err := s.Put(ctx, "delete-many/a", []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "delete-many/b", []byte("2")); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteMany(ctx, "delete-many/a", "delete-many/b", "delete-many/ghost"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "delete-many/a"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("delete-many/a survived")
		}
		if _, err := s.Get(ctx, "delete-many/b"); !errors.Is(err, objstore.ErrNotFound) {
			t.Fatal("delete-many/b survived")
		}
	})
}
