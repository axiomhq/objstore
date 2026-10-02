//go:build unix && !aix && (!solaris || illumos)

package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
)

func fsTempFiles(root string) []string {
	files, _ := filepath.Glob(filepath.Join(root, ".tmp-*"))
	var temps []string
	for _, file := range files {
		if !strings.HasPrefix(filepath.Base(file), ".tmp-lock-") {
			temps = append(temps, file)
		}
	}
	return temps
}

// TestFSSyncDirsDecision pins the publish-time directory-sync decision: the
// parent is synced alone only once THIS process has walked its chain to the
// bucket root — membership in the durable set decides, never the parent's
// mere existence, so an acknowledged publish depends on this process's own
// syncs only. A publish into any directory not yet walked costs the full
// chain walk; a failed walk marks nothing, and dropping the bucket drops
// the set with it. A commit's device bill on this backend is this count
// plus the one file fsync in writeTemp.
func TestFSSyncDirsDecision(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	f.dirSyncs.Store(0)
	syncs := func() int64 { return f.dirSyncs.Load() }
	check := func(want int64, what string) {
		t.Helper()
		if got := syncs(); got != want {
			t.Fatalf("%s: %d directory syncs, want %d", what, got, want)
		}
	}
	if err := f.Put(ctx, "top", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(1, "first publish (the chain is the root alone)")
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "top2", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(1, "second publish at the root level")
	// A directory that exists but that this process never walked: the set,
	// not Stat, decides — the chain is walked.
	if err := os.MkdirAll(filepath.Join(f.root, "pre"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "pre/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(2, "first publish into a pre-existing but unwalked directory (pre and the root)")
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "pre/k2", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(1, "second publish into pre/")
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "a/k1", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(2, "first publish into a/ (a and the root)")
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "a/k2", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(1, "second publish into a/")
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "x/y/z", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(3, "first publish into the fresh chain x/y/ (x/y, x and the root)")
	f.dirSyncs.Store(0)
	ok, err := f.PutIfAbsent(ctx, "a/k3", []byte("v"))
	if err != nil || !ok {
		t.Fatalf("PutIfAbsent: %v %v", ok, err)
	}
	check(1, "PutIfAbsent into the walked a/")
	_, etag, err := f.GetWithETag(ctx, "a/k1")
	if err != nil {
		t.Fatal(err)
	}
	f.dirSyncs.Store(0)
	won, err := f.PutIfMatch(ctx, "a/k1", []byte("w"), etag)
	if err != nil || !won {
		t.Fatalf("PutIfMatch: %v %v", won, err)
	}
	check(1, "PutIfMatch into the walked a/")
	for _, key := range []string{"top", "pre/k2", "a/k2", "x/y/z", "a/k3", "a/k1"} {
		got, err := f.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		want := "w"
		if key != "a/k1" {
			want = "v"
		}
		if string(got) != want {
			t.Fatalf("%s: got %q, want %q", key, got, want)
		}
	}
	// Dropping the bucket destroys the tree: the set goes with it, and a
	// later publish into the recreated bucket walks again.
	if err := f.DropBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	f.dirSyncs.Store(0)
	if err := f.Put(ctx, "a/again", []byte("v")); err != nil {
		t.Fatal(err)
	}
	check(2, "after DropBucket the process walks again (a and the root)")
	if got, err := f.Get(ctx, "a/again"); err != nil || string(got) != "v" {
		t.Fatalf("a/again after re-create: %q %v", got, err)
	}
}

// TestFSWritesBackInChunks: objects around and past writeBackChunk, which
// writeTemp writes back a chunk at a time, publish byte for byte through
// Put, PutIfAbsent and PutIfMatch, urgent or not, and leave no temp file
// behind.
func TestFSWritesBackInChunks(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, writeBackChunk - 1, writeBackChunk, writeBackChunk + 1, 3*writeBackChunk + writeBackChunk/2} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i*31 + n)
		}
		for i, op := range []string{"put", "put-if-absent", "put-if-match", "put", "put-if-absent", "put-if-match"} {
			ctx := ctx
			if i >= 3 {
				ctx = objstore.Urgent(ctx) // written whole, not in chunks
			}
			key := fmt.Sprintf("k/%s/%d-%d", op, n, i)
			var err error
			switch op {
			case "put":
				err = f.Put(ctx, key, data)
			case "put-if-absent":
				var ok bool
				if ok, err = f.PutIfAbsent(ctx, key, data); err == nil && !ok {
					t.Fatalf("%s: claim refused", key)
				}
			case "put-if-match":
				if err = f.Put(ctx, key, []byte("old")); err == nil {
					_, etag, _ := f.GetWithETag(ctx, key)
					var ok bool
					if ok, err = f.PutIfMatch(ctx, key, data, etag); err == nil && !ok {
						t.Fatalf("%s: match refused", key)
					}
				}
			}
			if err != nil {
				t.Fatalf("%s: %v", key, err)
			}
			got, err := f.Get(ctx, key)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("%s: read back %d bytes, err %v; want %d bytes", key, len(got), err, n)
			}
		}
	}
	for _, op := range []string{"put", "put-if-absent", "put-if-match"} {
		if temps := fsTempFiles(filepath.Join(f.root, "k", op)); len(temps) > 0 {
			t.Fatalf("temp files left: %v", temps)
		}
	}
}

// TestFSGetRangeEdges: a raw zero-length read at EOF is empty and error-free
// (Store rejects zero-length ranges; conformance covers non-empty ranges).
func TestFSGetRangeEdges(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "k", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if got, err := f.GetRange(ctx, "k", 10, 0); err != nil || len(got) != 0 {
		t.Fatalf("zero-length read at EOF: %q %v", got, err)
	}
}

// TestFSHonoursContext: the file backend's ops answer a cancelled context
// before touching the tree, and a contended root lock waits on the context
// instead of on a blocking flock. Either failure leaves no temp file behind.
func TestFSHonoursContext(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for name, op := range map[string]func(context.Context) error{
		"Put":         func(c context.Context) error { return f.Put(c, "c", []byte("x")) },
		"PutIfAbsent": func(c context.Context) error { _, err := f.PutIfAbsent(c, "c", []byte("x")); return err },
		"PutIfMatch":  func(c context.Context) error { _, err := f.PutIfMatch(c, "k", []byte("x"), "etag"); return err },
		"DropBucket":  func(c context.Context) error { return f.DropBucket(c) },
	} {
		if err := op(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s with a cancelled context: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(f.root, "c")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a cancelled put landed: %v", err)
	}
	if _, err := f.Get(ctx, "k"); err != nil {
		t.Fatalf("a cancelled drop took effect: %v", err)
	}

	unlock, err := f.lockRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	bounded, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = f.Put(bounded, "held", []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("put against a held root lock: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("put blocked on the flock past its deadline")
	}
	if tmps := fsTempFiles(f.root); len(tmps) != 0 {
		t.Fatalf("temp files leaked by a failed put: %v", tmps)
	}
}

// cancelWhen is a context that reports cancellation once its predicate holds,
// so a test can cancel at a precise point inside an operation.
type cancelWhen struct {
	context.Context
	when func() bool
}

func (c cancelWhen) Err() error {
	if c.when() {
		return context.Canceled
	}
	return nil
}

// TestFSHonoursContextMidOperation: cancellation that arrives after an
// operation has started still wins before any visible effect — PutIfMatch
// never renames once ctx is gone, and a list walk aborts between entries.
func TestFSHonoursContextMidOperation(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	_, etag, err := f.GetWithETag(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}

	// PutIfMatch: cancelled once the temp file exists — after the write, before
	// the rename. The precondition holds, so only ctx can stop publication.
	tmpWritten := cancelWhen{ctx, func() bool {
		tmps := fsTempFiles(f.root)
		return len(tmps) != 0
	}}
	ok, err := f.PutIfMatch(tmpWritten, "k", []byte("x"), etag)
	if ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("PutIfMatch cancelled before rename: ok=%v err=%v", ok, err)
	}
	if got, _ := f.Get(ctx, "k"); string(got) != "v" {
		t.Fatalf("cancelled PutIfMatch published: %q", got)
	}
	if tmps := fsTempFiles(f.root); len(tmps) != 0 {
		t.Fatalf("temp files leaked by a cancelled PutIfMatch: %v", tmps)
	}

	for i := range 8 {
		if err := f.Put(ctx, "p/"+string(rune('a'+i)), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	checks := 0
	walking := cancelWhen{ctx, func() bool { checks++; return checks > 1 }}
	if _, _, err := f.ListPage(walking, "", "", 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListPage cancelled mid-walk: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := New(t.TempDir(), "none").EnsureBucket(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureBucket with a cancelled context: %v", err)
	}
}

func TestFSKeyLockIsolation(t *testing.T) {
	root := t.TempDir()
	f, other := New(root, "b"), New(root, "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "held", []byte("old")); err != nil {
		t.Fatal(err)
	}
	_, etag, err := f.GetWithETag(ctx, "held")
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := f.lockKey(ctx, "held")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	bounded, cancelRoot := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelRoot()
	if release, err := other.lockRoot(bounded); err == nil {
		release()
		t.Fatal("legacy writer acquired the root while a new writer held a key")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	// Distinct store instances use OS locks, not process-local mutexes.
	for name, op := range map[string]func(context.Context) error{
		"put":    func(c context.Context) error { return other.Put(c, "held", []byte("new")) },
		"absent": func(c context.Context) error { _, err := other.PutIfAbsent(c, "held", []byte("new")); return err },
		"match":  func(c context.Context) error { _, err := other.PutIfMatch(c, "held", []byte("new"), etag); return err },
		"delete": func(c context.Context) error { return other.Delete(c, "held") },
	} {
		t.Run(name, func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			if err := op(c); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
		})
	}
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := other.Put(c, "independent", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if got, err := f.Get(ctx, "held"); err != nil || string(got) != "old" {
		t.Fatalf("held changed: %q %v", got, err)
	}
	keys, _, err := f.ListPage(ctx, "", "", 1000)
	if err != nil || len(keys) != 2 {
		t.Fatalf("internal locks leaked: %v %v", keys, err)
	}
}

func TestFSConcurrentCASWinner(t *testing.T) {
	root := t.TempDir()
	f := New(root, "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "key", []byte("before")); err != nil {
		t.Fatal(err)
	}
	_, etag, err := f.GetWithETag(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	winners := make(chan string, 16)
	start := make(chan struct{})
	for i := range 16 {
		wg.Go(func() {
			<-start
			value := string(rune('a' + i))
			ok, err := New(root, "b").PutIfMatch(ctx, "key", []byte(value), etag)
			if err != nil {
				t.Error(err)
			}
			if ok {
				winners <- value
			}
		})
	}
	close(start)
	wg.Wait()
	close(winners)
	if len(winners) != 1 {
		t.Fatalf("got %d winners", len(winners))
	}
	winner := <-winners
	if got, err := f.Get(ctx, "key"); err != nil || string(got) != winner {
		t.Fatalf("got %q, winner %q: %v", got, winner, err)
	}
}

func TestFSKeyLockAcrossProcesses(t *testing.T) {
	ctx := context.Background()
	if root := os.Getenv("OBJSTORE_TEST_FS_LOCK_ROOT"); root != "" {
		f := New(root, "b")
		bounded, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if err := f.Put(bounded, "held", []byte("wrong")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("child bypassed parent lock: %v", err)
		}
		boundedOther, cancelOther := context.WithTimeout(ctx, time.Second)
		defer cancelOther()
		if err := f.Put(boundedOther, "independent", []byte("child")); err != nil {
			t.Fatal(err)
		}
		return
	}
	root := t.TempDir()
	f := New(root, "b")
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	unlock, err := f.lockKey(ctx, "held")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(bounded, os.Args[0], "-test.run=^TestFSKeyLockAcrossProcesses$")
	cmd.Env = append(os.Environ(), "OBJSTORE_TEST_FS_LOCK_ROOT="+root)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, output)
	}
	if _, err := f.Get(ctx, "held"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("blocked write landed: %v", err)
	}
	if got, err := f.Get(ctx, "independent"); err != nil || string(got) != "child" {
		t.Fatalf("child write: %q %v", got, err)
	}
}

// TestIsolatedKeysDoNotShareAStripe: a write holds its key's stripe
// through the directory fsync; a WAL commit (a key IsolatedKeys selects)
// whose key hashes to the same stripe as a data key being written must not
// wait for it. The same key still serializes.
func TestIsolatedKeysDoNotShareAStripe(t *testing.T) {
	f := New(t.TempDir(), "bucket")
	f.IsolatedKeys = func(key string) bool { return strings.Contains(key, "/wal/") }
	if err := os.MkdirAll(f.root, 0o755); err != nil {
		t.Fatal(err)
	}
	const walKey = "ns/x/wal/00000000000000000001"
	want := sha256.Sum256([]byte(walKey))
	dataKey := ""
	for i := 0; dataKey == ""; i++ {
		k := fmt.Sprintf("ns/x/seg/pack-%d.bin", i)
		if sum := sha256.Sum256([]byte(k)); sum[0] == want[0] {
			dataKey = k
		}
	}
	unlock, err := f.lockKey(t.Context(), dataKey) // a bulk put, mid-fsync
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if ok, err := f.putIfAbsent(ctx, walKey, []byte("page")); err != nil || !ok {
		t.Fatalf("WAL commit behind a data key's stripe: ok=%v err=%v", ok, err)
	}
	held, err := f.lockKey(t.Context(), walKey)
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := f.putIfAbsent(ctx, walKey, []byte("again")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a WAL key's own writers no longer serialize: %v", err)
	}
}

// TestFSPutIfAbsentRetrySyncsAfterFailedDirSync: a PutIfAbsent whose link
// landed but whose directory sync failed is (true, err); the retry finds
// the object (false, nil) and must sync the directory itself, or a
// read-back adoption would rest on an entry nothing made durable.
func TestFSPutIfAbsentRetrySyncsAfterFailedDirSync(t *testing.T) {
	ctx := context.Background()
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprint("warm=", warm), func(t *testing.T) {
			f := New(t.TempDir(), "b")
			if err := f.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			if warm { // the directory chain is already proven durable
				if err := f.Put(ctx, "log/0", []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			injected := errors.New("injected dirsync failure")
			f.syncFault = func(string) error { return injected }
			ok, err := f.PutIfAbsent(ctx, "log/1", []byte("v"))
			if !ok || !errors.Is(err, injected) {
				t.Fatalf("faulted dirsync: ok=%v err=%v, want (true, injected)", ok, err)
			}
			f.syncFault = nil
			f.dirSyncs.Store(0)
			ok, err = f.PutIfAbsent(ctx, "log/1", []byte("v"))
			if ok || err != nil {
				t.Fatalf("retry: ok=%v err=%v, want (false, nil)", ok, err)
			}
			if n := f.dirSyncs.Load(); n == 0 {
				t.Fatal("retry that found its own unsynced object issued no directory sync")
			}
			f.dirSyncs.Store(0)
			if ok, err := f.PutIfAbsent(ctx, "log/1", []byte("v")); ok || err != nil || f.dirSyncs.Load() != 1 {
				t.Fatalf("lost race on a synced directory: ok=%v err=%v syncs=%d, want one sync", ok, err, f.dirSyncs.Load())
			}
		})
	}
}

// TestFSListOrderAndBound: listing yields S3's lexical key order even where
// it differs from directory order ("a-c" < "a/b" since '-' < '/'), pages
// correctly across that boundary, and a page reads only the directories it
// needs rather than the whole bucket.
func TestFSListOrderAndBound(t *testing.T) {
	ctx := context.Background()
	f := New(t.TempDir(), "b")
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"a-c", "a.d", "a/b", "a/b/c", "a0", "b/x/y"}
	slices.Sort(want)
	for _, k := range []string{"a/b/c", "a-c", "a/b", "b/x/y", "a0", "a.d"} {
		// "a/b" and "a/b/c" cannot both exist as a file and a directory.
		if k == "a/b" {
			continue
		}
		if err := f.Put(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	want = slices.DeleteFunc(want, func(k string) bool { return k == "a/b" })
	for limit := 1; limit <= len(want)+1; limit++ {
		var got []string
		for after := ""; ; {
			page, next, err := f.ListPage(ctx, "", after, limit)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, page...)
			if next == "" {
				break
			}
			after = next
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d: %v, want %v", limit, got, want)
		}
	}
	for prefix, want := range map[string][]string{
		"a":     {"a-c", "a.d", "a/b/c", "a0"},
		"a/":    {"a/b/c"},
		"a/b/c": {"a/b/c"},
		"b/x":   {"b/x/y"},
		"c/":    nil,
		"a//":   nil,
		"../":   nil,
	} {
		keys, next, err := f.ListPage(ctx, prefix, "", 100)
		if err != nil || next != "" || !slices.Equal(keys, want) {
			t.Fatalf("prefix %q: %v %q %v, want %v", prefix, keys, next, err, want)
		}
	}
	prefixes, _, err := f.ListPrefixesPage(ctx, "", "", 100)
	if err != nil || !slices.Equal(prefixes, []string{"a/", "b/"}) {
		t.Fatalf("prefixes: %v %v", prefixes, err)
	}
	if err := os.MkdirAll(filepath.Join(f.root, "empty", "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	prefixes, _, err = f.ListPrefixesPage(ctx, "", "a/", 100)
	if err != nil || !slices.Equal(prefixes, []string{"b/"}) {
		t.Fatalf("prefixes after a/ with an empty directory: %v %v", prefixes, err)
	}

	// A wide tree: 64 directories of 16 keys each. One small page reads a
	// handful of directories, not 65.
	for d := range 64 {
		for k := range 16 {
			if err := f.Put(ctx, fmt.Sprintf("wide/%02d/%02d", d, k), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.dirReads.Store(0)
	keys, next, err := f.ListPage(ctx, "wide/", "wide/10/05", 20)
	if err != nil || len(keys) != 20 || keys[0] != "wide/10/06" || next != "wide/11/09" {
		t.Fatalf("wide page: %v %q %v", keys, next, err)
	}
	if n := f.dirReads.Load(); n > 4 {
		t.Fatalf("one 20-key page read %d directories", n)
	}
	f.dirReads.Store(0)
	if keys, _, err := f.ListPage(ctx, "wide/63/", "", 100); err != nil || len(keys) != 16 || f.dirReads.Load() != 1 {
		t.Fatalf("one directory's keys: %d keys, %d reads, %v", len(keys), f.dirReads.Load(), err)
	}
}

// TestFSInvalidKeyIsErrInvalidKey: rejected keys wrap objstore.ErrInvalidKey.
func TestFSInvalidKeyIsErrInvalidKey(t *testing.T) {
	f := New(t.TempDir(), "b")
	if err := f.Put(context.Background(), "a//b", nil); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Fatalf("invalid key: %v", err)
	}
}

func BenchmarkFSListPage(b *testing.B) {
	ctx := context.Background()
	f := New(b.TempDir(), "b")
	if err := f.EnsureBucket(ctx); err != nil {
		b.Fatal(err)
	}
	for d := range 100 {
		for k := range 100 {
			if err := f.Put(objstore.Urgent(ctx), fmt.Sprintf("ns/%02d/%02d", d, k), nil); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := f.ListPage(ctx, "ns/50/", "", 100); err != nil {
			b.Fatal(err)
		}
	}
}

// TestFSIDIsTheBucketDirectory: two Opens of one bucket share an ID; another
// bucket under the same root does not.
func TestFSIDIsTheBucketDirectory(t *testing.T) {
	root := t.TempDir()
	a, again, other := Open(root, "a", objstore.Config{}), Open(root, "a", objstore.Config{}), Open(root, "b", objstore.Config{})
	if a.ID() != again.ID() || a.ID() == other.ID() || !strings.HasPrefix(a.ID(), "file://") {
		t.Fatalf("IDs %q %q %q", a.ID(), again.ID(), other.ID())
	}
}

func TestFSListPrefixesPropagatesWalkCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	f := New(t.TempDir(), "b")
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "a/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Cancel in hasObject's walk, after the outer walk has visited a/.
	walking := cancelWhen{ctx, func() bool { return f.dirReads.Load() >= 2 }}
	if _, _, err := f.ListPrefixesPage(walking, "", "", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled subtree walk: %v, want context.Canceled", err)
	}
}

func TestFSWriteTempStopsOnCancellation(t *testing.T) {
	for _, size := range []int{writeBackChunk, 3 * writeBackChunk} {
		t.Run(string(rune('0'+size/writeBackChunk)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			f := New(t.TempDir(), "b")
			if err := f.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			var timings objstore.Timings
			writing := cancelWhen{objstore.WithTimings(ctx, &timings), func() bool {
				for _, p := range fsTempFiles(f.root) {
					if st, err := os.Stat(p); err == nil && st.Size() >= writeBackChunk {
						return true
					}
				}
				return false
			}}
			if err := f.Put(writing, "k", make([]byte, size)); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled write: %v", err)
			}
			for _, a := range timings.Attrs() {
				if (a.Key == "sys_write_n" && a.Value.Int64() != 1) ||
					((a.Key == "sys_writeback_n" || a.Key == "sys_fsync_n") && a.Value.Int64() != 0) {
					t.Errorf("work continued after cancellation: %s=%d", a.Key, a.Value.Int64())
				}
			}
			if temps := fsTempFiles(f.root); len(temps) != 0 {
				t.Fatalf("temp files leaked: %v", temps)
			}
		})
	}
}

func TestFSEnsureBucketSyncsCreatedParents(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	root := t.TempDir()
	f := New(filepath.Join(root, "a", "b"), "bucket")
	var dirs []string
	f.syncFault = func(dir string) error { dirs = append(dirs, dir); return nil }
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	var want []string
	for dir := f.root; ; dir = filepath.Dir(dir) {
		want = append(want, dir)
		if dir == filepath.Dir(dir) {
			break
		}
	}
	if !slices.Equal(dirs, want) {
		t.Fatalf("synced directories %v, want %v", dirs, want)
	}
	// Even an existing bucket must repay a previous failed sync.
	injected := errors.New("directory sync failed")
	f.syncFault = func(string) error { return injected }
	if err := f.EnsureBucket(ctx); !errors.Is(err, injected) {
		t.Fatalf("EnsureBucket lost sync error: %v", err)
	}
}

func TestFSEnsureBucketRetrySyncsFailedAncestor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	root := t.TempDir()
	f := New(filepath.Join(root, "a", "b"), "bucket")
	injected := errors.New("directory sync failed")
	f.syncFault = func(dir string) error {
		if dir == root {
			return injected
		}
		return nil
	}
	if err := f.EnsureBucket(ctx); !errors.Is(err, injected) {
		t.Fatalf("first EnsureBucket: %v, want sync failure", err)
	}
	var dirs []string
	f.syncFault = func(dir string) error { dirs = append(dirs, dir); return nil }
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(dirs, root) {
		t.Fatalf("retry synced %v, omitted failed ancestor %s", dirs, root)
	}
}

func TestFSDropBucketSyncsParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	root := t.TempDir()
	f := New(root, "bucket")
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	var dirs []string
	f.syncFault = func(dir string) error {
		if _, err := os.Stat(f.root); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("bucket still exists at sync: %v", err)
		}
		dirs = append(dirs, dir)
		return nil
	}
	if err := f.DropBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dirs, []string{root}) {
		t.Fatalf("synced directories %v, want [%s]", dirs, root)
	}
	// A retry must repay a failed sync even though the bucket is gone.
	injected := errors.New("directory sync failed")
	f.syncFault = func(string) error { return injected }
	if err := f.DropBucket(ctx); !errors.Is(err, injected) {
		t.Fatalf("DropBucket lost sync error: %v", err)
	}
	dirs = nil
	f.syncFault = func(dir string) error { dirs = append(dirs, dir); return nil }
	if err := f.DropBucket(ctx); err != nil || !slices.Equal(dirs, []string{root}) {
		t.Fatalf("retry synced %v: %v", dirs, err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err := f.DropBucket(ctx); err != nil {
		t.Fatalf("DropBucket with a missing parent: %v", err)
	}
}

func TestFSListPrefixesSkipsVanishedChild(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "replaced"}[replaced], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			f := New(t.TempDir(), "b")
			if err := f.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a/k", "b/k"} {
				if err := f.Put(ctx, key, []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			changed := false
			// Remove a/ after ReadDir has captured it, before hasObject reads it.
			walking := cancelWhen{ctx, func() bool {
				if !changed && f.dirReads.Load() == 1 {
					changed = true
					path := filepath.Join(f.root, "a")
					if err := os.RemoveAll(path); err != nil {
						t.Fatal(err)
					}
					if replaced {
						if err := os.WriteFile(path, []byte("v"), 0o644); err != nil {
							t.Fatal(err)
						}
					}
				}
				return false
			}}
			got, next, err := f.ListPrefixesPage(walking, "", "", 10)
			if err != nil || next != "" || !slices.Equal(got, []string{"b/"}) {
				t.Fatalf("listing after child vanished: %v %q %v, want [b/] and no error", got, next, err)
			}
		})
	}
}

func TestFSPutIfAbsentSyncsOtherHandlesEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	root := t.TempDir()
	f, other := New(root, "b"), New(root, "b")
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "a/warm", []byte("v")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("directory sync failed")
	other.syncFault = func(string) error { return injected }
	if ok, err := other.PutIfAbsent(ctx, "a/k", []byte("v")); !ok || !errors.Is(err, injected) {
		t.Fatalf("other handle: %v %v", ok, err)
	}
	f.dirSyncs.Store(0)
	if ok, err := f.PutIfAbsent(ctx, "a/k", []byte("v")); ok || err != nil {
		t.Fatalf("lost race: %v %v", ok, err)
	}
	if got := f.dirSyncs.Load(); got != 1 {
		t.Fatalf("lost race synced %d directories, want 1", got)
	}
}

func TestFSDeleteSyncsDistinctParents(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			f := New(t.TempDir(), "b")
			if err := f.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			keys := []string{"a/1", "a/2", "b/1"}
			for _, key := range keys {
				if err := f.Put(ctx, key, []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			var dirs []string
			f.syncFault = func(dir string) error { dirs = append(dirs, dir); return nil }
			var err error
			want := []string{filepath.Join(f.root, "a")}
			if batch {
				err = f.DeleteMany(ctx, append(keys, "missing/k")...)
				want = append(want, filepath.Join(f.root, "b"))
			} else {
				err = f.Delete(ctx, keys[0])
			}
			if err != nil || !slices.Equal(dirs, want) {
				t.Fatalf("delete synced %v, want %v: %v", dirs, want, err)
			}
			if err := f.Put(ctx, "err/k", []byte("v")); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("directory sync failed")
			f.syncFault = func(string) error { return injected }
			if batch {
				err = f.DeleteMany(ctx, "err/k")
			} else {
				err = f.Delete(ctx, "err/k")
			}
			if !errors.Is(err, injected) {
				t.Fatalf("delete lost sync error: %v", err)
			}
		})
	}
}

// TestFSEnsureBucketUnderUnreadableAncestor: a directory we may cross but not
// read (mode 0311) was not created by EnsureBucket, so it is not synced and
// does not fail the call.
func TestFSEnsureBucketUnderUnreadableAncestor(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every directory")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := filepath.Join(t.TempDir(), "p")
	if err := os.Mkdir(p, 0o311); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(p, 0o755) })
	if err := New(filepath.Join(p, "data"), "b").EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestFSEnsureBucketReportsUnsyncableBucket: only an ancestor we may not open
// is skipped. A bucket directory we cannot open, or an ancestor whose fsync
// is refused, fails the call.
func TestFSEnsureBucketReportsUnsyncableBucket(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every directory")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	root := t.TempDir()
	bucket := filepath.Join(root, "b")
	if err := os.Mkdir(bucket, 0o311); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(bucket, 0o755) })
	if err := New(root, "b").EnsureBucket(ctx); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("unreadable bucket: %v, want ErrPermission", err)
	}

	f := New(t.TempDir(), "b")
	f.syncFault = func(dir string) error {
		if dir != f.root {
			return os.ErrPermission // refused fsync, not a refused open
		}
		return nil
	}
	if err := f.EnsureBucket(ctx); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("refused ancestor fsync: %v, want ErrPermission", err)
	}
}
