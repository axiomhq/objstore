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

// TestFSGetRangeEdges pins the range contract at EOF: a read ending exactly
// at the last byte succeeds, one byte past it is objstore.ErrRange before any read,
// and a zero-length read at EOF is empty and error-free.
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

func TestFSGetRangeEdges(t *testing.T) {
	f := New(t.TempDir(), "b")
	ctx := context.Background()
	if err := f.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(ctx, "k", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	got, err := f.GetRange(ctx, "k", 9, 1)
	if err != nil || string(got) != "9" {
		t.Fatalf("read ending exactly at EOF: %q %v", got, err)
	}
	if _, err := f.GetRange(ctx, "k", 10, 1); !errors.Is(err, objstore.ErrRange) {
		t.Fatalf("read past EOF: %v", err)
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
		"Get":         func(c context.Context) error { _, err := f.Get(c, "k"); return err },
		"ListPage":    func(c context.Context) error { _, _, err := f.ListPage(c, "", "", 10); return err },
		"Delete":      func(c context.Context) error { return f.Delete(c, "k") },
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
		t.Fatalf("a cancelled delete/drop took effect: %v", err)
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value := string(rune('a' + i))
			ok, err := New(root, "b").PutIfMatch(ctx, "key", []byte(value), etag)
			if err != nil {
				t.Error(err)
			}
			if ok {
				winners <- value
			}
		}()
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

// TestWALKeysDoNotShareAFoldWritesStripe: a write holds its key's stripe
// through the directory fsync; a WAL commit whose key hashes to the same
// stripe as a data key being written must not wait for it. The same key
// still serializes.
func TestWALKeysDoNotShareAFoldWritesStripe(t *testing.T) {
	f := New(t.TempDir(), "bucket")
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
