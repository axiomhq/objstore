//go:build unix && !aix && (!solaris || illumos)

package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/axiomhq/objstore"
)

// Backend is the file:// backend: write-once objects as files, O_EXCL-style
// links for PutIfAbsent, temp+rename for atomic Put, striped flocks plus a
// content hash for PutIfMatch. Dev and hermetic-CI backend; Unix with
// flock only (see the package doc). Same single-writer-per-bucket
// contract as S3.
type Backend struct {
	// IsolatedKeys, when set, gives the keys it reports true for a set of
	// lock stripes of their own. A write holds its key's stripe through the
	// directory fsync, so a latency-critical key (a write-ahead log commit)
	// that hashes to a bulk write's stripe (1 in 256) waits for that
	// fsync; isolating the log's keys removes the wait. It must depend on
	// the key alone, so one key always takes one stripe, and be the same
	// in every process sharing the bucket. Set it before first use.
	IsolatedKeys func(key string) bool

	root string
	// durable is the set of directory paths whose chain up to the bucket
	// root THIS PROCESS has fully synced (see syncPublishedDir): it decides
	// the publish-time sync, so an acknowledged write's durability rests on
	// this process's own syncs alone — never on another writer's later
	// fsync. Bounded by the number of distinct key directories the process
	// has published into.
	durable sync.Map // string -> struct{}
	// dirSyncs counts the directory fsyncs this store has issued. Test
	// observability for the publish-time sync decision; an atomic so
	// concurrent writers racing the reader of this counter stay
	// race-clean. Trivial against an fsync.
	dirSyncs atomic.Int64
	// dirReads counts the directories listings have read. Test
	// observability for the bounded list walk.
	dirReads atomic.Int64
	// syncFault, when set by a test, fails the directory sync of dir with
	// its result.
	syncFault func(dir string) error
}

var _ objstore.Backend = (*Backend)(nil)

// New returns the file backend for bucket, a directory under root.
func New(root, bucket string) *Backend {
	return &Backend{root: filepath.Join(filepath.FromSlash(root), bucket)}
}

// ID is the bucket directory as a file:// URL.
func (b *Backend) ID() string { return "file://" + filepath.ToSlash(b.root) }

// Open returns a Store over New(root, bucket) with cfg's pacing, write
// bound.
func Open(root, bucket string, cfg objstore.Config) *objstore.Store {
	return objstore.Open(New(root, bucket), cfg)
}

// path maps a key to a file path. Keys become paths — a trust boundary the
// S3 backend never had: empty keys, absolute keys, empty elements ("a//b",
// which filepath.Join would silently collapse onto "a/b"), and traversal
// elements (".." and ".") are rejected, never resolved. Dot-prefixed nested
// elements are valid because namespace names may begin with a dot; the
// backend's own terminal .lock and .tmp-* files remain reserved.
func (f *Backend) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w %q", objstore.ErrInvalidKey, key)
	}
	parts := strings.Split(key, "/")
	for i, el := range parts {
		if el == "" || el == "." || el == ".." || i == len(parts)-1 && reserved(el) {
			return "", fmt.Errorf("%w %q", objstore.ErrInvalidKey, key)
		}
	}
	return filepath.Join(f.root, filepath.FromSlash(key)), nil
}

// writeBackChunk is how much of one object may sit dirty in the page cache
// before writeTemp forces it to the device. On ext4 an fsync commits the
// journal, and in ordered mode that commit first writes the dirty data of
// every file allocated in the transaction: a 1.5 MB log entry's fsync
// waited 1,459 ms p50 behind a background job's 32 MiB objects, 33 ms on an
// idle disk. Written back a chunk at a time, each large writer holds at
// most one chunk ahead of anyone's sync (one log writer beside 6 bulk
// writers on RAID: 3.3-3.9k rows/s before, 10.4k after, the idle rate;
// 1 MiB chunks left it at 9.1-9.9k). Durability is unchanged:
// the fsync below still decides it. Objects come out in ~3.6 MiB extents.
const writeBackChunk = 256 << 10

// writeTemp writes data to a temp file in dst's directory, synced — ready
// to be renamed or linked into place atomically. The bucket root must
// already exist (EnsureBucket is the sole root creator, matching S3); only
// key sub-directories beneath it are created here. An urgent write (a log
// commit) is written whole; others over writeBackChunk are written back as
// they are written (see writeBackChunk).
func (f *Backend) writeTemp(dst string, data []byte, urgent bool, t *objstore.Timings) (string, error) {
	if _, err := os.Stat(f.root); errors.Is(err, iofs.ErrNotExist) {
		return "", fmt.Errorf("store: bucket %s: %w", f.root, objstore.ErrNotFound)
	} else if err != nil {
		return "", err
	}
	parent := filepath.Dir(dst)
	start := time.Now()
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(parent, ".tmp-*")
	t.Since(objstore.CallCreate, start)
	if err != nil {
		return "", err
	}
	chunk := writeBackChunk
	if urgent || len(data) <= chunk {
		chunk = len(data) // one write, left to the fsync
	}
	for off := 0; off < len(data); off += chunk {
		part := data[off:min(off+chunk, len(data))]
		start = time.Now()
		_, err := tmp.Write(part)
		t.Since(objstore.CallWrite, start)
		if err == nil && chunk < len(data) {
			start = time.Now()
			err = writeBack(tmp, int64(off), int64(len(part)))
			t.Since(objstore.CallWriteBack, start)
		}
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return "", err
		}
	}
	start = time.Now()
	err = tmp.Sync()
	t.Since(objstore.CallFsync, start)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// reserved reports whether a file name is the backend's own: the root lock,
// the lock stripes and temp files.
func reserved(name string) bool { return name == ".lock" || strings.HasPrefix(name, ".tmp-") }

// syncDir fsyncs one directory: the durable-publication primitive. A synced
// file in an unsynced directory is not durable.
func (f *Backend) syncDir(dir string) error {
	if f.syncFault != nil {
		if err := f.syncFault(dir); err != nil {
			f.dirSyncs.Add(1)
			return err
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	cerr := d.Close()
	f.dirSyncs.Add(1)
	if err != nil {
		return err
	}
	return cerr
}

// syncPublishedDir fsyncs dst's parent after a rename or link has published
// into it. If this process has already walked dst's chain to the bucket
// root, the parent alone is owed — it is the only directory whose entry the
// publish changed. Otherwise the full walk (syncDirs) runs, and a completed
// walk — never a failed one — marks the chain durable. The invariant: an
// acknowledged publish's durability is decided by this process's own syncs
// only. Two processes racing one fresh directory each run their own walk,
// so neither's acknowledgement depends on the other's later fsync; within
// one process the set is written only after a walk succeeds, so a failed
// sync makes the next publish walk again. Cost: one chain walk per
// directory per process lifetime. A failed parent-only sync unmarks the
// parent, so a retry that finds its own object (PutIfAbsent's EEXIST) still
// owes, and pays, the sync.
func (f *Backend) syncPublishedDir(dst string, t *objstore.Timings) error {
	defer t.Since(objstore.CallDirSync, time.Now())
	parent := filepath.Dir(dst)
	if _, walked := f.durable.Load(parent); walked {
		err := f.syncDir(parent)
		if err != nil {
			f.durable.Delete(parent)
		}
		return err
	}
	if err := f.syncDirs(dst); err != nil {
		return err
	}
	for dir := parent; ; dir = filepath.Dir(dir) {
		f.durable.Store(dir, struct{}{})
		if dir == f.root {
			return nil
		}
	}
}

// syncDirs fsyncs dst's parent directory and every ancestor up to and
// including f.root, so a just-published rename or link survives a crash —
// a synced file in an unsynced directory is not durable.
func (f *Backend) syncDirs(dst string) error {
	for dir := filepath.Dir(dst); ; dir = filepath.Dir(dir) {
		if err := f.syncDir(dir); err != nil {
			return err
		}
		if dir == f.root {
			return nil
		}
	}
}

// lockKey serializes mutations of the same key across processes. The shared
// root lock also excludes older binaries which take an exclusive root lock.
// Stripes bound lock-file count; they are reserved internal files and must
// never be unlinked while the bucket is live (waiters hold their inodes).
// Keys IsolatedKeys selects have stripes of their own.
func (f *Backend) lockKey(ctx context.Context, key string) (func(), error) {
	rootUnlock, err := flock(ctx, filepath.Join(f.root, ".lock"), syscall.LOCK_SH)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(key))
	stripe := fmt.Sprintf(".tmp-lock-%02x", sum[0])
	if f.IsolatedKeys != nil && f.IsolatedKeys(key) {
		stripe = fmt.Sprintf(".tmp-lock-iso-%02x", sum[0])
	}
	unlock, err := flock(ctx, filepath.Join(f.root, stripe), syscall.LOCK_EX)
	if err != nil {
		rootUnlock()
		return nil, err
	}
	return func() { unlock(); rootUnlock() }, nil
}

// flock dies with the process — no stale-lock recovery. Non-blocking with a
// short poll: a blocking flock pins an OS thread and ignores ctx.
func flock(ctx context.Context, path string, mode int) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	var retry *time.Timer
	for {
		err := syscall.Flock(int(lock.Fd()), mode|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			lock.Close()
			return nil, err
		}
		if retry == nil {
			retry = time.NewTimer(time.Millisecond)
			defer retry.Stop()
		} else {
			retry.Reset(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			lock.Close()
			return nil, ctx.Err()
		case <-retry.C:
		}
	}
	if err := ctx.Err(); err != nil {
		// cancellation raced the acquire: never hand out a lock ctx has abandoned
		syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		lock.Close()
		return nil, err
	}
	return func() {
		syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		lock.Close()
	}, nil
}

// Put publishes data at key atomically (temp file, fsync, rename, directory
// fsync).
func (f *Backend) Put(ctx context.Context, key string, data []byte) error {
	return objstore.OpErr("put", key, f.put(ctx, key, data))
}

func (f *Backend) put(ctx context.Context, key string, data []byte) error {
	dst, err := f.path(key)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := f.writeTemp(dst, data, objstore.IsUrgent(ctx), objstore.TimingsOf(ctx))
	if err != nil {
		return err
	}
	t := objstore.TimingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.Since(objstore.CallLock, start)
	if err != nil {
		os.Remove(tmp)
		return err
	}
	defer unlock()
	start = time.Now()
	err = os.Rename(tmp, dst)
	t.Since(objstore.CallLink, start)
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return f.syncPublishedDir(dst, t)
}

// PutIfAbsent publishes data at key only if no object is there (link(2)
// fails with EEXIST). A failed directory sync after the link is (true,
// err): the object is visible but not proven durable. A retry then finds
// it (false) and syncs the directory itself, so (false, nil) always means
// the object present is durable as far as this process can tell.
func (f *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := f.putIfAbsent(ctx, key, data)
	return ok, objstore.OpErr("put-if-absent", key, err)
}

func (f *Backend) putIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	dst, err := f.path(key)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tmp, err := f.writeTemp(dst, data, objstore.IsUrgent(ctx), objstore.TimingsOf(ctx))
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	t := objstore.TimingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.Since(objstore.CallLock, start)
	if err != nil {
		return false, err
	}
	defer unlock()
	// link(2) fails with EEXIST atomically AND publishes complete content —
	// unlike O_EXCL+write, a crash can never leave a half-written winner.
	start = time.Now()
	err = os.Link(tmp, dst)
	t.Since(objstore.CallLink, start)
	if err != nil {
		if !errors.Is(err, iofs.ErrExist) {
			return false, err
		}
		// The object may be this process's own from a try whose directory
		// sync failed: owe the sync until this process has made it.
		if _, walked := f.durable.Load(filepath.Dir(dst)); !walked {
			if err := f.syncPublishedDir(dst, t); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	return true, f.syncPublishedDir(dst, t)
}

// Get reads the whole object at key.
func (f *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := f.get(ctx, key)
	return data, objstore.OpErr("get", key, err)
}

func (f *Backend) get(ctx context.Context, key string) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()
	data, err := os.ReadFile(p)
	objstore.TimingsOf(ctx).Since(objstore.CallRead, start)
	if errors.Is(err, iofs.ErrNotExist) {
		return nil, objstore.ErrNotFound
	}
	return data, err
}

// GetWithETag reads the object at key and its ETag, the hex SHA-256 of its
// content.
func (f *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, err := f.get(ctx, key)
	if err != nil {
		return nil, "", objstore.OpErr("get-with-etag", key, err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

// GetIfChanged reads the object at key unless its ETag is etag. The file
// is read either way: the ETag is its content hash.
func (f *Backend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	data, current, err := f.GetWithETag(ctx, key)
	if err != nil {
		return nil, "", false, err
	}
	if etag != "" && current == etag {
		return nil, current, true, nil
	}
	return data, current, false, nil
}

// PutIfMatch replaces the object at key only if its ETag is etag. The new
// content is written and synced before the key's lock is taken, so the lock
// covers only the compare, the rename and the directory sync; the cost is
// that a losing writer still pays its temp file's write and fsync. See
// PutIfAbsent for (true, err).
func (f *Backend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	ok, err := f.putIfMatch(ctx, key, data, etag)
	return ok, objstore.OpErr("put-if-match", key, err)
}

func (f *Backend) putIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	dst, err := f.path(key)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	t := objstore.TimingsOf(ctx)
	tmp, err := f.writeTemp(dst, data, objstore.IsUrgent(ctx), t)
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.Since(objstore.CallLock, start)
	if err != nil {
		return false, err
	}
	defer unlock()
	start = time.Now()
	cur, err := os.ReadFile(dst)
	t.Since(objstore.CallRead, start)
	if errors.Is(err, iofs.ErrNotExist) {
		return false, nil // precondition cannot hold
	}
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(cur)
	if hex.EncodeToString(sum[:]) != etag {
		return false, nil
	}
	// Rename is the publication point: the last chance to honour ctx. Once it
	// lands, the sync below completes regardless.
	if err := ctx.Err(); err != nil {
		return false, err
	}
	start = time.Now()
	err = os.Rename(tmp, dst)
	t.Since(objstore.CallLink, start)
	if err != nil {
		return false, err
	}
	return true, f.syncPublishedDir(dst, t)
}

// ListPage lists up to limit keys under prefix after after. The walk starts
// at prefix's directory, visits entries in key order, skips subtrees that
// cannot hold a key under prefix and after after, and stops once it has
// one key past the page: a page costs the directories it touches, not the
// bucket.
func (f *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	keys, err := f.list(ctx, prefix, limit, func(key string, isDir bool, out []string) ([]string, bool) {
		if isDir {
			// A subtree's keys all start with key: some may follow after
			// only if after is inside it or it sorts past after.
			return out, strings.HasPrefix(after, key) || key > after
		}
		if key > after {
			out = append(out, key)
		}
		return out, false
	})
	if err != nil {
		return nil, "", objstore.OpErr("list-page", prefix, err)
	}
	return page(keys, limit)
}

// ListPrefixesPage lists up to limit child prefixes under prefix after
// after. A prefix counts only if an object lives under it, so a directory
// left empty by a Delete is invisible here exactly as it is on S3, which
// has no directories at all.
func (f *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	keys, err := f.list(ctx, prefix, limit, func(key string, isDir bool, out []string) ([]string, bool) {
		if !isDir || !strings.HasPrefix(key, prefix) || len(key) == len(prefix) {
			return out, isDir // an object at the prefix level, or prefix's own directory
		}
		// key is one level under prefix: a child prefix, never descended.
		if key > after && f.hasObject(ctx, key) {
			out = append(out, key)
		}
		return out, false
	})
	if err != nil {
		return nil, "", objstore.OpErr("list-prefixes-page", prefix, err)
	}
	return page(keys, limit)
}

func page(keys []string, limit int) ([]string, string, error) {
	if len(keys) > limit {
		keys = keys[:limit]
		return keys, keys[limit-1], nil
	}
	return keys, "", nil
}

// list collects up to limit+1 names under prefix, in key order. take sees
// each object key under prefix, and each directory (as "dir/") that is an
// ancestor of prefix or lies under it, and returns out with any additions
// and, for a directory, whether to descend into it.
func (f *Backend) list(ctx context.Context, prefix string, limit int, take func(key string, isDir bool, out []string) ([]string, bool)) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("limit %d < 1", limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer objstore.TimingsOf(ctx).Since(objstore.CallList, time.Now())
	start := prefix[:strings.LastIndex(prefix, "/")+1]
	if start != "" {
		for _, el := range strings.Split(start[:len(start)-1], "/") {
			if el == "" || el == "." || el == ".." {
				return nil, nil // no valid key lies under it
			}
		}
	}
	var out []string
	_, err := f.walk(ctx, start, func(key string, isDir bool) (descend, stop bool) {
		if !strings.HasPrefix(key, prefix) && !(isDir && strings.HasPrefix(prefix, key)) {
			// Past prefix in key order means past every key under it.
			return false, key > prefix
		}
		out, descend = take(key, isDir, out)
		return descend, len(out) > limit
	})
	if err == nil || start == "" || !(errors.Is(err, iofs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)) {
		return out, f.bucketErr(err)
	}
	// prefix's directory is missing: an empty listing, unless the bucket is.
	if _, err := os.Stat(f.root); err != nil {
		return nil, f.bucketErr(err)
	}
	return nil, nil
}

// bucketErr maps a missing bucket root to ErrNotFound, matching S3's
// NoSuchBucket.
func (f *Backend) bucketErr(err error) error {
	if errors.Is(err, iofs.ErrNotExist) {
		return fmt.Errorf("store: bucket %s: %w", f.root, objstore.ErrNotFound)
	}
	return err
}

// hasObject reports whether any object lives under dir ("a/b/").
func (f *Backend) hasObject(ctx context.Context, dir string) bool {
	found, _ := f.walk(ctx, dir, func(_ string, isDir bool) (bool, bool) { return isDir, !isDir })
	return found
}

// walk visits the entries under dir (a key prefix ending in "/", or "" for
// the bucket root) in key order: a directory d sorts as "d/", where every
// key under it sorts, so a depth-first walk with each directory's entries
// sorted that way yields keys in exactly the order S3 lists them. visit
// sees each object key and each directory; for a directory it returns
// whether to descend. walk returns true as soon as visit says stop.
// Directories vanishing mid-walk are skipped; the reserved lock and temp
// files are not objects.
func (f *Backend) walk(ctx context.Context, dir string, visit func(key string, isDir bool) (descend, stop bool)) (bool, error) {
	f.dirReads.Add(1)
	entries, err := os.ReadDir(filepath.Join(f.root, filepath.FromSlash(dir)))
	if err != nil {
		return false, err
	}
	type entry struct {
		key   string
		isDir bool
	}
	list := make([]entry, 0, len(entries))
	for _, e := range entries {
		switch {
		case e.IsDir():
			list = append(list, entry{dir + e.Name() + "/", true})
		case !reserved(e.Name()):
			list = append(list, entry{dir + e.Name(), false})
		}
	}
	slices.SortFunc(list, func(a, b entry) int { return strings.Compare(a.key, b.key) })
	for _, e := range list {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		descend, stop := visit(e.key, e.isDir)
		if stop {
			return true, nil
		}
		if !e.isDir || !descend {
			continue
		}
		stop, err := f.walk(ctx, e.key, visit)
		if errors.Is(err, iofs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			continue // vanished or replaced mid-walk
		}
		if err != nil || stop {
			return stop, err
		}
	}
	return false, nil
}

// Delete removes the object at key. A missing key is not an error.
func (f *Backend) Delete(ctx context.Context, key string) error {
	return objstore.OpErr("delete", key, f.delete(ctx, key))
}

func (f *Backend) delete(ctx context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	t := objstore.TimingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.Since(objstore.CallLock, start)
	if err != nil {
		return err
	}
	defer unlock()
	defer t.Since(objstore.CallDelete, time.Now())
	if err := os.Remove(p); err != nil && !errors.Is(err, iofs.ErrNotExist) {
		return err
	}
	return nil
}

// DeleteMany removes keys one by one. Missing keys are not an error.
func (f *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := f.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// EnsureBucket creates the bucket directory if missing. It is the only
// creator of the root; writes into a missing bucket fail with ErrNotFound.
func (f *Backend) EnsureBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr("create-bucket", f.root, err)
	}
	return objstore.OpErr("create-bucket", f.root, os.MkdirAll(f.root, 0o755))
}

// DropBucket removes the bucket directory and everything in it. A missing
// bucket is not an error.
func (f *Backend) DropBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr("drop-bucket", f.root, err)
	}
	err := objstore.OpErr("drop-bucket", f.root, os.RemoveAll(f.root))
	// The tree is gone: nothing in it is durable any more, and a stale mark
	// would let a later publish into a recreated bucket skip its walk.
	f.durable.Range(func(k, _ any) bool {
		f.durable.Delete(k)
		return true
	})
	return err
}

// GetRange reads exactly length bytes of key at offset; a range past the
// end is ErrRange.
func (f *Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	defer objstore.TimingsOf(ctx).Since(objstore.CallRead, time.Now())
	file, err := os.Open(p)
	if errors.Is(err, iofs.ErrNotExist) {
		return nil, objstore.OpErr("get-range", key, objstore.ErrNotFound)
	}
	if err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	if offset > info.Size() || length > info.Size()-offset {
		return nil, objstore.OpErr("get-range", key, objstore.ErrRange)
	}
	// One exact-size buffer: ReadAll would grow a fresh buffer for every
	// range read, which drains measurable allocation on a hot merge path.
	// A short read was ErrUnexpectedEOF under ReadAll's nil-on-EOF contract;
	// ReadFull reports it directly, and a zero-byte read stays nil.
	data := make([]byte, length)
	if _, err := io.ReadFull(io.NewSectionReader(file, offset, length), data); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, objstore.OpErr("get-range", key, err)
	}
	return data, nil
}
