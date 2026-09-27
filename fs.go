package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// fsStore is the file:// backend: write-once objects as files, O_EXCL-style
// links for PutIfAbsent, temp+rename for atomic Put, striped flocks plus a
// content hash for PutIfMatch. Dev and hermetic-CI backend; unix-only
// (flock). Same single-writer-per-bucket contract as S3.
type fsStore struct {
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
}

func newFS(root, bucket string) *fsStore {
	return &fsStore{root: filepath.Join(filepath.FromSlash(root), bucket)}
}

// path maps a key to a file path. Keys become paths — a trust boundary the
// S3 backend never had: empty keys, absolute keys, empty elements ("a//b",
// which filepath.Join would silently collapse onto "a/b"), and traversal
// elements (".." and ".") are rejected, never resolved. Dot-prefixed nested
// elements are valid because namespace names may begin with a dot; the
// backend's own terminal .lock and .tmp-* files remain reserved.
func (f *fsStore) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	parts := strings.Split(key, "/")
	for i, el := range parts {
		if el == "" || el == "." || el == ".." || i == len(parts)-1 && (el == ".lock" || strings.HasPrefix(el, ".tmp-")) {
			return "", fmt.Errorf("store: invalid key %q", key)
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
func (f *fsStore) writeTemp(dst string, data []byte, urgent bool, t *Timings) (string, error) {
	if _, err := os.Stat(f.root); errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("store: bucket %s: %w", f.root, ErrNotFound)
	} else if err != nil {
		return "", err
	}
	parent := filepath.Dir(dst)
	start := time.Now()
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(parent, ".tmp-*")
	t.since(CallCreate, start)
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
		t.since(CallWrite, start)
		if err == nil && chunk < len(data) {
			start = time.Now()
			err = writeBack(tmp, int64(off), int64(len(part)))
			t.since(CallWriteBack, start)
		}
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return "", err
		}
	}
	start = time.Now()
	err = tmp.Sync()
	t.since(CallFsync, start)
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

// syncDir fsyncs one directory: the durable-publication primitive. A synced
// file in an unsynced directory is not durable.
func (f *fsStore) syncDir(dir string) error {
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
// directory per process lifetime.
func (f *fsStore) syncPublishedDir(dst string, t *Timings) error {
	defer t.since(CallDirSync, time.Now())
	parent := filepath.Dir(dst)
	if _, walked := f.durable.Load(parent); walked {
		return f.syncDir(parent)
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
func (f *fsStore) syncDirs(dst string) error {
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
// Keys containing "/wal/" (a write-ahead log) have stripes of their own: a
// write holds its stripe through the directory fsync, and a log commit whose
// key hashed to a bulk write's stripe (1 in 256) waited for that fsync. The class is the key's, never the
// caller's, so one key always takes one stripe.
func (f *fsStore) lockKey(ctx context.Context, key string) (func(), error) {
	rootUnlock, err := flock(ctx, filepath.Join(f.root, ".lock"), syscall.LOCK_SH)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(key))
	stripe := fmt.Sprintf(".tmp-lock-%02x", sum[0])
	if strings.Contains(key, "/wal/") {
		stripe = fmt.Sprintf(".tmp-lock-wal-%02x", sum[0])
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

func (f *fsStore) Put(ctx context.Context, key string, data []byte) error {
	return opErr("put", key, f.put(ctx, key, data))
}

func (f *fsStore) put(ctx context.Context, key string, data []byte) error {
	dst, err := f.path(key)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := f.writeTemp(dst, data, isUrgent(ctx), timingsOf(ctx))
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // a no-op after Rename; the leak otherwise
	t := timingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.since(CallLock, start)
	if err != nil {
		return err
	}
	defer unlock()
	start = time.Now()
	err = os.Rename(tmp, dst)
	t.since(CallLink, start)
	if err != nil {
		return err
	}
	return f.syncPublishedDir(dst, t)
}

func (f *fsStore) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := f.putIfAbsent(ctx, key, data)
	return ok, opErr("put-if-absent", key, err)
}

func (f *fsStore) putIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	dst, err := f.path(key)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tmp, err := f.writeTemp(dst, data, isUrgent(ctx), timingsOf(ctx))
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	t := timingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.since(CallLock, start)
	if err != nil {
		return false, err
	}
	defer unlock()
	// link(2) fails with EEXIST atomically AND publishes complete content —
	// unlike O_EXCL+write, a crash can never leave a half-written winner.
	start = time.Now()
	err = os.Link(tmp, dst)
	t.since(CallLink, start)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, f.syncPublishedDir(dst, t)
}

func (f *fsStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := f.get(ctx, key)
	return data, opErr("get", key, err)
}

func (f *fsStore) get(ctx context.Context, key string) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()
	data, err := os.ReadFile(p)
	timingsOf(ctx).since(CallRead, start)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

func (f *fsStore) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, err := f.get(ctx, key)
	if err != nil {
		return nil, "", opErr("get-with-etag", key, err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func (f *fsStore) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	data, current, err := f.GetWithETag(ctx, key)
	if err != nil {
		return nil, "", false, err
	}
	if etag != "" && current == etag {
		return nil, current, true, nil
	}
	return data, current, false, nil
}

func (f *fsStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	ok, err := f.putIfMatch(ctx, key, data, etag)
	return ok, opErr("put-if-match", key, err)
}

func (f *fsStore) putIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	dst, err := f.path(key)
	if err != nil {
		return false, err
	}
	t := timingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.since(CallLock, start)
	if err != nil {
		return false, err
	}
	defer unlock()
	start = time.Now()
	cur, err := os.ReadFile(dst)
	t.since(CallRead, start)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil // precondition cannot hold
	}
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(cur)
	if hex.EncodeToString(sum[:]) != etag {
		return false, nil
	}
	tmp, err := f.writeTemp(dst, data, isUrgent(ctx), timingsOf(ctx))
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	// Rename is the publication point: the last chance to honour ctx. Once it
	// lands, the sync below completes regardless.
	if err := ctx.Err(); err != nil {
		return false, err
	}
	start = time.Now()
	err = os.Rename(tmp, dst)
	t.since(CallLink, start)
	if err != nil {
		return false, err
	}
	return true, f.syncPublishedDir(dst, t)
}

func (f *fsStore) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	keys, err := f.list(ctx, prefix)
	if err != nil {
		return nil, "", opErr("list-page", prefix, err)
	}
	return pageStrings(keys, after, limit)
}

func (f *fsStore) list(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer timingsOf(ctx).since(CallList, time.Now())
	var keys []string
	err := filepath.WalkDir(f.root, func(p string, d fs.DirEntry, err error) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if err != nil {
			if p == f.root && errors.Is(err, fs.ErrNotExist) {
				// missing bucket root is an error, matching S3's NoSuchBucket
				return fmt.Errorf("store: bucket %s: %w", f.root, ErrNotFound)
			}
			if errors.Is(err, fs.ErrNotExist) {
				return nil // entry vanished mid-walk
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == ".lock" || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil // internal files are not objects
		}
		rel, err := filepath.Rel(f.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

// ListPrefixesPage derives the common prefixes from the object walk, so a
// directory left empty by a Delete is invisible here exactly as it is on S3,
// which has no directories at all.
func (f *fsStore) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	keys, err := f.list(ctx, prefix)
	if err != nil {
		return nil, "", opErr("list-prefixes-page", prefix, err)
	}
	var out []string
	for _, k := range keys {
		i := strings.Index(k[len(prefix):], "/")
		if i < 0 {
			continue // an object AT the prefix level, not under a child prefix
		}
		// list() is sorted, so equal prefixes are adjacent.
		if p := k[:len(prefix)+i+1]; len(out) == 0 || out[len(out)-1] != p {
			out = append(out, p)
		}
	}
	return pageStrings(out, after, limit)
}

func pageStrings(all []string, after string, limit int) ([]string, string, error) {
	start := sort.SearchStrings(all, after)
	for start < len(all) && all[start] <= after {
		start++
	}
	end := min(start+limit, len(all))
	page := all[start:end]
	if end < len(all) {
		return page, page[len(page)-1], nil
	}
	return page, "", nil
}

func (f *fsStore) Delete(ctx context.Context, key string) error {
	return opErr("delete", key, f.delete(ctx, key))
}

func (f *fsStore) delete(ctx context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	t := timingsOf(ctx)
	start := time.Now()
	unlock, err := f.lockKey(ctx, key)
	t.since(CallLock, start)
	if err != nil {
		return err
	}
	defer unlock()
	defer t.since(CallDelete, time.Now())
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (f *fsStore) DeleteMany(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := f.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (f *fsStore) EnsureBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return opErr("create-bucket", f.root, err)
	}
	return opErr("create-bucket", f.root, os.MkdirAll(f.root, 0o755))
}

func (f *fsStore) DropBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return opErr("drop-bucket", f.root, err)
	}
	err := opErr("drop-bucket", f.root, os.RemoveAll(f.root))
	// The tree is gone: nothing in it is durable any more, and a stale mark
	// would let a later publish into a recreated bucket skip its walk.
	f.durable.Range(func(k, _ any) bool {
		f.durable.Delete(k)
		return true
	})
	return err
}

func (f *fsStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	defer timingsOf(ctx).since(CallRead, time.Now())
	file, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, opErr("get-range", key, ErrNotFound)
	}
	if err != nil {
		return nil, opErr("get-range", key, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if offset > info.Size() || length > info.Size()-offset {
		return nil, opErr("get-range", key, ErrRange)
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
		return nil, opErr("get-range", key, err)
	}
	return data, nil
}
