package cache

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Disk owns a private, disposable directory. It caches only immutable
// objects, never log frontiers, leases or other mutable heads.
// Losing this directory is a cache miss. A checksum prevents damaged cache
// bytes from masquerading as durable object corruption.
//
// Keys under ns/<name>/ belong to namespace <name>: pins, ExpireInactive
// and Touch work per namespace. A nil *Disk is a disk tier that holds
// nothing; every method is safe on it.
type Disk struct {
	// Logger receives failures the cache survives but an operator should
	// see (a temporary file it could not remove). Nil discards them. Set it
	// before first use.
	Logger *slog.Logger

	// Pin transactions include reservation and warming. Serializing them keeps
	// rollback from losing capacity to a concurrent replacement reservation.
	// ponytail: one pin transaction per worker; shard this lock if pin churn matters.
	pinMu                             sync.Mutex
	mu                                sync.Mutex
	dir                               string
	cap, size                         int64
	reserved                          int64 // charges of Puts writing their temporary file
	ll                                list.List
	items                             map[string]*list.Element
	closed                            bool
	pinCap, pinnedBytes               int64
	maxPinned                         int                         // distinct pinned names (MaxPinnedNamespaces)
	pins                              map[string]map[string]int64 // name → disk key → charge
	pinBytes                          map[string]int64
	pinAt                             map[string]time.Time // name → when the reservation was (re)made
	pinPrevAt                         map[string]time.Time // name → pinAt of the reservation Pin replaced (RestorePin)
	pinned                            map[string]int       // disk key → number of names pinning it
	pinnedResident                    int64                // charge of resident entries that are pinned
	lastAccess                        map[string]time.Time // namespace → last Get hit or Put of one of its objects
	hits, misses, evictions, failures uint64
	inactiveExpiries                  uint64 // namespaces ExpireInactive evicted
}

// ErrPinCapacity is Pin's refusal: the reservation does not fit the pin
// capacity, or the name would exceed the pinned-namespace count cap.
var ErrPinCapacity = errors.New("cache: pin capacity exceeded")

// MaxPinnedNamespaces is the default bound on distinct pinned names
// (SetMaxPinnedNamespaces). Pin refuses the 257th distinct name with
// ErrPinCapacity; re-pinning a pinned name does not count twice.
const MaxPinnedNamespaces = 256

// ErrWarmCache is PutChecked's refusal: the object is not resident, because
// it does not fit, the cache is closed or writing it failed.
var ErrWarmCache = errors.New("cache: pinned cache object is not resident")

type diskEntry struct {
	key, path     string
	bytes, charge int64
	sums          []uint32 // CRC32C of each diskBlock, so a range verifies alone
}

// DiskStats is a snapshot of a Disk's occupancy and counters.
type DiskStats struct {
	CapacityBytes                     int64
	UsedBytes                         int64
	PinCapacityBytes, PinnedBytes     int64
	PinnedNamespaces                  int
	Entries                           int
	Hits, Misses, Evictions, Failures uint64
	InactiveExpiries                  uint64
}

// dirPrefix names every directory a Disk creates under its root: its cache
// directory and the trash a Wipe or Close renames it to.
const dirPrefix = "objstore-cache-"

// NewDisk creates a disk tier of capacity bytes in a new directory under
// root (the system temporary directory when root is ""). pinCapacity, if
// given, bounds pin reservations (default: capacity). A capacity of zero
// or less returns a nil *Disk, which caches nothing.
//
// A non-empty root belongs to one process: NewDisk removes the directories
// an earlier Disk left under it (a crash skips Close), since nothing on
// them is ever recovered.
func NewDisk(root string, capacity int64, pinCapacity ...int64) (*Disk, error) {
	if capacity <= 0 {
		return nil, nil
	}
	pc := capacity
	if len(pinCapacity) > 0 {
		pc = pinCapacity[0]
	}
	if pc < 0 || pc > capacity {
		return nil, ErrPinCapacity
	}
	if root != "" {
		if err := os.MkdirAll(root, 0700); err != nil {
			return nil, err
		}
		stale, err := filepath.Glob(filepath.Join(root, dirPrefix+"*"))
		if err != nil {
			return nil, err
		}
		for _, dir := range stale {
			if err := os.RemoveAll(dir); err != nil {
				return nil, fmt.Errorf("cache: remove stale %s: %w", dir, err)
			}
		}
	}
	dir, err := os.MkdirTemp(root, dirPrefix)
	if err != nil {
		return nil, err
	}
	return &Disk{dir: dir, cap: capacity, pinCap: pc, maxPinned: MaxPinnedNamespaces, items: make(map[string]*list.Element), pins: make(map[string]map[string]int64), pinBytes: make(map[string]int64), pinAt: make(map[string]time.Time), pinPrevAt: make(map[string]time.Time), pinned: make(map[string]int), lastAccess: make(map[string]time.Time)}, nil
}

// diskBlock is the unit a cached file is checksummed and read in. An
// object may be tens of megabytes and a request want 8 KiB of it: GetRange
// reads and verifies the covering 4 KiB blocks, not the file.
const diskBlock = 4 << 10

var diskCRC = crc32.MakeTable(crc32.Castagnoli)

func blockSums(b []byte) []uint32 {
	sums := make([]uint32, 0, (len(b)+diskBlock-1)/diskBlock)
	for off := 0; off < len(b); off += diskBlock {
		sums = append(sums, crc32.Checksum(b[off:min(off+diskBlock, len(b))], diskCRC))
	}
	return sums
}

// Get returns key's cached object, verified against its block checksums.
// A damaged or vanished file is a miss that drops the entry.
func (c *Disk) Get(key string) ([]byte, bool) { return c.read(key, 0, -1) }

// GetRange returns bytes [off, off+n) of key's cached object, reading and
// verifying only the blocks that cover them. A range outside the object is
// a miss that leaves the entry alone.
func (c *Disk) GetRange(key string, off, n int64) ([]byte, bool) {
	if off < 0 || n < 0 {
		return nil, false
	}
	return c.read(key, off, n)
}

// read serves Get (n < 0: the whole object) and GetRange.
func (c *Disk) read(key string, off, n int64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	el := c.items[key]
	if c.closed || el == nil {
		if el == nil {
			c.misses++
		}
		c.mu.Unlock()
		return nil, false
	}
	e := *el.Value.(*diskEntry)
	c.mu.Unlock()
	if n < 0 {
		n = e.bytes
	}
	if off > e.bytes || n > e.bytes-off {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil, false
	}
	// Read and verify outside the lock: one request may ask for ~1,000
	// ranges, and holding the mutex across open, read and hash serialized
	// every disk hit in the process. An entry evicted meanwhile has lost its
	// file (a miss); one replaced meanwhile fails the checksum (a miss).
	b, ok := readBlocks(e, off, n)
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.items[key] == el
	if !ok {
		c.misses++
		if current {
			c.failures++
			c.remove(el)
		}
		return nil, false
	}
	c.hits++
	if current {
		c.ll.MoveToFront(el)
	}
	c.touch(key)
	return b, true
}

// Has reports whether the key is in the index, WITHOUT reading the file or
// touching the hit/miss counters: a presence question asked before deciding
// which blocks one ranged read has to cover. The consumer's own Get owns
// the accounting, exactly as ByteCache.Peek does for the memory tier.
func (c *Disk) Has(key string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.items[key] != nil
}

// DiskCharge is the cache charge for an object of n bytes: whole 4 KiB
// blocks, at least one even for an empty object. This bounds both cached
// file count and metadata, including pin reservations, when objects are tiny.
func DiskCharge(n int) int64 {
	return max(int64(n)+diskBlock-1, diskBlock) / diskBlock * diskBlock
}

// Put caches b under key, best effort: a fill that does not fit or fails
// is dropped. b is not retained.
func (c *Disk) Put(key string, b []byte) {
	_ = c.PutChecked(key, b)
}

// PutChecked is the strict Put, for when residency is a readiness
// condition. Ordinary fills remain best effort through Put.
func (c *Disk) PutChecked(key string, b []byte) error {
	if c == nil {
		return ErrWarmCache
	}
	charge := DiskCharge(len(b))
	c.mu.Lock()
	if c.closed || charge > c.cap {
		c.mu.Unlock()
		return ErrWarmCache
	}
	if c.items[key] != nil {
		c.touch(key)
		c.mu.Unlock()
		return nil
	}
	// Reserve the charge before writing: temporary files count against the
	// capacity, so a warm's 32 concurrent fills of objects up to 32 MB cannot
	// overshoot it on disk, and a fill that cannot fit writes nothing.
	if !c.makeRoom(charge) {
		c.mu.Unlock()
		return ErrWarmCache
	}
	c.reserved += charge
	dir := c.dir
	c.mu.Unlock()
	// Write and hash outside the lock (see Get); the rename below publishes
	// the complete file under it, so readers never see a partial one.
	tmp, sums, werr := writeTemp(dir, b)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reserved -= charge
	discard := func() {
		if tmp == "" {
			return
		}
		if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
			// A partial file we cannot reclaim must never escape accounting:
			// stop caching rather than grow past the capacity.
			c.closed = true
			c.logger().Error("cache: disk tier closed: cannot remove temporary file", "path", tmp, "err", err)
		}
	}
	if werr != nil {
		c.failures++
		discard()
		return errors.Join(ErrWarmCache, werr)
	}
	if c.closed {
		discard()
		return ErrWarmCache
	}
	if c.items[key] != nil { // a concurrent Put of the same immutable bytes won
		discard()
		c.touch(key)
		return nil
	}
	if !c.makeRoom(charge) { // a pin or Wipe changed the room meanwhile
		discard()
		return ErrWarmCache
	}
	id := sha256.Sum256([]byte(key))
	path := filepath.Join(c.dir, hex.EncodeToString(id[:]))
	if err := os.Rename(tmp, path); err != nil {
		c.failures++
		discard()
		return errors.Join(ErrWarmCache, err)
	}
	e := &diskEntry{key: key, path: path, bytes: int64(len(b)), charge: charge, sums: sums}
	c.items[key] = c.ll.PushFront(e)
	c.size += charge
	if c.isPinned(key) {
		c.pinnedResident += charge
	}
	c.touch(key)
	return nil
}

func (c *Disk) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.Logger
}

// detachDir renames the cache directory aside, so the caller can remove it
// after releasing c.mu, and forgets every entry. recreate makes a fresh,
// empty directory in its place. It returns the path to remove, "" if none.
func (c *Disk) detachDir(recreate bool) (string, error) {
	trash, err := os.MkdirTemp(filepath.Dir(c.dir), filepath.Base(c.dir)+".trash-")
	if err != nil {
		return "", err
	}
	if err := os.Rename(c.dir, filepath.Join(trash, "d")); err != nil && !os.IsNotExist(err) {
		os.Remove(trash)
		return "", err
	}
	clear(c.items)
	c.ll.Init()
	c.size, c.pinnedResident = 0, 0
	if recreate {
		if err := os.MkdirAll(c.dir, 0700); err != nil {
			return trash, err
		}
	}
	return trash, nil
}

// makeRoom evicts unpinned entries, least recent first, until charge more
// bytes fit beside the entries and the in-flight reservations. It evicts
// nothing when even evicting every unpinned entry could not make room.
func (c *Disk) makeRoom(charge int64) bool {
	if c.cap-c.pinnedResident-c.reserved < charge {
		return false
	}
	el := c.ll.Back()
	for c.size+c.reserved > c.cap-charge {
		for el != nil && c.isPinned(el.Value.(*diskEntry).key) {
			el = el.Prev()
		}
		if el == nil {
			return false
		}
		prev := el.Prev()
		if !c.remove(el) {
			return false
		}
		el = prev
	}
	return true
}

// readBlocks reads [off, off+n) of e's file through its covering blocks and
// verifies each against the checksum recorded when the file was written.
func readBlocks(e diskEntry, off, n int64) ([]byte, bool) {
	lo := off / diskBlock * diskBlock
	hi := min((off+n+diskBlock-1)/diskBlock*diskBlock, e.bytes)
	f, err := os.Open(e.path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	buf := make([]byte, hi-lo)
	if _, err := f.ReadAt(buf, lo); err != nil && !(err == io.EOF && len(buf) == 0) {
		return nil, false
	}
	for at := lo; at < hi; at += diskBlock {
		if crc32.Checksum(buf[at-lo:min(at+diskBlock, hi)-lo], diskCRC) != e.sums[at/diskBlock] {
			return nil, false
		}
	}
	if lo == off && hi-lo == n {
		return buf, true
	}
	return bytes.Clone(buf[off-lo : off-lo+n]), true
}

// writeTemp writes b to a new file in dir and returns its path and block sums.
// This cache is intentionally not recovered after restart, so fsync is
// unnecessary. On error the path, if any, names the partial file.
func writeTemp(dir string, b []byte) (string, []uint32, error) {
	f, err := os.CreateTemp(dir, "put-")
	if err != nil {
		return "", nil, err
	}
	_, err = f.Write(b)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return f.Name(), blockSums(b), err
}

// keyNamespace is the namespace whose ns/<name>/ prefix an object key
// carries, after DiskKey's optional "<generation>:" prefix; "" for keys
// outside ns/.
func keyNamespace(key string) string {
	if i := strings.IndexByte(key, ':'); i >= 0 && !strings.HasPrefix(key, "ns/") {
		key = key[i+1:]
	}
	rest, ok := strings.CutPrefix(key, "ns/")
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(rest, "/")
	return name
}

func (c *Disk) touch(key string) {
	if name := keyNamespace(key); name != "" {
		c.lastAccess[name] = time.Now()
	}
}

// Touch restarts name's inactivity clock. Query admission calls it, since a
// query answered from the memory tier never reaches this cache.
func (c *Disk) Touch(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.lastAccess[name]; ok {
		c.lastAccess[name] = time.Now()
	}
}

// ExpireInactive evicts the cached objects of every namespace whose objects
// nobody read or cached for ttl, and returns those names: LRU alone would
// keep an idle namespace until capacity pressure. Keys held by any pin
// reservation stay, for as long as the namespace stays pinned.
func (c *Disk) ExpireInactive(ttl time.Duration, now time.Time) []string {
	if c == nil || ttl <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	idle := map[string]bool{}
	for name, at := range c.lastAccess {
		if _, pinned := c.pins[name]; !pinned && now.Sub(at) >= ttl {
			idle[name] = true
		}
	}
	if len(idle) == 0 {
		return nil
	}
	for key, el := range c.items {
		if idle[keyNamespace(key)] && !c.isPinned(key) {
			c.remove(el)
		}
	}
	names := make([]string, 0, len(idle))
	for name := range idle {
		delete(c.lastAccess, name)
		names = append(names, name)
	}
	c.inactiveExpiries += uint64(len(names))
	slices.Sort(names)
	return names
}

func (c *Disk) isPinned(key string) bool { return c.pinned[key] > 0 }

// pinKeys adds delta (±1) to the pin count of every key, keeping
// pinnedResident equal to the charge of the resident keys pinned at all.
func (c *Disk) pinKeys(keys map[string]int64, delta int) {
	for key := range keys {
		before := c.pinned[key]
		after := before + delta
		if after <= 0 {
			delete(c.pinned, key)
		} else {
			c.pinned[key] = after
		}
		if (before > 0) == (after > 0) {
			continue
		}
		if el := c.items[key]; el != nil {
			if after > 0 {
				c.pinnedResident += el.Value.(*diskEntry).charge
			} else {
				c.pinnedResident -= el.Value.(*diskEntry).charge
			}
		}
	}
}

// PinHeadroom is the reservation name may hold without overcommitting pin
// capacity. A caller sizing a set uses it to stop early; Pin decides.
func (c *Disk) PinHeadroom(name string) int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pinCap - c.pinnedBytes + c.pinBytes[name]
}

// Pin reserves every key (a disk key) at its charge, resident or not,
// replacing name's previous reservation. Absent keys are charged too: the
// reservation is a promise of space, not a tally of what happens to be
// cached right now. An empty set (a pinned namespace with no objects yet)
// records a zero-byte reservation: the namespace is pinned and counts
// against MaxPinnedNamespaces. Unpin removes a name. Pin copies keys and
// refuses a negative charge.
func (c *Disk) Pin(name string, keys map[string]int64) error {
	if c == nil {
		return ErrPinCapacity
	}
	var bytes int64
	for key, charge := range keys {
		if charge < 0 {
			return fmt.Errorf("cache: pin %q: negative charge %d for %q", name, charge, key)
		}
		bytes += charge
	}
	keys = maps.Clone(keys)
	if keys == nil {
		keys = map[string]int64{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrWarmCache
	}
	if bytes > c.pinCap-c.pinnedBytes+c.pinBytes[name] {
		return ErrPinCapacity
	}
	if _, ok := c.pins[name]; !ok && len(c.pins) >= c.maxPinned {
		return ErrPinCapacity
	}
	if at, ok := c.pinAt[name]; ok {
		c.pinPrevAt[name] = at
	}
	c.pinKeys(c.pins[name], -1)
	c.pinKeys(keys, +1)
	c.pinnedBytes += bytes - c.pinBytes[name]
	c.pinBytes[name], c.pins[name], c.pinAt[name] = bytes, keys, time.Now()
	return nil
}

// SetMaxPinnedNamespaces overrides the MaxPinnedNamespaces count cap, for
// tests that cannot afford 257 namespaces. It does not unpin anything.
func (c *Disk) SetMaxPinnedNamespaces(n int) {
	if c == nil || n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxPinned = n
}

// PinnedNamespace reports whether name currently holds a pin reservation. It
// is one map lookup under the cache mutex, cheap enough to ask per request.
// A process with no disk cache has no pinned namespaces.
func (c *Disk) PinnedNamespace(name string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pins[name]
	return ok
}

// Unpin releases name's reservation. Its keys become evictable unless
// another name pins them.
func (c *Disk) Unpin(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unpinLocked(name)
}

func (c *Disk) unpinLocked(name string) {
	c.pinKeys(c.pins[name], -1)
	c.pinnedBytes -= c.pinBytes[name]
	delete(c.pinBytes, name)
	delete(c.pins, name)
	delete(c.pinAt, name)
	delete(c.pinPrevAt, name)
}

// Wipe models loss of disposable local storage while retaining
// reservations: every entry is dropped and the directory starts empty.
func (c *Disk) Wipe() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	trash, err := c.detachDir(true)
	c.mu.Unlock()
	if trash != "" {
		if rerr := os.RemoveAll(trash); err == nil {
			err = rerr
		}
	}
	return err
}

func (c *Disk) remove(el *list.Element) bool {
	if el == nil {
		return false
	}
	e := el.Value.(*diskEntry)
	if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
		c.failures++
		return false // retain the charge and refuse further growth
	}
	delete(c.items, e.key)
	c.ll.Remove(el)
	c.size -= e.charge
	if c.isPinned(e.key) {
		c.pinnedResident -= e.charge
	}
	c.evictions++
	return true
}

// PinStatus is one namespace's reservation, when it was (re)made, and how
// much of it is actually on disk right now. Pin charges every key in the
// set whether or not it is resident (the reservation is a promise of
// space), so resident < reserved is the ordinary state between Pin and
// the warm that follows it, and the state after the disk cache is lost
// (Wipe keeps reservations).
func (c *Disk) PinStatus(name string) (reserved, resident int64, at time.Time, pinned bool) {
	if c == nil {
		return 0, 0, time.Time{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys, ok := c.pins[name]
	if !ok {
		return 0, 0, time.Time{}, false
	}
	for key, charge := range keys {
		reserved += charge
		if c.items[key] != nil {
			resident += charge
		}
	}
	return reserved, resident, c.pinAt[name], true
}

// Stats snapshots occupancy and counters.
func (c *Disk) Stats() DiskStats {
	if c == nil {
		return DiskStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return DiskStats{CapacityBytes: c.cap, UsedBytes: c.size, PinCapacityBytes: c.pinCap, PinnedBytes: c.pinnedBytes, PinnedNamespaces: len(c.pins), Entries: len(c.items), Hits: c.hits, Misses: c.misses, Evictions: c.evictions, Failures: c.failures, InactiveExpiries: c.inactiveExpiries}
}

// Close stops caching and removes the directory. Reservations are kept, so
// PinStatus still answers; every later Put is refused.
func (c *Disk) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	trash, err := c.detachDir(false)
	if err != nil {
		c.failures++
	}
	logger := c.logger()
	c.mu.Unlock()
	if err == nil && trash != "" {
		err = os.RemoveAll(trash)
	}
	if err != nil {
		logger.Error("cache: remove disk tier directory", "dir", c.dir, "err", err)
	}
}

// DiskKey is the on-disk identity of a logical cache key at one generation
// of its namespace (ByteCache.GenerationOf). A generation of 0 stores the
// logical key unchanged.
func DiskKey(key string, generation uint64) string {
	if generation == 0 {
		return key
	}
	return strconv.FormatUint(generation, 10) + ":" + key
}

// Dir is the disposable directory this cache owns. Tests remove it to model
// a failed disk publication.
func (c *Disk) Dir() string {
	if c == nil {
		return ""
	}
	return c.dir
}

// PinLock serializes pin transactions (reservation + warm + rollback). On
// a nil *Disk it is a fresh mutex: there is nothing to serialize.
func (c *Disk) PinLock() *sync.Mutex {
	if c == nil {
		return new(sync.Mutex)
	}
	return &c.pinMu
}

// PinState is the pin capacity and whether the cache has closed.
func (c *Disk) PinState() (capacity int64, closed bool) {
	if c == nil {
		return 0, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pinCap, c.closed
}

// SnapshotPin copies name's current reservation, or nil if none.
func (c *Disk) SnapshotPin(name string) map[string]int64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.pins[name])
}

// RestorePin undoes a failed pin transaction by putting previous (from
// SnapshotPin) back, or clearing the name when previous is nil (it was not
// pinned). An empty previous restores a pinned namespace with nothing to
// reserve. The reservation keeps the time it was made. Caller holds
// PinLock.
func (c *Disk) RestorePin(name string, previous map[string]int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.pinPrevAt[name]
	if !ok {
		at, ok = c.pinAt[name]
	}
	c.unpinLocked(name)
	if previous == nil {
		return
	}
	if !ok {
		at = time.Now()
	}
	var bytes int64
	for _, charge := range previous {
		bytes += charge
	}
	previous = maps.Clone(previous)
	c.pinKeys(previous, +1)
	c.pins[name], c.pinBytes[name], c.pinAt[name] = previous, bytes, at
	c.pinnedBytes += bytes
}

// FirstFile is the key and path of one cached file, for tests that corrupt it.
func (c *Disk) FirstFile() (key, path string) {
	if c == nil {
		return "", ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		return k, el.Value.(*diskEntry).path
	}
	return "", ""
}

// KeysForTest lists the disk cache's keys, for tests that assert exactly
// what a warm left behind.
func (d *Disk) KeysForTest() []string {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := make([]string, 0, len(d.items))
	for k := range d.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
