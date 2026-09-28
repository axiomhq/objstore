package cache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestDiskCachePinCapacityAndEviction(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 8192, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Put("ns/a/one", []byte("one"))
	if err := c.Pin("a", map[string]int64{"ns/a/one": 4096}); err != nil {
		t.Fatal(err)
	}
	c.Put("ns/b/two", []byte("two"))
	if err := c.Pin("b", map[string]int64{"ns/b/two": 4096}); !errors.Is(err, ErrPinCapacity) {
		t.Fatalf("second pin = %v, want ErrPinCapacity", err)
	}
	c.Unpin("a")
	if err := c.Pin("b", map[string]int64{"ns/b/two": 4096}); err != nil {
		t.Fatalf("pin after unpin: %v", err)
	}
	c.Put("ns/c/three", []byte("three"))
	if _, ok := c.Get("ns/b/two"); !ok {
		t.Fatal("pinned entry evicted")
	}
}

func TestDiskCacheLossIsAMissAndPinSurvivesRewarm(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 8192, 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Put("ns/a/one", []byte("one"))
	if err := c.Pin("a", map[string]int64{"ns/a/one": 4096}); err != nil {
		t.Fatal(err)
	}
	if err := c.Wipe(); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("ns/a/one"); ok {
		t.Fatal("lost file remained a cache hit")
	}
	c.Put("ns/a/one", []byte("one"))
	c.Put("ns/b/two", []byte("two"))
	c.Put("ns/c/three", []byte("three"))
	if _, ok := c.Get("ns/a/one"); !ok {
		t.Fatal("pin was not retained across cache loss and re-warm")
	}
}

func TestPinnedCacheHitRatioUnderPressure(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 12<<10, 4<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Put("ns/pinned/object", make([]byte, 1024))
	if err := c.Pin("pinned", map[string]int64{"ns/pinned/object": 4096}); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		c.Put("ns/pressure/"+string(rune(i)), make([]byte, 1024))
		if _, ok := c.Get("ns/pinned/object"); !ok {
			t.Fatalf("pinned read %d missed", i)
		}
	}
	if stats := c.Stats(); stats.Hits < 100 {
		t.Fatalf("pinned hits = %d, want 100", stats.Hits)
	}
}

// TestDiskCachePinCountCap pins MaxPinnedNamespaces: the 257th distinct
// name is refused whatever the byte headroom, re-pinning a pinned name does not count twice, and unpinning
// frees a slot.
func TestDiskCachePinCountCap(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	name := func(i int) string { return "ns" + strconv.Itoa(i) }
	for i := range MaxPinnedNamespaces {
		if err := c.Pin(name(i), map[string]int64{name(i) + "/obj": 1}); err != nil {
			t.Fatalf("pin %d: %v", i, err)
		}
	}
	if err := c.Pin(name(MaxPinnedNamespaces), map[string]int64{"extra/obj": 1}); !errors.Is(err, ErrPinCapacity) {
		t.Fatalf("pin 257th = %v, want ErrPinCapacity", err)
	}
	if err := c.Pin(name(0), map[string]int64{name(0) + "/obj": 1, name(0) + "/obj2": 1}); err != nil {
		t.Fatalf("re-pin at the cap: %v", err)
	}
	if st := c.Stats(); st.PinnedNamespaces != MaxPinnedNamespaces {
		t.Fatalf("pinned namespaces = %d, want %d", st.PinnedNamespaces, MaxPinnedNamespaces)
	}
	c.Unpin(name(1))
	if err := c.Pin(name(MaxPinnedNamespaces), map[string]int64{"extra/obj": 1}); err != nil {
		t.Fatalf("pin after unpin: %v", err)
	}
	c.SetMaxPinnedNamespaces(1)
	if err := c.Pin("another", map[string]int64{"another/obj": 1}); !errors.Is(err, ErrPinCapacity) {
		t.Fatalf("pin over a lowered cap = %v, want ErrPinCapacity", err)
	}
}

// TestDiskConcurrentGetPutUnderEviction: reads and writes run outside the
// cache mutex, so a Get can race the eviction or replacement of its file.
// Every hit must still return the key's exact bytes, and the accounting
// must end consistent with the files on disk.
func TestDiskConcurrentGetPutUnderEviction(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 16*4096)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	value := func(i int) []byte { return bytes.Repeat([]byte{byte(i)}, 3000+i) }
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 400 {
				i := (w*7 + n) % 40
				key := "ns/x/k" + strconv.Itoa(i)
				if n%3 == 0 {
					c.Put(key, value(i))
					continue
				}
				if b, ok := c.Get(key); ok && !bytes.Equal(b, value(i)) {
					t.Errorf("%s: got %d bytes of another value", key, len(b))
					return
				}
			}
		}()
	}
	wg.Wait()
	st := c.Stats()
	files, err := os.ReadDir(c.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if st.UsedBytes > st.CapacityBytes || st.Entries != len(files) || st.Failures != 0 {
		t.Fatalf("stats %+v with %d files on disk", st, len(files))
	}
}

// TestDiskGetRangeVerifiesOnlyItsBlocks: a range is read and checked by
// its covering 4 KiB blocks; a damaged block outside it does not matter, a
// damaged block inside it is a miss that drops the entry, and a range past
// the object is a miss that keeps it.
func TestDiskGetRangeVerifiesOnlyItsBlocks(t *testing.T) {
	c, err := NewDisk(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	obj := make([]byte, 5*diskBlock+100)
	for i := range obj {
		obj[i] = byte(i * 7)
	}
	c.Put("ns/x/object", obj)
	for _, r := range [][2]int64{{0, 1}, {diskBlock - 3, 10}, {2*diskBlock + 5, 3 * diskBlock}, {int64(len(obj)) - 7, 7}, {int64(len(obj)), 0}} {
		got, ok := c.GetRange("ns/x/object", r[0], r[1])
		if !ok || !bytes.Equal(got, obj[r[0]:r[0]+r[1]]) || cap(got) > len(got)+len(got)/8+64 { // no whole block retained
			t.Fatalf("range %v: ok=%v, %d bytes (cap %d)", r, ok, len(got), cap(got))
		}
	}
	if _, ok := c.GetRange("ns/x/object", int64(len(obj))-7, 8); ok {
		t.Fatal("a range past the object was served")
	}
	_, path := c.FirstFile()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 4*diskBlock+1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if got, ok := c.GetRange("ns/x/object", 0, 2*diskBlock); !ok || !bytes.Equal(got, obj[:2*diskBlock]) {
		t.Fatal("a damaged block outside the range failed the range")
	}
	if _, ok := c.GetRange("ns/x/object", 4*diskBlock, 10); ok {
		t.Fatal("a damaged block was served")
	}
	if c.Has("ns/x/object") || c.Stats().Failures != 1 {
		t.Fatalf("damaged entry kept: has=%v stats=%+v", c.Has("ns/x/object"), c.Stats())
	}
}

// TestDiskPutReservesBeforeWriting: a Put's charge counts against the
// capacity while its temporary file is written, so concurrent fills never
// hold more bytes on disk than the cap, and the reservation is released.
func TestDiskPutReservesBeforeWriting(t *testing.T) {
	const cap = 8 * diskBlock
	c, err := NewDisk(t.TempDir(), cap)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var over sync.Once
	wg.Add(1)
	go func() { // sample the directory while the writers run
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var total int64
			files, _ := os.ReadDir(c.Dir())
			for _, f := range files {
				if info, err := f.Info(); err == nil {
					total += DiskCharge(int(info.Size()))
				}
			}
			if total > cap {
				over.Do(func() { t.Errorf("%d bytes on disk over the %d cap", total, cap) })
			}
		}
	}()
	var writers sync.WaitGroup
	for w := range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range 50 {
				c.Put("ns/x/k"+strconv.Itoa(w*50+i), make([]byte, 3*diskBlock))
			}
		}()
	}
	writers.Wait()
	close(stop)
	wg.Wait()
	c.mu.Lock()
	reserved := c.reserved
	c.mu.Unlock()
	if st := c.Stats(); reserved != 0 || st.UsedBytes > cap || st.Failures != 0 {
		t.Fatalf("reserved %d, stats %+v", reserved, st)
	}
}

func newDisk(t *testing.T, capacity int64, pinCapacity ...int64) *Disk {
	t.Helper()
	c, err := NewDisk(t.TempDir(), capacity, pinCapacity...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func (c *Disk) pinnedResidentForTest() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pinnedResident
}

// TestMakeRoomRefusesBeforeEvicting: a Put that cannot fit even with every
// unpinned entry gone evicts nothing: it used to empty the cache and then
// fail anyway.
func TestMakeRoomRefusesBeforeEvicting(t *testing.T) {
	c := newDisk(t, 3*diskBlock, 2*diskBlock)
	c.Put("ns/p/1", []byte("1"))
	c.Put("ns/p/2", []byte("2"))
	if err := c.Pin("p", map[string]int64{"ns/p/1": diskBlock, "ns/p/2": diskBlock}); err != nil {
		t.Fatal(err)
	}
	c.Put("ns/o/1", []byte("o"))
	if err := c.PutChecked("ns/o/big", make([]byte, 2*diskBlock)); !errors.Is(err, ErrWarmCache) {
		t.Fatalf("put past the unpinned room = %v, want ErrWarmCache", err)
	}
	if st := c.Stats(); st.Evictions != 0 || !c.Has("ns/o/1") {
		t.Fatalf("a refused put evicted: %+v, other namespace resident %v", st, c.Has("ns/o/1"))
	}
	if err := c.PutChecked("ns/o/2", []byte("2")); err != nil { // fits by evicting ns/o/1
		t.Fatal(err)
	}
	if c.Has("ns/o/1") || !c.Has("ns/p/1") || !c.Has("ns/p/2") {
		t.Fatalf("keys %v, want the unpinned entry evicted", c.KeysForTest())
	}
}

func TestPinnedResidentTracksPinsAndResidency(t *testing.T) {
	c := newDisk(t, 16*diskBlock)
	want := func(n int64) {
		t.Helper()
		if got := c.pinnedResidentForTest(); got != n*diskBlock {
			t.Fatalf("pinned resident %d, want %d blocks", got, n)
		}
	}
	pin := func(name string, keys ...string) {
		t.Helper()
		set := map[string]int64{}
		for _, k := range keys {
			set[k] = diskBlock
		}
		if err := c.Pin(name, set); err != nil {
			t.Fatal(err)
		}
	}
	pin("a", "ns/a/1")
	want(0) // pinned, not resident
	c.Put("ns/a/1", []byte("1"))
	want(1)
	pin("b", "ns/a/1") // pinned twice, charged once
	want(1)
	c.Unpin("a")
	want(1)
	c.Unpin("b")
	want(0)
	snap := c.SnapshotPin("b")
	pin("b", "ns/a/1")
	want(1)
	c.RestorePin("b", snap) // nil: b was not pinned
	want(0)
	pin("a", "ns/a/1")
	if err := c.Wipe(); err != nil {
		t.Fatal(err)
	}
	want(0)
	c.Put("ns/a/1", []byte("1"))
	want(1)
	pin("a") // re-pin to an empty set releases the key
	want(0)
}

func TestPinSnapshotRestore(t *testing.T) {
	c := newDisk(t, 16*diskBlock)
	keys := map[string]int64{"ns/a/1": diskBlock}
	if err := c.Pin("a", keys); err != nil {
		t.Fatal(err)
	}
	keys["ns/a/2"] = 8 * diskBlock // Pin copied the caller's map
	if reserved, _, _, _ := c.PinStatus("a"); reserved != diskBlock {
		t.Fatalf("reserved %d after mutating the caller's map", reserved)
	}
	_, _, at, _ := c.PinStatus("a")
	snap := c.SnapshotPin("a")
	time.Sleep(time.Millisecond) // distinguishable timestamps; not synchronization
	if err := c.Pin("a", map[string]int64{"ns/a/1": diskBlock, "ns/a/2": diskBlock}); err != nil {
		t.Fatal(err)
	}
	c.RestorePin("a", snap)
	reserved, _, restoredAt, pinned := c.PinStatus("a")
	if !pinned || reserved != diskBlock || !restoredAt.Equal(at) {
		t.Fatalf("restored %d bytes at %v (pinned %v), want %d at %v", reserved, restoredAt, pinned, diskBlock, at)
	}
	if st := c.Stats(); st.PinnedBytes != diskBlock {
		t.Fatalf("pinned bytes %d after restore", st.PinnedBytes)
	}
	if err := c.Pin("a", map[string]int64{"ns/a/1": -1}); err == nil {
		t.Fatal("negative charge accepted")
	}
}

func TestExpireInactive(t *testing.T) {
	c := newDisk(t, 16*diskBlock)
	for _, k := range []string{"ns/a/1", "ns/b/1", "ns/c/1", "other"} {
		c.Put(k, []byte(k))
	}
	if err := c.Pin("c", map[string]int64{"ns/c/1": diskBlock}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if got := c.ExpireInactive(time.Hour, now); got != nil {
		t.Fatalf("expired %v within the ttl", got)
	}
	c.Touch("unknown") // restarts a known namespace's clock only; never adds one
	later := time.Now().Add(time.Hour)
	got := c.ExpireInactive(time.Minute, later)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expired %v, want [a b]", got)
	}
	if c.Has("ns/a/1") || c.Has("ns/b/1") || !c.Has("ns/c/1") || !c.Has("other") {
		t.Fatalf("keys after expiry: %v", c.KeysForTest())
	}
	if c.Stats().InactiveExpiries != 2 {
		t.Fatalf("stats %+v", c.Stats())
	}
	if got := c.ExpireInactive(time.Minute, later); got != nil {
		t.Fatalf("expired %v twice", got)
	}
}

// TestNewDiskRemovesStaleDirectories: a process that crashed before Close
// left its directory under root; the next NewDisk over the root removes it.
// The root's name holds glob metacharacters, which the sweep must take
// literally.
func TestNewDiskRemovesStaleDirectories(t *testing.T) {
	if !sweepable {
		t.Skip("no file lock: nothing is swept")
	}
	root := filepath.Join(t.TempDir(), "root[1]")
	crashed, err := NewDisk(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	crashed.Put("ns/a/1", []byte("1"))
	crashed.lock.Close() // the crash: the lock goes with the process, the directory stays
	keep := filepath.Join(root, "unrelated")
	if err := os.Mkdir(keep, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := NewDisk(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(crashed.Dir()); !os.IsNotExist(err) {
		t.Fatalf("stale directory survived: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated directory removed: %v", err)
	}
	c.Put("ns/a/1", []byte("1"))
	if err := c.Wipe(); err != nil {
		t.Fatal(err)
	}
	c.Close()
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 || entries[0].Name() != "unrelated" {
		t.Fatalf("root after Wipe and Close: %v", entries)
	}
}

// TestTwoDisksShareARoot: a root may host several live Disks. Neither
// NewDisk sweeps the other's directory, and closing one leaves the other
// serving; Wipe after Close is a no-op.
func TestTwoDisksShareARoot(t *testing.T) {
	if !sweepable {
		t.Skip("no file lock: nothing is swept")
	}
	root := t.TempDir()
	a, err := NewDisk(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.Put("ns/a/1", []byte("from a"))
	b, err := NewDisk(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	b.Put("ns/b/1", []byte("from b"))
	third, err := NewDisk(root, 1<<20) // sweeps again, with both live
	if err != nil {
		t.Fatal(err)
	}
	third.Close()
	for _, tc := range []struct {
		d        *Disk
		key, val string
	}{{a, "ns/a/1", "from a"}, {b, "ns/b/1", "from b"}} {
		if got, ok := tc.d.Get(tc.key); !ok || string(got) != tc.val {
			t.Fatalf("%s: %q, %v: a live Disk's directory was swept", tc.key, got, ok)
		}
	}
	b.Close()
	if err := b.Wipe(); err != nil {
		t.Fatalf("Wipe after Close: %v", err)
	}
	b.Close() // idempotent
	if _, err := os.Stat(b.home); !os.IsNotExist(err) {
		t.Fatalf("closed Disk's directory: %v", err)
	}
	if got, ok := a.Get("ns/a/1"); !ok || string(got) != "from a" {
		t.Fatalf("closing b cost a its entry: %q, %v", got, ok)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 || filepath.Join(root, entries[0].Name()) != a.home {
		t.Fatalf("root holds %v, want only a's directory", entries)
	}
}

// TestSweepRacesNewDisk: Disks created while others sweep the same root
// all survive with their entries.
func TestSweepRacesNewDisk(t *testing.T) {
	if !sweepable {
		t.Skip("no file lock: nothing is swept")
	}
	root := t.TempDir()
	const n = 8
	disks := make([]*Disk, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if disks[i], errs[i] = NewDisk(root, 1<<20); errs[i] == nil {
				disks[i].Put("k", []byte{byte(i)})
			}
		})
	}
	wg.Wait()
	for i, d := range disks {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if got, ok := d.Get("k"); !ok || got[0] != byte(i) {
			t.Fatalf("disk %d lost its entry: %v, %v", i, got, ok)
		}
		d.Close()
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Fatalf("root after closing every Disk: %v", entries)
	}
}

func TestNilDiskIsSafe(t *testing.T) {
	var c *Disk
	c.Put("k", nil)
	if c.PutChecked("k", nil) == nil || c.Pin("a", nil) == nil {
		t.Fatal("nil disk accepted a put or a pin")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("nil disk hit")
	}
	if _, ok := c.GetRange("k", 0, 1); ok || c.Has("k") || c.PinnedNamespace("a") {
		t.Fatal("nil disk holds something")
	}
	c.Touch("a")
	c.Unpin("a")
	c.SetMaxPinnedNamespaces(1)
	c.RestorePin("a", nil)
	c.PinLock().Lock()
	if c.Wipe() != nil || c.ExpireInactive(time.Second, time.Now()) != nil || c.PinHeadroom("a") != 0 ||
		c.SnapshotPin("a") != nil || c.KeysForTest() != nil || c.Dir() != "" || c.Stats() != (DiskStats{}) {
		t.Fatal("nil disk reports state")
	}
	if _, _, _, pinned := c.PinStatus("a"); pinned {
		t.Fatal("nil disk pinned")
	}
	if k, p := c.FirstFile(); k != "" || p != "" {
		t.Fatal("nil disk has a file")
	}
	c.Close()
}

// TestRemoveHomeClosingLockFirst runs the Windows removal path where CI
// runs (Unix): the whole directory goes, lock file included, and the lock
// is released.
func TestRemoveHomeClosingLockFirst(t *testing.T) {
	root := t.TempDir()
	home, lock, err := lockedHome(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, dataDir, "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, dataDir, "sub", "blob"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := removeHomeClosingLockFirst(home, lock); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("home after removal: %v", err)
	}
	if _, err := lock.Stat(); err == nil {
		t.Fatal("lock still open after removal")
	}
}
