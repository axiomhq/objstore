package cache

import (
	"bytes"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
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
