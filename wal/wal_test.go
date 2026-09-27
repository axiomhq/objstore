package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"strings"
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
)

const testPrefix = "log/"

// entry is an Entry decoded with Decode: each page's records.
type entry = Entry[[][]byte]

// rows builds one record per id, the id being the record's bytes.
func rows(ids ...string) []Bytes {
	out := make([]Bytes, len(ids))
	for i, id := range ids {
		out[i] = Bytes(id)
	}
	return out
}

// put claims h.Seq under prefix with one page. false = another writer got
// there first.
func put(ctx context.Context, s *objstore.Store, prefix string, h Header, records ...Bytes) (bool, error) {
	data, err := Encode(h, records)
	if err != nil {
		return false, err
	}
	return s.PutIfAbsent(ctx, Key(prefix, h.Seq), data)
}

// collect walks (after, through] and returns every visited entry.
func collect(ctx context.Context, s *objstore.Store, prefix string, after, through uint64) ([]entry, error) {
	var out []entry
	err := Walk(ctx, s, prefix, after, through, Decode, func(e entry) error {
		out = append(out, e)
		return nil
	})
	return out, err
}

// replay walks the live log above after.
func replay(ctx context.Context, s *objstore.Store, prefix string, after uint64) ([]entry, error) {
	return collect(ctx, s, prefix, after, 0)
}

// ids flattens an entry's pages into its records, as strings.
func ids(e entry) []string {
	var out []string
	for _, p := range e.Pages {
		for _, r := range p {
			out = append(out, string(r))
		}
	}
	return out
}

// sameRecords reports whether an entry's pages carry want, in order.
func sameRecords(e entry, want []Bytes) bool {
	var got [][]byte
	for _, p := range e.Pages {
		got = append(got, p...)
	}
	return slices.EqualFunc(got, want, func(a []byte, b Bytes) bool { return bytes.Equal(a, b) })
}

func TestKeyOrdering(t *testing.T) {
	// Lexical order must equal numeric order — List depends on it.
	if Key("x/", 2) >= Key("x/", 10) {
		t.Fatalf("key ordering broken: %q >= %q", Key("x/", 2), Key("x/", 10))
	}
	seq, err := SeqFromKey(Key("x/", 42))
	if err != nil || seq != 42 {
		t.Fatalf("round-trip: %d, %v", seq, err)
	}
}

func TestAppendReplay(t *testing.T) {
	s := storetest.New(t)
	ctx := context.Background()
	for seq := uint64(1); seq <= 3; seq++ {
		ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes{byte('a' + seq - 1)})
		if err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", seq, ok, err)
		}
	}
	// Claiming an existing seq loses.
	ok, err := put(ctx, s, testPrefix, Header{Seq: 2}, Bytes("z"))
	if err != nil || ok {
		t.Fatalf("reclaim seq 2 should lose: ok=%v err=%v", ok, err)
	}
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Seq != 1 || entries[2].Seq != 3 {
		t.Fatalf("replay all: %+v", entries)
	}
	if !slices.Equal(ids(entries[1]), []string{"b"}) {
		t.Fatalf("entry 2 content: %+v", entries[1])
	}
	entries, err = replay(ctx, s, testPrefix, 2)
	if err != nil || len(entries) != 1 || entries[0].Seq != 3 {
		t.Fatalf("replay after=2: %+v, %v", entries, err)
	}
	// Prefixes are isolated.
	entries, err = replay(ctx, s, "other/", 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("other prefix should be empty: %+v, %v", entries, err)
	}
}

// TestReplayWalksToNotFound: sequence numbers are the page catalog, so a
// walk GETs after+1, +2, ... and stops at the first miss. No listing, and
// the only request beyond the pages it had to read anyway is that miss.
func TestReplayWalksToNotFound(t *testing.T) {
	s, f := storetest.NewFaulty(t)
	ctx := context.Background()
	for seq := uint64(1); seq <= 3; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes("d")); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", seq, ok, err)
		}
	}
	f.ResetOps()
	entries, err := replay(ctx, s, testPrefix, 0)
	if err != nil || len(entries) != 3 {
		t.Fatalf("replay: %+v, %v", entries, err)
	}
	ops := f.Ops()
	if ops[storetest.OpList] != 0 {
		t.Fatalf("the walk listed the WAL prefix: %v", ops)
	}
	if ops[storetest.OpGet] != 4 {
		t.Fatalf("replay of 3 pages cost %d GETs, want 3 + the miss: %v", ops[storetest.OpGet], ops)
	}
	// From a watermark: only what is above it, plus the miss.
	f.ResetOps()
	if entries, err = replay(ctx, s, testPrefix, 2); err != nil || len(entries) != 1 || entries[0].Seq != 3 {
		t.Fatalf("replay after=2: %+v, %v", entries, err)
	}
	if ops := f.Ops(); ops[storetest.OpGet] != 2 || ops[storetest.OpList] != 0 {
		t.Fatalf("replay from a watermark: %v", ops)
	}
	// A page ABOVE a gap is not part of the log: the walk ends at the gap.
	// No writer can produce one (TestCrashedWriterLeavesNoHole); this pins
	// what the rule means if bytes ever appear up there anyway.
	if ok, err := put(ctx, s, testPrefix, Header{Seq: 5}, Bytes("orphan")); err != nil || !ok {
		t.Fatalf("plant seq 5: ok=%v err=%v", ok, err)
	}
	if entries, err = replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 3 {
		t.Fatalf("the walk did not stop at the gap: %+v, %v", entries, err)
	}
	f.Set(storetest.Plan{Op: storetest.OpGet, Key: Key(testPrefix, 2), N: 1, Mode: storetest.Fail})
	if _, err := replay(ctx, s, testPrefix, 0); !errors.Is(err, storetest.ErrFault) || !strings.Contains(err.Error(), Key(testPrefix, 2)) {
		t.Fatalf("replay read failure must preserve cause and name the page: %v", err)
	}
}

// FuzzSeqFromKey: a hostile or truncated key must error rather than panic
// on the slice.
func FuzzSeqFromKey(f *testing.F) {
	for _, s := range []string{
		Key(testPrefix, 0), Key(testPrefix, 42), Key(testPrefix, ^uint64(0)),
		"", "/", "log/", "log/abc", "log/-1",
		"log/99999999999999999999999", "log/+7", "log/0x10",
		"12", strings.Repeat("/", 64), "\xff",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, key string) {
		seq, err := SeqFromKey(key)
		if err != nil {
			return
		}
		// A key that parses must round-trip through the canonical form.
		got, err := SeqFromKey(Key(testPrefix, seq))
		if err != nil || got != seq {
			t.Fatalf("SeqFromKey(%q)=%d, but canonical key re-parses as %d (%v)", key, seq, got, err)
		}
	})
}

func TestWalkWithGetUsesSuppliedFetcher(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	if ok, err := put(ctx, s, testPrefix, Header{Seq: 1}, Bytes("a")); err != nil || !ok {
		t.Fatalf("append: ok=%v err=%v", ok, err)
	}
	reads, seen := 0, 0
	err := WalkWithGet(ctx, func(ctx context.Context, key string) ([]byte, error) {
		reads++
		return s.Get(ctx, key)
	}, testPrefix, 0, 1, Decode, func(e entry) error {
		seen += len(ids(e))
		return nil
	})
	if err != nil || reads != 1 || seen != 1 {
		t.Fatalf("walk: reads=%d records=%d err=%v", reads, seen, err)
	}
}

func TestReplayPublishesBatchOnlyAtLastPage(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	records := rows("a", "b")
	for i, r := range records {
		h := Header{Seq: uint64(i + 1), Nonce: "batch", BatchPages: 2, BatchIndex: uint64(i)}
		if ok, err := put(ctx, s, testPrefix, h, r); err != nil || !ok {
			t.Fatalf("append page %d: %v, %v", i, ok, err)
		}
		entries, err := replay(ctx, s, testPrefix, 0)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && (len(entries) != 1 || entries[0].Seq != 1 || !entries[0].Incomplete || entries[0].Pages != nil) {
			t.Fatalf("incomplete batch visible: %+v", entries)
		}
		if i == 1 && (len(entries) != 1 || entries[0].Seq != 2 || entries[0].Incomplete || !sameRecords(entries[0], records)) {
			t.Fatalf("completed batch replay: %+v", entries)
		}
	}
	entries, err := collect(ctx, s, testPrefix, 0, 1)
	if err != nil || len(entries) != 1 || entries[0].Seq != 1 || entries[0].Incomplete || entries[0].Pages != nil {
		t.Fatalf("bounded walk included a batch completed beyond its boundary: %+v, %v", entries, err)
	}
	entries, err = collect(ctx, s, testPrefix, 0, 2)
	if err != nil || len(entries) != 1 || !sameRecords(entries[0], records) {
		t.Fatalf("complete bounded batch: %+v, %v", entries, err)
	}
	if _, err := collect(ctx, s, testPrefix, 0, 3); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing bounded page is corruption, not end of log: %v", err)
	}
}

func TestReplayRejectsSequenceMismatchInsideBatch(t *testing.T) {
	ctx := context.Background()
	s := storetest.New(t)
	for seq := uint64(1); seq <= 2; seq++ {
		h := Header{Seq: seq, Nonce: "batch", BatchPages: 2, BatchIndex: seq - 1}
		if seq == 1 {
			h.Seq = 42
		}
		data, err := Encode(h, rows("a"))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, Key(testPrefix, seq), data); err != nil {
			t.Fatal(err)
		}
	}
	if entries, err := replay(ctx, s, testPrefix, 0); err == nil {
		t.Fatalf("batch coalescing hid an invalid page sequence: %+v", entries)
	}
}

func TestDecodeRejectsHugeDeclaredCountWithoutAllocation(t *testing.T) {
	b := append([]byte(pageMagic), 0, 0, 0, 0, 0) // seq, empty nonce, zero commit time, no batch
	b = binary.AppendUvarint(b, ^uint64(0))
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	if allocs := testing.AllocsPerRun(100, func() {
		if _, _, err := Decode(b); err == nil {
			t.Fatal("accepted impossible record count")
		}
	}); allocs > 5 {
		t.Fatalf("huge count used %.0f allocations, want constant-size rejection", allocs)
	}
}
