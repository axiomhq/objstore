package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
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
	if got := Key("x/", 7); got != "x/00000000000000000007" {
		t.Fatalf("key format: %q", got)
	}
}

func TestAppendReplay(t *testing.T) {
	s := bucket.New(t)
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
	s, f := bucket.NewFaulty(t)
	ctx := context.Background()
	for seq := uint64(1); seq <= 3; seq++ {
		if ok, err := put(ctx, s, testPrefix, Header{Seq: seq}, Bytes("d")); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", seq, ok, err)
		}
	}
	// From either the start or a watermark: only what is above it, plus the miss.
	for name, after := range map[string]uint64{"all": 0, "after-watermark": 2} {
		t.Run(name, func(t *testing.T) {
			f.ResetOps()
			entries, err := replay(ctx, s, testPrefix, after)
			if err != nil || len(entries) != 3-int(after) {
				t.Fatalf("replay after=%d: %+v, %v", after, entries, err)
			}
			for i, e := range entries {
				if e.Seq != after+uint64(i)+1 {
					t.Fatalf("replay after=%d: entry %d has sequence %d", after, i, e.Seq)
				}
			}
			if ops := f.Ops(); ops[storetest.OpGet] != len(entries)+1 || ops[storetest.OpList] != 0 {
				t.Fatalf("replay after=%d: %v", after, ops)
			}
		})
	}
	// A page ABOVE a gap is not part of the log: the walk ends at the gap.
	// No writer can produce one (TestCrashedWriterLeavesNoHole); this pins
	// what the rule means if bytes ever appear up there anyway.
	if ok, err := put(ctx, s, testPrefix, Header{Seq: 5}, Bytes("orphan")); err != nil || !ok {
		t.Fatalf("plant seq 5: ok=%v err=%v", ok, err)
	}
	if entries, err := replay(ctx, s, testPrefix, 0); err != nil || len(entries) != 3 {
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

func TestReplayPublishesBatchOnlyAtLastPage(t *testing.T) {
	ctx := context.Background()
	s := bucket.New(t)
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
	s := bucket.New(t)
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

func TestFramedSizeAllocationFree(t *testing.T) {
	if allocs := testing.AllocsPerRun(100, func() {
		for _, tc := range []struct{ n, want int }{{0, 1}, {127, 128}, {128, 130}, {16383, 16385}, {16384, 16387}, {maxPageBytes, maxPageBytes + 4}} {
			if got := framedSize(tc.n); got != tc.want {
				t.Fatalf("framedSize(%d) = %d, want %d", tc.n, got, tc.want)
			}
		}
	}); allocs != 0 {
		t.Fatalf("framedSize used %g allocations, want zero", allocs)
	}
}

func TestEncodeRefusesOverflowingSize(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("Encode panicked instead of refusing Size math.MaxInt: %v", p)
		}
	}()
	if _, err := Encode(Header{}, []testRecord{{size: math.MaxInt}}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Encode: %v, want ErrRecordTooLarge", err)
	}
}

func TestEncodeRefusesWeightOverflow(t *testing.T) {
	records := []weighed{{Bytes("a"), math.MaxUint64}, {Bytes("b"), 1}}
	if _, err := Encode(Header{}, records); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Encode overflowing weight: %v, want ErrInvalidRecord", err)
	}
	records[1].w = 0
	data, err := Encode(Header{}, records)
	if err != nil {
		t.Fatal(err)
	}
	h, _, err := Decode(data)
	if err != nil || h.Weight != math.MaxUint64 {
		t.Fatalf("maximum weight: %d, %v", h.Weight, err)
	}
}

func TestEncodeRefusesUnsetCommitMillisecond(t *testing.T) {
	for name, at := range map[string]time.Time{
		"epoch":     time.UnixMilli(0).UTC(),
		"sub-ms":    time.Unix(0, 999999).UTC(),
		"pre-epoch": time.UnixMilli(-1).UTC(),
	} {
		t.Run(name, func(t *testing.T) {
			data, err := Encode(Header{At: at}, rows("a"))
			if err == nil {
				h, _, derr := Decode(data)
				t.Fatalf("Encode accepted unrepresentable At %v: decoded %v, %v", at, h.At, derr)
			}
		})
	}
	for name, at := range map[string]time.Time{"unset": {}, "first-ms": time.UnixMilli(1).UTC()} {
		t.Run(name, func(t *testing.T) {
			data, err := Encode(Header{At: at}, rows("a"))
			if err != nil {
				t.Fatal(err)
			}
			h, _, err := Decode(data)
			if err != nil || !h.At.Equal(at) {
				t.Fatalf("At round trip: %v, %v, want %v", h.At, err, at)
			}
		})
	}
}

func TestEncodeRefusesUndecodablePage(t *testing.T) {
	for name, h := range map[string]Header{
		"oversized-empty": {Nonce: strings.Repeat("n", maxPageBytes)},
		"no-batch-index":  {BatchIndex: 1},
		"past-batch-end":  {BatchPages: 2, BatchIndex: 2},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := Encode[Bytes](h, nil)
			if err == nil {
				_, _, derr := Decode(data)
				t.Fatalf("Encode accepted a %d-byte page Decode rejects: %v", len(data), derr)
			}
		})
	}
}

// fuzzSeeds are pages of every shape the codec writes.
func fuzzSeeds(f *testing.F) {
	for _, h := range []Header{{}, {Seq: 7, Nonce: "n"}, {Seq: 1 << 40, Nonce: "batch", At: time.UnixMilli(1234).UTC(), BatchPages: 3, BatchIndex: 2}} {
		for _, records := range [][]Bytes{nil, rows(""), rows("a", "bc", "def")} {
			data, err := Encode(h, records)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}
	f.Add([]byte(pageMagic))
	f.Add([]byte{})
}

// FuzzDecode: no input panics, and a page Decode accepts re-encodes into
// one that decodes to the same header and records.
func FuzzDecode(f *testing.F) {
	fuzzSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		h, records, err := Decode(data)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		// Re-encode with the header's weight on the first record.
		recs := pageRecords(records)
		for i := range recs {
			recs[i].w = 0
		}
		if len(recs) > 0 {
			recs[0].w = h.Weight
		} else {
			h.Weight = 0 // no record to carry it
		}
		again, err := Encode(h, recs)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		h2, records2, err := Decode(again)
		if err != nil || !reflect.DeepEqual(h2, h) || !slices.EqualFunc(records2, records, bytes.Equal) {
			t.Fatalf("round trip: %+v %q, want %+v %q (%v)", h2, records2, h, records, err)
		}
	})
}

// FuzzScan: Scan agrees with Decode on every input.
func FuzzScan(f *testing.F) {
	fuzzSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		var scanned [][]byte
		h, err := Scan(data, func(r []byte) error { scanned = append(scanned, r); return nil })
		hd, records, derr := Decode(data)
		if (err == nil) != (derr == nil) {
			t.Fatalf("Scan %v, Decode %v", err, derr)
		}
		if err == nil && (!reflect.DeepEqual(h, hd) || !slices.EqualFunc(scanned, records, bytes.Equal)) {
			t.Fatal("Scan and Decode disagree")
		}
	})
}

// TestEncodeDecodeRoundTrip: random headers and records survive the codec.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 500 {
		h := Header{Seq: rng.Uint64(), Nonce: string(randomBytes(rng, rng.IntN(40)))}
		if rng.IntN(2) == 0 {
			h.At = time.UnixMilli(rng.Int64N(1 << 45)).UTC()
		}
		if n := rng.Uint64N(4); n > 0 {
			h.BatchPages, h.BatchIndex = n, rng.Uint64N(n)
		}
		records := make([]counted, rng.IntN(20))
		for i := range records {
			records[i] = counted{weighed{randomBytes(rng, rng.IntN(300)), rng.Uint64N(1 << 40)}, rng.IntN(600)}
			h.Weight += records[i].w
			h.Rows += records[i].rows
		}
		h.Records = len(records)
		data, err := Encode(Header{Seq: h.Seq, Nonce: h.Nonce, At: h.At, BatchPages: h.BatchPages, BatchIndex: h.BatchIndex, Records: 99, Weight: 99, Rows: 99}, records)
		if err != nil {
			t.Fatal(err)
		}
		got, decoded, err := Decode(data)
		if err != nil || !reflect.DeepEqual(got, h) || !slices.EqualFunc(decoded, records, func(a []byte, b counted) bool { return bytes.Equal(a, b.Bytes) }) {
			t.Fatalf("round trip of %+v: %+v, %v", h, got, err)
		}
		if sh, err := Scan(data, nil); err != nil || !reflect.DeepEqual(sh, h) {
			t.Fatalf("scan of %+v: %+v, %v", h, sh, err)
		}
		if len(h.Nonce) <= 32 {
			if hh, err := DecodeHeader(data[:min(len(data), headerReadBytes)]); err != nil || !reflect.DeepEqual(hh, h) {
				t.Fatalf("header of %+v: %+v, %v", h, hh, err)
			}
		}
	}
}

// weighed is a record with a weight (Weigher).
type weighed struct {
	Bytes
	w uint64
}

func (r weighed) Weight() uint64 { return r.w }

// counted is a weighed record that stands for rows rows (Counter).
type counted struct {
	weighed
	rows int
}

func (r counted) Rows() int { return r.rows }

// TestHeaderRowsCountOnePerPlainRecord: a record that is not a Counter
// counts one row.
func TestHeaderRowsCountOnePerPlainRecord(t *testing.T) {
	data, err := Encode(Header{Seq: 1}, []Bytes{Bytes("a"), Bytes("b"), Bytes("c")})
	if err != nil {
		t.Fatal(err)
	}
	h, _, err := Decode(data)
	if err != nil || h.Records != 3 || h.Rows != 3 {
		t.Fatalf("plain records: %+v, %v; want 3 records, 3 rows", h, err)
	}
}

// TestV1PagesStillDecode: a page written before the row count (magic
// OWAL\x01, no rows field) decodes, its rows its record count, by Decode,
// Scan and DecodeHeader alike.
func TestV1PagesStillDecode(t *testing.T) {
	recs := [][]byte{[]byte("one"), []byte("two")}
	b := []byte(pageMagicV1)
	b = binary.AppendUvarint(b, 7)                // seq
	b = binary.AppendUvarint(b, uint64(len("n"))) // nonce
	b = append(b, "n"...)
	b = binary.AppendUvarint(b, 1_700_000_000_000) // at
	b = binary.AppendUvarint(b, 0)                 // batch pages
	b = binary.AppendUvarint(b, 0)                 // batch index
	b = binary.AppendUvarint(b, uint64(len(recs))) // records
	b = binary.AppendUvarint(b, 42)                // weight
	for _, r := range recs {
		b = binary.AppendUvarint(b, uint64(len(r)))
		b = append(b, r...)
	}
	b = seal(b)
	want := Header{Seq: 7, Nonce: "n", At: time.UnixMilli(1_700_000_000_000).UTC(), Records: 2, Weight: 42, Rows: 2}
	h, got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(h, want) || len(got) != 2 || string(got[1]) != "two" {
		t.Fatalf("v1 page: %+v %q %v, want %+v", h, got, err, want)
	}
	if sh, err := Scan(b, nil); err != nil || !reflect.DeepEqual(sh, want) {
		t.Fatalf("v1 scan: %+v %v", sh, err)
	}
	if hh, err := DecodeHeader(b[:min(len(b), headerReadBytes)]); err != nil || !reflect.DeepEqual(hh, want) {
		t.Fatalf("v1 header: %+v %v", hh, err)
	}
	// A third version is not a page.
	bad := bytes.Clone(b)
	bad[len(pageMagic)-1] = 3
	if _, _, err := Decode(seal(bad[:len(bad)-4])); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("version 3 page decoded: %v", err)
	}
}

func randomBytes(rng *rand.Rand, n int) Bytes {
	b := make(Bytes, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func BenchmarkDecode(b *testing.B) {
	records := make([]Bytes, 1024)
	for i := range records {
		records[i] = filled(byte(i), 512)
	}
	data, err := Encode(Header{Seq: 1, Nonce: "benchmark"}, records)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}
