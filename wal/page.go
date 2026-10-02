package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"time"
	"unsafe"
)

const pageMagic = "OWAL\x01"

// maxPageBytes is the codec ceiling: no page, however it was cut, is larger.
const maxPageBytes = 65 << 20

// ErrCorrupt marks a page or a log that is not what the WAL wrote: a bad
// checksum or framing, a sequence that disagrees with its key, a page
// missing inside a bounded range.
var ErrCorrupt = errors.New("wal: corrupt")

// Record is one row as the WAL writes it. The WAL frames and checksums the
// bytes and never looks inside them. Size and AppendTo must not block: the
// Writer calls them on its flush goroutine, outside the attempt timeout.
type Record interface {
	// Size is the exact number of bytes AppendTo appends.
	Size() int
	// AppendTo appends the record's bytes to b.
	AppendTo(b []byte) ([]byte, error)
}

// Weigher is a Record with a weight of the caller's choosing, a logical
// size say. A page's header carries the sum over its records (Header.Weight),
// so a reader can account a page from its header alone (WalkHeaders).
// Records that are not Weighers weigh zero. A page whose sum overflows
// uint64 is ErrInvalidRecord. A Writer keeps each batch's sum within uint64:
// an Append that would overflow it fails alone, with ErrInvalidRecord.
// Like Size and AppendTo, Weight must not block.
type Weigher interface {
	Weight() uint64
}

func weightOf[R Record](records []R) (uint64, error) {
	var w uint64
	for _, r := range records {
		if x, ok := any(r).(Weigher); ok {
			n := x.Weight()
			if n > math.MaxUint64-w {
				return math.MaxUint64, fmt.Errorf("%w: page weight overflows uint64", ErrInvalidRecord)
			}
			w += n
		}
	}
	return w, nil
}

// Bytes is a Record that is already encoded.
type Bytes []byte

// Size is len(b).
func (b Bytes) Size() int { return len(b) }

// AppendTo appends b to dst.
func (b Bytes) AppendTo(dst []byte) ([]byte, error) { return append(dst, b...), nil }

// Header is what a page says about itself.
type Header struct {
	Seq   uint64
	Nonce string // the writing batch's identity; resolves ambiguous conditional PUTs
	// At is when the batch was first attempted, millisecond precision.
	// Zero is unset; otherwise UnixMilli must be positive, since the format
	// reserves millisecond 0 for unset.
	At time.Time
	// BatchPages is the page count of a batch; BatchIndex is this page's
	// position in it. A single-page entry has BatchPages 0 (Encode's
	// default) or 1 (what a Writer writes): walks treat the two alike.
	BatchPages, BatchIndex uint64
	// Records is the page's record count and Weight the sum of their
	// weights (Weigher). A read fills them; Encode and the Writer compute
	// them from the records and ignore what the caller set.
	Records int
	Weight  uint64
}

// Encode writes one page: magic, sequence, nonce, commit time (unix
// milliseconds), batch page count and index, record count, record weight
// (Weigher), each record
// length-prefixed, CRC32C. Encoding is deterministic, so retries of the
// same batch write identical bytes: fix At once per batch.
func Encode[R Record](h Header, records []R) ([]byte, error) {
	if len(h.Nonce) > maxPageBytes {
		return nil, fmt.Errorf("wal: page exceeds size limit")
	}
	size := headerReserve(len(h.Nonce))
	for _, r := range records {
		n := r.Size()
		if err := checkRecordSize(n); err != nil {
			return nil, err
		}
		if size += framedSize(n); size > maxPageBytes+binary.MaxVarintLen64*6 {
			return nil, fmt.Errorf("wal: page exceeds size limit")
		}
	}
	weight, err := weightOf(records)
	if err != nil {
		return nil, err
	}
	b, err := appendHeader(make([]byte, 0, size), h, len(records), weight)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		if b, err = appendRecord(b, r); err != nil {
			return nil, err
		}
	}
	if len(b) > maxPageBytes-4 {
		return nil, fmt.Errorf("wal: page exceeds size limit")
	}
	return seal(b), nil
}

func appendHeader(b []byte, h Header, records int, weight uint64) ([]byte, error) {
	if !validBatch(h.BatchPages, h.BatchIndex) || records < 0 || records > maxPageBytes {
		return nil, fmt.Errorf("wal: invalid page header")
	}
	b = append(b, pageMagic...)
	b = binary.AppendUvarint(b, h.Seq)
	b = binary.AppendUvarint(b, uint64(len(h.Nonce)))
	b = append(b, h.Nonce...)
	var at uint64
	if !h.At.IsZero() {
		if h.At.UnixMilli() < 0 {
			return nil, fmt.Errorf("wal: commit time before the epoch")
		}
		if h.At.UnixMilli() == 0 {
			return nil, fmt.Errorf("wal: commit millisecond 0 is reserved for unset")
		}
		at = uint64(h.At.UnixMilli())
	}
	b = binary.AppendUvarint(b, at)
	b = binary.AppendUvarint(b, h.BatchPages)
	b = binary.AppendUvarint(b, h.BatchIndex)
	b = binary.AppendUvarint(b, uint64(records))
	b = binary.AppendUvarint(b, weight)
	return b, nil
}

// headerReserve bounds a page header's size for a nonce of n bytes, CRC
// included.
func headerReserve(n int) int { return len(pageMagic) + 7*binary.MaxVarintLen64 + n + 4 }

func appendRecord[R Record](b []byte, r R) ([]byte, error) {
	n := r.Size()
	if err := checkRecordSize(n); err != nil {
		return nil, err
	}
	b = binary.AppendUvarint(b, uint64(n))
	start := len(b)
	b, err := r.AppendTo(b)
	if err != nil {
		return nil, err
	}
	if len(b)-start != n {
		return nil, fmt.Errorf("wal: record wrote %d bytes, Size said %d", len(b)-start, n)
	}
	return b, nil
}

// checkRecordSize bounds n before framing arithmetic can overflow.
func checkRecordSize(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: Size %d is negative", ErrInvalidRecord, n)
	}
	if n > maxPageBytes {
		return ErrRecordTooLarge
	}
	return nil
}

// framedSize is a record of n bytes with its length prefix.
func framedSize(n int) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], uint64(n)) + n
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// seal appends the page's CRC32C.
func seal(b []byte) []byte {
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// Decode checks a page and returns its header and records. The records
// alias data.
func Decode(data []byte) (Header, [][]byte, error) {
	var records [][]byte
	h, err := scan(data, func(n int) { records = make([][]byte, 0, n) }, func(r []byte) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		return Header{}, nil, err
	}
	return h, records, nil
}

// Scan checks a page and hands visit each record in order, aliasing data.
// visit may be nil: Scan then only validates the page and reads its header.
func Scan(data []byte, visit func(record []byte) error) (Header, error) {
	return scan(data, nil, visit)
}

func scan(data []byte, count func(int), visit func([]byte) error) (Header, error) {
	bad := func() (Header, error) { return Header{}, fmt.Errorf("%w: invalid WAL page", ErrCorrupt) }
	if len(data) > maxPageBytes || len(data) < len(pageMagic)+4 {
		return bad()
	}
	end := len(data) - 4
	if crc32.Checksum(data[:end], castagnoli) != binary.LittleEndian.Uint32(data[end:]) {
		return bad()
	}
	r := pageReader{data: data[:end]}
	h, ok := r.header()
	if !ok || h.Records > len(r.data) { // every record has at least its length prefix
		return bad()
	}
	if count != nil {
		count(h.Records)
	}
	for range h.Records {
		rec := r.blob()
		if r.err {
			return bad()
		}
		if visit != nil {
			if err := visit(rec); err != nil {
				return Header{}, err
			}
		}
	}
	if r.err || len(r.data) != 0 {
		return bad()
	}
	return h, nil
}

// DecodeHeader reads the header at the start of a page, or of a prefix of
// one long enough to hold it. Nothing checks it against the page's CRC,
// which covers the whole page: a reader that trusts a header alone trusts
// the store to return what the writer's acknowledged PUT stored.
func DecodeHeader(prefix []byte) (Header, error) {
	r := pageReader{data: prefix}
	h, ok := r.header()
	if !ok {
		return Header{}, fmt.Errorf("%w: invalid WAL page header", ErrCorrupt)
	}
	return h, nil
}

// pageMemoryBytes covers wire bytes, the nonce copy and Decode's record
// descriptors. Non-WAL decoders keep the four-times-wire budget.
func pageMemoryBytes(data []byte) int64 {
	n := int64(len(data))
	r := pageReader{data: data}
	h, _, ok := r.headerFields()
	if !ok {
		return 4 * n
	}
	return max(4*n, 2*n+int64(h.Records)*int64(unsafe.Sizeof([]byte{})))
}

// header reads the magic and header fields; false if they are malformed
// or run past r's data.
func (r *pageReader) header() (Header, bool) {
	h, nonce, ok := r.headerFields()
	if ok {
		h.Nonce = string(nonce)
	}
	return h, ok
}

// headerFields leaves the nonce aliased, so budgeting needs no allocation.
func (r *pageReader) headerFields() (Header, []byte, bool) {
	var h Header
	if len(r.data) < len(pageMagic) || string(r.data[:len(pageMagic)]) != pageMagic {
		return Header{}, nil, false
	}
	r.data = r.data[len(pageMagic):]
	h.Seq = r.number()
	nonce := r.blob()
	if at := r.number(); at > math.MaxInt64 {
		return Header{}, nil, false
	} else if at != 0 {
		h.At = time.UnixMilli(int64(at)).UTC()
	}
	h.BatchPages, h.BatchIndex = r.number(), r.number()
	if !validBatch(h.BatchPages, h.BatchIndex) {
		return Header{}, nil, false
	}
	n := r.number()
	h.Weight = r.number()
	if r.err || n > maxPageBytes {
		return Header{}, nil, false
	}
	h.Records = int(n)
	return h, nonce, true
}

func validBatch(pages, index uint64) bool {
	return (pages == 0 && index == 0) || index < pages
}

type pageReader struct {
	data []byte
	err  bool
}

func (r *pageReader) number() uint64 {
	v, n := binary.Uvarint(r.data)
	if n <= 0 {
		r.err = true
		return 0
	}
	r.data = r.data[n:]
	return v
}

func (r *pageReader) blob() []byte {
	n := r.number()
	if r.err || n > uint64(len(r.data)) {
		r.err = true
		return nil
	}
	v := r.data[:n]
	r.data = r.data[n:]
	return v
}
