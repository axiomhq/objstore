package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

const pageMagic = "OWAL\x01"

// maxPageBytes is the codec ceiling: no page, however it was cut, is larger.
const maxPageBytes = 65 << 20

// ErrCorrupt marks a page or a log that is not what the WAL wrote: a bad
// checksum or framing, a sequence that disagrees with its key, a page
// missing inside a bounded range.
var ErrCorrupt = errors.New("wal: corrupt")

// Record is one row as the WAL writes it. The WAL frames and checksums the
// bytes and never looks inside them.
type Record interface {
	// Size is the exact number of bytes AppendTo appends.
	Size() int
	// AppendTo appends the record's bytes to b.
	AppendTo(b []byte) ([]byte, error)
}

// Bytes is a Record that is already encoded.
type Bytes []byte

func (b Bytes) Size() int                           { return len(b) }
func (b Bytes) AppendTo(dst []byte) ([]byte, error) { return append(dst, b...), nil }

// Header is what a page says about itself.
type Header struct {
	Seq   uint64
	Nonce string    // the writing batch's identity; resolves ambiguous conditional PUTs
	At    time.Time // when the batch was first attempted, millisecond precision
	// BatchPages is the page count of a multi-page batch, zero for a
	// single-page entry; BatchIndex is this page's position in it.
	BatchPages, BatchIndex uint64
}

// Encode writes one page: magic, sequence, nonce, commit time (unix
// milliseconds), batch page count and index, record count, each record
// length-prefixed, CRC32C. Encoding is deterministic, so retries of the
// same batch write identical bytes: fix At once per batch.
func Encode[R Record](h Header, records []R) ([]byte, error) {
	b, err := appendHeader(nil, h, len(records))
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		if b, err = appendRecord(b, r); err != nil {
			return nil, err
		}
		if len(b) > maxPageBytes-4 {
			return nil, fmt.Errorf("wal: page exceeds size limit")
		}
	}
	return seal(b), nil
}

func appendHeader(b []byte, h Header, records int) ([]byte, error) {
	b = append(b, pageMagic...)
	b = binary.AppendUvarint(b, h.Seq)
	b = binary.AppendUvarint(b, uint64(len(h.Nonce)))
	b = append(b, h.Nonce...)
	var at uint64
	if !h.At.IsZero() {
		if h.At.UnixMilli() < 0 {
			return nil, fmt.Errorf("wal: commit time before the epoch")
		}
		at = uint64(h.At.UnixMilli())
	}
	b = binary.AppendUvarint(b, at)
	b = binary.AppendUvarint(b, h.BatchPages)
	b = binary.AppendUvarint(b, h.BatchIndex)
	b = binary.AppendUvarint(b, uint64(records))
	return b, nil
}

// headerReserve bounds a page header's size for a nonce of n bytes, CRC
// included.
func headerReserve(n int) int { return len(pageMagic) + 6*binary.MaxVarintLen64 + n + 4 }

func appendRecord[R Record](b []byte, r R) ([]byte, error) {
	n := r.Size()
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

// framedSize is a record of n bytes with its length prefix.
func framedSize(n int) int { return uvarintSize(n) + n }

func uvarintSize(n int) int {
	size := 1
	for n >= 0x80 {
		n >>= 7
		size++
	}
	return size
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
	var h Header
	bad := func() (Header, error) { return Header{}, fmt.Errorf("%w: invalid WAL page", ErrCorrupt) }
	if len(data) > maxPageBytes || len(data) < len(pageMagic)+4 || string(data[:len(pageMagic)]) != pageMagic {
		return bad()
	}
	end := len(data) - 4
	if crc32.Checksum(data[:end], castagnoli) != binary.LittleEndian.Uint32(data[end:]) {
		return bad()
	}
	r := pageReader{data: data[len(pageMagic):end]}
	h.Seq = r.number()
	h.Nonce = string(r.blob())
	if at := r.number(); at > math.MaxInt64 {
		return bad()
	} else if at != 0 {
		h.At = time.UnixMilli(int64(at)).UTC()
	}
	h.BatchPages, h.BatchIndex = r.number(), r.number()
	if (h.BatchPages == 0 && h.BatchIndex != 0) || (h.BatchPages != 0 && h.BatchIndex >= h.BatchPages) {
		return bad()
	}
	n := r.count(1) // every record has at least its length prefix
	if r.err {
		return bad()
	}
	if count != nil {
		count(n)
	}
	for range n {
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

func (r *pageReader) count(width int) int {
	n := r.number()
	if r.err || n > uint64(len(r.data)/width) {
		r.err = true
		return 0
	}
	return int(n)
}

func (r *pageReader) take(n int) []byte {
	if r.err || n > len(r.data) {
		r.err = true
		return nil
	}
	v := r.data[:n]
	r.data = r.data[n:]
	return v
}

func (r *pageReader) blob() []byte { return r.take(r.count(1)) }
