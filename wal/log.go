package wal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/axiomhq/objstore"
)

// ErrFenced means this session definitively lost publication authority.
var ErrFenced = errors.New("wal: session fenced")

const (
	maxLogMetadata = 1 << 20
	maxLogConfig   = 64 << 10
	// Includes the fixed descriptor, cursor, JSON punctuation and bookkeeping.
	descriptorReserve int64 = 1024
	// One record can require one page descriptor. Reserve its JSON, metadata
	// slice, record, call and receipt overhead even for an empty payload.
	recordSummaryReserve int64 = 512
)

// Cursor identifies a logical committed group, not a physical page or owner.
// It is comparable and JSON serializable. The zero value is invalid. JSON
// decoding does not certify ancestry: only Replay against a Snapshot does.
type Cursor struct {
	incarnation string
	sequence    uint64
	hash        string
	bytes       int // bounds the descriptor read before allocation; zero at genesis
}

type cursorJSON struct {
	Format      int
	Incarnation string
	Sequence    uint64
	Hash        string
	Bytes       int
}

// Sequence returns the logical group sequence; genesis has sequence zero.
func (c Cursor) Sequence() uint64 { return c.sequence }

func (c Cursor) MarshalJSON() ([]byte, error) {
	if !c.valid() {
		return nil, fmt.Errorf("%w: invalid cursor", ErrCorrupt)
	}
	return json.Marshal(cursorJSON{Format: 1, Incarnation: c.incarnation, Sequence: c.sequence, Hash: c.hash, Bytes: c.bytes})
}

func (c *Cursor) UnmarshalJSON(p []byte) error {
	var v cursorJSON
	if err := decodeLogJSON(p, &v); err != nil {
		return err
	}
	next := Cursor{incarnation: v.Incarnation, sequence: v.Sequence, hash: v.Hash, bytes: v.Bytes}
	if v.Format != 1 || !next.valid() {
		return fmt.Errorf("%w: invalid cursor", ErrCorrupt)
	}
	*c = next
	return nil
}

func (c Cursor) valid() bool {
	sizeOK := c.sequence == 0 && c.bytes == 0 || c.sequence > 0 && c.bytes > 0 && c.bytes <= maxLogMetadata
	return logHex(c.incarnation, 32) && logHex(c.hash, 64) && sizeOK
}

// Snapshot is a fixed HEAD-authorized upper bound. Only Snapshot and Acquire
// produce valid values, bound to their originating Log handle. It cannot be
// reconstructed from a Cursor or deserialized. Old snapshots remain valid:
// Log never deletes accepted pages or descriptors.
type Snapshot struct {
	log      *Log
	cursor   Cursor
	metadata []byte
}

func (s Snapshot) Cursor() Cursor   { return s.cursor }
func (s Snapshot) Metadata() []byte { return bytes.Clone(s.metadata) }
func (*Snapshot) UnmarshalJSON([]byte) error {
	return errors.New("wal: snapshots cannot be deserialized")
}

// Log is a CAS-owned WAL. One HEAD contains both ownership and the accepted
// tip. Staged objects alone never authorize replay. The prefix must be reserved
// exclusively for this protocol; opening it does not migrate a legacy Writer.
type Log struct {
	store         *objstore.Store
	prefix        string
	configuration []byte
	genesis       Cursor
}

type logHead struct {
	Format        int
	Prefix        string
	Incarnation   string
	Configuration []byte
	Owner         string
	Revision      string
	Tip           Cursor
}

// OpenLog opens or initializes a log. Configuration is opaque, immutable,
// copied, and limited to 64 KiB. Prefix is a nonempty canonical relative path
// ending in '/', at most 512 bytes of valid UTF-8, without NUL, empty, '.' or
// '..' components. Backend path-component limits still apply. No incarnation
// is reused.
func OpenLog(ctx context.Context, store *objstore.Store, prefix string, configuration []byte) (*Log, error) {
	if store == nil || !validLogPrefix(prefix) {
		return nil, fmt.Errorf("wal: invalid log store or prefix: %w", objstore.ErrInvalidKey)
	}
	if len(configuration) > maxLogConfig {
		return nil, ErrRecordTooLarge
	}
	l := &Log{store: store, prefix: prefix, configuration: bytes.Clone(configuration)}
	h, _, err := l.readHead(ctx)
	if errors.Is(err, objstore.ErrNotFound) {
		inc := logToken()
		h = logHead{Format: 1, Prefix: prefix, Incarnation: inc,
			Configuration: l.configuration, Revision: logToken(), Tip: l.genesisFor(inc)}
		p, marshalErr := json.Marshal(h)
		if marshalErr != nil {
			return nil, marshalErr
		}
		// In particular, fs's false,nil retry syncs the existing directory.
		// A readback after an error alone is not a durability acknowledgment.
		for range 3 {
			_, err = store.PutIfAbsent(objstore.Urgent(ctx), l.prefix+"HEAD", p)
			if err == nil || ctx.Err() != nil {
				break
			}
		}
		if err != nil {
			return nil, errors.Join(ErrUnresolved, err)
		}
		h, _, err = l.readHead(ctx)
	}
	if err != nil {
		return nil, err
	}
	l.genesis = l.genesisFor(h.Incarnation)
	return l, nil
}

func (l *Log) Genesis() Cursor { return l.genesis }

func (l *Log) genesisFor(inc string) Cursor {
	return Cursor{incarnation: inc, hash: logHash([]byte("owned-wal-v1\x00" + l.prefix + inc))}
}

func (l *Log) readHead(ctx context.Context) (logHead, string, error) {
	p, tag, err := l.store.GetWithETag(ctx, l.prefix+"HEAD")
	var h logHead
	if err != nil {
		return h, "", err
	}
	if err := decodeLogJSON(p, &h); err != nil {
		return h, "", err
	}
	valid := h.Format == 1 && h.Prefix == l.prefix && logHex(h.Revision, 32) && logHex(h.Incarnation, 32)
	valid = valid && (h.Owner == "" || logHex(h.Owner, 32)) && h.Tip.incarnation == h.Incarnation
	valid = valid && (l.genesis == (Cursor{}) || h.Incarnation == l.genesis.incarnation)
	valid = valid && (h.Tip.sequence != 0 || h.Tip == l.genesisFor(h.Incarnation))
	valid = valid && (h.Owner != "" || h.Tip.sequence == 0) && tag != ""
	if !valid || len(h.Configuration) > maxLogConfig {
		return h, "", fmt.Errorf("%w: invalid owned HEAD", ErrCorrupt)
	}
	if !bytes.Equal(h.Configuration, l.configuration) {
		return h, "", errors.New("wal: log configuration mismatch")
	}
	return h, tag, nil
}

// Snapshot reads HEAD once. Later commits cannot change this upper bound.
// It validates the tip descriptor, but does not certify unread pages exist.
func (l *Log) Snapshot(ctx context.Context) (Snapshot, error) {
	h, _, err := l.readHead(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return l.snapshot(ctx, h.Tip)
}

func (l *Log) snapshot(ctx context.Context, c Cursor) (Snapshot, error) {
	s := Snapshot{log: l, cursor: c}
	if c == l.genesis {
		return s, nil
	}
	d, _, err := l.readDescriptor(ctx, c, math.MaxInt64)
	if err != nil {
		return Snapshot{}, err
	}
	s.metadata = bytes.Clone(d.Metadata[len(d.Metadata)-1])
	return s, nil
}

// SessionOptions bounds retained work. Zero values select a one-second
// collection window, 128 MiB pending memory, 32 MiB encoded batches and 1 MiB
// descriptor reservation. Negative values are invalid. Windows above five
// seconds are allowed. MaxBatchBytes has a 512 MiB hard ceiling; metadata has
// a 1 MiB encoded hard ceiling. Admission conservatively reserves three times
// encoded size (raw, encoding scratch and staged pages), four times metadata size,
// and descriptor/bookkeeping overhead, including for empty records.
type SessionOptions struct {
	CollectionWindow time.Duration
	MaxPendingBytes  int64
	MaxBatchBytes    int64
	MaxMetadataBytes int64
}

func (o SessionOptions) defaults() (SessionOptions, error) {
	if o.CollectionWindow < 0 || o.MaxPendingBytes < 0 || o.MaxBatchBytes < 0 || o.MaxMetadataBytes < 0 {
		return o, errors.New("wal: negative session limit")
	}
	if o.CollectionWindow == 0 {
		o.CollectionWindow = time.Second
	}
	if o.MaxPendingBytes == 0 {
		o.MaxPendingBytes = maxUnackedBytes
	}
	if o.MaxBatchBytes == 0 {
		o.MaxBatchBytes = maxEntryBytes
	}
	if o.MaxMetadataBytes == 0 {
		o.MaxMetadataBytes = maxLogMetadata
	}
	if o.MaxBatchBytes > maxBatchBytes || o.MaxMetadataBytes > maxLogMetadata {
		return o, fmt.Errorf("%w: session limit exceeds hard ceiling", ErrRecordTooLarge)
	}
	return o, nil
}

// Session is one acquisition, never automatically reacquired. Acquire's ctx
// governs its entire lifetime. Enqueue copies inputs before accepting them;
// caller cancellation while waiting never retracts an accepted record.
type Session struct {
	log    *Log
	base   Snapshot
	opts   SessionOptions
	writer *Writer[logRecord]
	// Only the engine's flush goroutine changes head and etag.
	head logHead
	etag string
}

type logRecord struct{ data, metadata []byte }

func (r logRecord) Size() int                           { return len(r.data) }
func (r logRecord) AppendTo(dst []byte) ([]byte, error) { return append(dst, r.data...), nil }

// Acquire replaces ownership with a fresh token, preserving exactly the tip
// read with the CAS ETag. A competing acquisition is returned, never retried
// against refreshed authority. Base is the exact preserved snapshot.
func (l *Log) Acquire(ctx context.Context, options SessionOptions) (*Session, error) {
	o, err := options.defaults()
	if err != nil {
		return nil, err
	}
	h, tag, err := l.readHead(ctx)
	if err != nil {
		return nil, err
	}
	base, err := l.snapshot(ctx, h.Tip)
	if err != nil {
		return nil, err
	}
	h.Owner = logToken()
	tag, terminal, err := l.swapHead(ctx, h, tag)
	if err != nil {
		return nil, err
	}
	if terminal != nil {
		return nil, terminal
	}
	lifetime, cancel := context.WithCancel(ctx)
	s := &Session{log: l, base: base, opts: o, head: h, etag: tag}
	w := &Writer[logRecord]{
		store: l.store, prefix: l.prefix + "owners/" + h.Owner + "/", nextSeq: 1,
		ctx: lifetime, cancel: cancel, attemptTimeout: defaultAttemptTimeout,
		collectionWindow: o.CollectionWindow, commitInterval: o.CollectionWindow,
		lastCommit: time.Now(), kick: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
	}
	w.publish = s.publish
	s.writer = w
	go w.loop()
	return s, nil
}

func (s *Session) Base() Snapshot { return s.base }

// Err reports the known terminal failure under the engine lock. Nil is not
// an authority grant: only the next HEAD CAS can establish acceptance.
func (s *Session) Err() error {
	s.writer.mu.Lock()
	defer s.writer.mu.Unlock()
	return s.writer.terminal
}

// Receipt is a repeatable broadcast completion. It retains no payload. Done
// closes only after the engine has released this batch's copied inputs,
// encoded pages and temporary encoding/publication buffers, on failure too.
type Receipt struct {
	done chan struct{}
	err  error // published by closing done
}

func (r *Receipt) Done() <-chan struct{} { return r.done }
func (r *Receipt) Wait(ctx context.Context) error {
	// Once completed, even a canceled waiter can discover the final result.
	select {
	case <-r.done:
		return r.err
	default:
	}
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Enqueue admits one indivisible record. Capacity rejections are synchronous.
// An expired pending window refuses new admission until engine promotion;
// accepted successors are never published after a final batch failure.
func (s *Session) Enqueue(data, metadata []byte) (*Receipt, error) {
	w := s.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal != nil {
		return nil, w.failedLocked()
	}
	if w.closed {
		return nil, ErrWriterClosed
	}
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > maxPageBytes-entryHeaderReserve || len(metadata) > maxLogMetadata {
		return nil, ErrRecordTooLarge
	}
	framed := framedSize(len(data))
	if framed > maxPageBytes-entryHeaderReserve {
		return nil, ErrRecordTooLarge
	}
	encoded := int64(reservedEntryBytes(framed, 1))
	summary := recordSummaryReserve + int64((len(metadata)+2)/3)*4
	cost := 3*encoded + 4*(summary+descriptorReserve)
	tooLarge := encoded > s.opts.MaxBatchBytes || summary+descriptorReserve > s.opts.MaxMetadataBytes
	if tooLarge || cost > s.opts.MaxPendingBytes {
		return nil, ErrRecordTooLarge
	}
	if !w.pendingSince.IsZero() && !time.Now().Before(w.pendingSince.Add(s.opts.CollectionWindow)) {
		return nil, ErrOverloaded
	}
	pending := int64(reservedEntryBytes(w.pendingBytes, len(w.pending)))
	if pending+encoded > s.opts.MaxBatchBytes || w.pendingMetadata+summary+descriptorReserve > s.opts.MaxMetadataBytes {
		return nil, ErrOverloaded
	}
	used := int64(0)
	if len(w.pending) > 0 {
		used = 3*pending + 4*(w.pendingMetadata+descriptorReserve)
	}
	if w.inflight != nil {
		used += 3*int64(w.inflight.bytes) + 4*(w.inflight.metadataBytes+descriptorReserve)
	}
	if used > s.opts.MaxPendingBytes-cost {
		return nil, ErrOverloaded
	}
	r := &Receipt{done: make(chan struct{})}
	if w.pendingSince.IsZero() {
		w.pendingSince = time.Now()
	}
	w.pending = append(w.pending, logRecord{data: bytes.Clone(data), metadata: bytes.Clone(metadata)})
	w.pendingBytes += framed
	w.pendingMetadata += summary
	w.calls = append(w.calls, call{n: 1, receipt: r})
	select {
	case w.kick <- struct{}{}:
	default:
	}
	return r, nil
}

// Close stops admission and drains without shortening any collection window.
// On deadline it cancels lifetime I/O and returns ctx's error; receipts still
// settle as the backend observes cancellation. Repeated Close may await that
// settlement. A normal return waits for the engine to stop.
func (s *Session) Close(ctx context.Context) error {
	w := s.writer
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.done)
	}
	w.mu.Unlock()
	select {
	case <-w.stopped:
		return s.Err()
	case <-ctx.Done():
		w.cancel()
		return ctx.Err()
	}
}

// swapHead settles ambiguous publication only with a positive CAS, never
// readback alone (fs can expose a rename before directory sync succeeds).
// A successful CAS remains successful after takeover; terminal then prevents
// dependent successors. Ambiguous takeover is unresolved, not fenced success.
func (l *Log) swapHead(ctx context.Context, h logHead, tag string) (string, error, error) {
	var ambiguous error
	for range 3 {
		h.Revision = logToken()
		p, err := json.Marshal(h)
		if err != nil {
			return "", nil, err
		}
		ok, err := l.store.PutIfMatch(objstore.Urgent(ctx), l.prefix+"HEAD", p, tag)
		if err == nil && !ok {
			if ambiguous != nil {
				// The settlement lost authority, but the original publication
				// may have landed. Do not classify its receipt as a fenced write.
				return "", nil, errors.Join(ErrUnresolved, ambiguous)
			}
			return "", nil, ErrFenced
		}
		current, nextTag, readErr := l.readHead(ctx)
		if err == nil && ok {
			if readErr != nil {
				return "", errors.Join(ErrUnresolved, readErr), nil
			}
			if current.Owner != h.Owner || current.Tip != h.Tip {
				return "", ErrFenced, nil
			}
			return nextTag, nil, nil
		}
		ambiguous = errors.Join(ambiguous, err)
		if readErr != nil {
			return "", nil, errors.Join(ErrUnresolved, ambiguous, readErr)
		}
		if current.Owner != h.Owner || current.Tip != h.Tip || current.Revision != h.Revision {
			return "", nil, errors.Join(ErrUnresolved, ambiguous)
		}
		if ctx.Err() != nil {
			return "", nil, errors.Join(ErrUnresolved, ambiguous, ctx.Err())
		}
		tag = nextTag
	}
	return "", nil, errors.Join(ErrUnresolved, ambiguous)
}

type logPage struct {
	Hash    string
	Bytes   int
	Records int
}

type logDescriptor struct {
	Format    int
	Previous  Cursor
	Sequence  uint64
	Owner     string
	Nonce     string
	First     uint64
	At        int64
	ReadLimit int64
	Pages     []logPage
	Metadata  [][]byte
}

func (s *Session) publish(ctx context.Context, b *batch[logRecord], first uint64) error {
	if s.head.Tip.sequence == math.MaxUint64 {
		return ErrWriterFailed
	}
	d := logDescriptor{Format: 1, Previous: s.head.Tip, Sequence: s.head.Tip.sequence + 1,
		Owner: s.head.Owner, Nonce: b.nonce, First: first, At: b.at.UnixMilli(), ReadLimit: 2 * s.opts.MaxBatchBytes,
		Pages: make([]logPage, len(b.pages)), Metadata: make([][]byte, len(b.records))}
	for i, p := range b.pages {
		h, err := Scan(p, nil)
		if err != nil {
			return err
		}
		d.Pages[i] = logPage{Hash: logHash(p), Bytes: len(p), Records: h.Records}
	}
	for i, r := range b.records {
		d.Metadata[i] = r.metadata
	}
	p, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if int64(len(p)) > s.opts.MaxMetadataBytes || len(p) > maxLogMetadata {
		return ErrRecordTooLarge
	}
	c := Cursor{incarnation: s.head.Incarnation, sequence: d.Sequence, hash: logHash(p), bytes: len(p)}
	if err := putLogImmutable(ctx, s.log.store, s.log.prefix+"groups/"+c.hash, p); err != nil {
		return err
	}
	h := s.head
	h.Tip = c
	tag, terminal, err := s.log.swapHead(ctx, h, s.etag)
	if err != nil {
		return err
	}
	s.head, s.etag = h, tag
	if terminal != nil {
		s.writer.mu.Lock()
		s.writer.terminal = terminal
		s.writer.mu.Unlock()
	}
	return nil
}

func (w *Writer[R]) commitOwned(b *batch[R]) error {
	started := time.Now() // logical start, not encoding or storage completion
	pages, kept, err := splitBatch(w.nextSeq, b)
	if err != nil {
		return err
	}
	if kept != nil {
		return fmt.Errorf("%w: owned records cannot be filtered", ErrInvalidRecord)
	}
	b.pages = pages
	ctx, cancel := context.WithTimeout(w.ctx, w.attemptTimeout)
	defer cancel()
	for i, p := range pages {
		if err := putLogImmutable(ctx, w.store, Key(w.prefix, w.nextSeq+uint64(i)), p); err != nil {
			return err
		}
	}
	if err := w.publish(ctx, b, w.nextSeq); err != nil {
		return err
	}
	w.nextSeq += uint64(len(pages))
	w.mu.Lock()
	w.lastCommit = started
	w.mu.Unlock()
	return nil
}

// finishOwned releases payloads before memory credit, then broadcasts. Every
// post-admission failure terminates the stream, including encoding/staging.
func (w *Writer[R]) finishOwned(b *batch[R], err error) {
	w.mu.Lock()
	if err != nil && w.terminal == nil {
		w.terminal = err
	}
	// commitOwned and publish have returned: their scratch/page/descriptor
	// locals are gone. No onCommit callback or history cache retains inputs.
	// Clear the last owning slices before removing admission credit and,
	// strictly after that, closing any Receipt.Done channel in deliver.
	b.records, b.pages = nil, nil
	w.inflight = nil
	var queued []call
	var failed error
	if w.terminal != nil {
		queued, failed = w.calls, w.failedLocked()
		w.pending, w.pendingBytes, w.calls, w.pendingSince = nil, 0, nil, time.Time{}
		w.pendingMetadata = 0
	}
	w.mu.Unlock()
	deliver(b.calls, err)
	deliver(queued, failed)
}

func (w *Writer[R]) abortOwned(err error) {
	w.mu.Lock()
	if w.terminal == nil {
		w.terminal = err
	}
	calls := w.calls
	w.pending, w.pendingBytes, w.calls, w.pendingSince = nil, 0, nil, time.Time{}
	w.pendingMetadata = 0
	w.mu.Unlock()
	deliver(calls, err)
}

func putLogImmutable(ctx context.Context, store *objstore.Store, key string, p []byte) error {
	var err error
	for range 3 {
		var ok bool
		ok, err = store.PutIfAbsent(objstore.Urgent(ctx), key, p)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			continue
		}
		if ok {
			return nil
		}
		got, err := store.Get(ctx, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, p) {
			return fmt.Errorf("%w: immutable object collision", ErrCorrupt)
		}
		return nil
	}
	return err
}

// Batch is a validated committed summary. Read lazily verifies every page;
// successful summary replay alone does not certify unread pages exist.
type Batch struct {
	log        *Log
	cursor     Cursor
	descriptor *logDescriptor
}

func (b Batch) Cursor() Cursor { return b.cursor }
func (b Batch) Metadata() [][]byte {
	if b.descriptor == nil {
		return nil
	}
	out := make([][]byte, len(b.descriptor.Metadata))
	for i, m := range b.descriptor.Metadata {
		out[i] = bytes.Clone(m)
	}
	return out
}

// Read returns all ordered raw payloads or none. Hash, CRC, nonce, sequence,
// page ordering, timestamp, record count and the descriptor's read budget are
// checked before any payloads escape. Reads have no retained cache.
func (b Batch) Read(ctx context.Context) ([][]byte, error) {
	if b.log == nil || b.descriptor == nil {
		return nil, fmt.Errorf("%w: invalid batch", ErrCorrupt)
	}
	d := b.descriptor
	out := make([][]byte, 0, len(d.Metadata))
	var used int64
	for i, page := range d.Pages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		used += int64(page.Bytes) + 24*int64(page.Records)
		if used > d.ReadLimit {
			return nil, ErrRecordTooLarge
		}
		key := Key(b.log.prefix+"owners/"+d.Owner+"/", d.First+uint64(i))
		// The accepted descriptor fixes the exact range; a corrupt object
		// cannot force an unbounded Get allocation.
		p, err := b.log.store.GetRange(ctx, key, 0, int64(page.Bytes))
		if err != nil {
			return nil, fmt.Errorf("%w: accepted page: %w", ErrCorrupt, err)
		}
		if len(p) != page.Bytes || logHash(p) != page.Hash {
			return nil, fmt.Errorf("%w: page hash", ErrCorrupt)
		}
		// Bound record descriptors by the accepted summary before scanning;
		// Decode would allocate a second slice from the page's claimed count.
		h, err := Scan(p, func(record []byte) error {
			if len(out) == cap(out) {
				return fmt.Errorf("%w: excess accepted records", ErrCorrupt)
			}
			out = append(out, record)
			return nil
		})
		if err != nil {
			return nil, err
		}
		valid := h.Seq == d.First+uint64(i) && h.Nonce == d.Nonce && h.At.UnixMilli() == d.At
		valid = valid && h.BatchIndex == uint64(i) && h.BatchPages == uint64(len(d.Pages))
		valid = valid && h.Records == page.Records && h.Rows == page.Records && h.Weight == 0
		if !valid {
			return nil, fmt.Errorf("%w: accepted page header", ErrCorrupt)
		}
	}
	if len(out) != len(d.Metadata) {
		return nil, fmt.Errorf("%w: accepted record count", ErrCorrupt)
	}
	return out, nil
}

// Replay validates and bounds the entire backward descriptor chain before
// invoking visit in forward order. from must be an exact ancestor, not merely
// a smaller sequence. maxSummaryBytes reserves sixteen times encoded descriptors
// (including empty-record slice overhead) plus per-batch overhead. Exceeding it
// returns ErrRecordTooLarge with zero callbacks. through must originate from
// this Log's Snapshot or Acquire.
func (l *Log) Replay(ctx context.Context, from Cursor, through Snapshot, maxSummaryBytes int64, visit func(Batch) error) error {
	valid := through.log == l && from.valid() && from.incarnation == l.genesis.incarnation
	valid = valid && through.cursor.valid() && from.sequence <= through.cursor.sequence
	if !valid || visit == nil {
		return fmt.Errorf("%w: invalid replay bounds", ErrCorrupt)
	}
	if maxSummaryBytes < 0 {
		return ErrRecordTooLarge
	}
	var chain []Batch
	c := through.cursor
	var used int64
	for c != from {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.sequence <= from.sequence {
			return fmt.Errorf("%w: cursor is not committed ancestry", ErrCorrupt)
		}
		d, cost, err := l.readDescriptor(ctx, c, maxSummaryBytes-used)
		if err != nil {
			return err
		}
		used += cost
		chain = append(chain, Batch{log: l, cursor: c, descriptor: d})
		c = d.Previous
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(chain[i]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (l *Log) readDescriptor(ctx context.Context, c Cursor, budget int64) (*logDescriptor, int64, error) {
	if !c.valid() || c.sequence == 0 {
		return nil, 0, fmt.Errorf("%w: invalid descriptor cursor", ErrCorrupt)
	}
	cost := 16*int64(c.bytes) + descriptorReserve
	if cost > budget {
		return nil, 0, ErrRecordTooLarge
	}
	p, err := l.store.GetRange(ctx, l.prefix+"groups/"+c.hash, 0, int64(c.bytes))
	if err != nil {
		return nil, 0, fmt.Errorf("%w: accepted descriptor: %w", ErrCorrupt, err)
	}
	if len(p) != c.bytes || logHash(p) != c.hash {
		return nil, 0, fmt.Errorf("%w: descriptor hash", ErrCorrupt)
	}
	var d logDescriptor
	if err := decodeLogJSON(p, &d); err != nil {
		return nil, 0, err
	}
	valid := d.Format == 1 && d.Sequence == c.sequence && c.sequence > 0
	valid = valid && d.Previous.incarnation == l.genesis.incarnation && d.Previous.sequence == c.sequence-1
	valid = valid && (d.Previous.sequence != 0 || d.Previous == l.genesis)
	valid = valid && logHex(d.Owner, 32) && logHex(d.Nonce, 32) && d.First > 0 && d.At > 0
	valid = valid && len(d.Pages) > 0 && len(d.Metadata) > 0 && len(d.Pages) <= len(d.Metadata)
	valid = valid && uint64(len(d.Pages)) <= math.MaxUint64-d.First && d.ReadLimit > 0 && d.ReadLimit <= 2*maxBatchBytes
	if !valid {
		return nil, 0, fmt.Errorf("%w: invalid descriptor", ErrCorrupt)
	}
	var records, memory int64
	for _, p := range d.Pages {
		if !logHex(p.Hash, 64) || p.Bytes <= 0 || p.Bytes > maxPageBytes || p.Records <= 0 || p.Records > maxPageBytes {
			return nil, 0, fmt.Errorf("%w: invalid page descriptor", ErrCorrupt)
		}
		records += int64(p.Records)
		memory += int64(p.Bytes) + 24*int64(p.Records)
	}
	if records != int64(len(d.Metadata)) || memory > d.ReadLimit {
		return nil, 0, fmt.Errorf("%w: invalid descriptor budget", ErrCorrupt)
	}
	return &d, cost, nil
}

func decodeLogJSON(p []byte, out any) error {
	if len(p) > maxLogMetadata {
		return ErrRecordTooLarge
	}
	d := json.NewDecoder(bytes.NewReader(p))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return fmt.Errorf("%w: trailing metadata", ErrCorrupt)
	}
	return nil
}

func logToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func logHash(p []byte) string { h := sha256.Sum256(p); return hex.EncodeToString(h[:]) }
func logHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validLogPrefix(prefix string) bool {
	if len(prefix) == 0 || len(prefix) > 512 || !strings.HasSuffix(prefix, "/") {
		return false
	}
	if !utf8.ValidString(prefix) || strings.ContainsRune(prefix, 0) {
		return false
	}
	for _, part := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
