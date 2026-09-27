package objstore

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
)

// Call is a kind of work the store does for a caller: the file store's system
// calls, and the write gate every backend shares.
type Call int

const (
	CallGate      Call = iota // waiting for a write slot (Config.MaxInflightWrites)
	CallCreate                // creating an object's temp file (and its directory)
	CallWrite                 // write(2) of an object's temp file
	CallWriteBack             // sync_file_range of a chunk (writeBackChunk)
	CallFsync                 // fsync of an object's temp file
	CallLock                  // the key's stripe lock
	CallLink                  // rename(2) or link(2) into place
	CallDirSync               // fsync of a directory after a publish
	CallRead                  // an object read, whole or ranged
	CallList                  // a listing's directory walk
	CallDelete                // unlink(2)
	callCount
)

var callNames = [callCount]string{"gate", "create", "write", "writeback", "fsync", "lock", "link", "dirsync", "read", "list", "delete"}

// String is the Call's short name, as used in Attrs ("gate", "fsync", ...).
func (c Call) String() string {
	if c < 0 || c >= callCount {
		return "Call(" + strconv.Itoa(int(c)) + ")"
	}
	return callNames[c]
}

// Timings sums the wall time the store spent in each Call for the calls made
// under one context (WithTimings), and how many there were: a background job
// can log its own. Concurrent calls add up, so the sum can pass the span's
// wall time. The file store fills every Call; S3 only the gate.
type Timings struct {
	nanos, n [callCount]atomic.Int64
}

type timingsKey struct{}

// WithTimings has the store add the calls made under ctx to t.
func WithTimings(ctx context.Context, t *Timings) context.Context {
	return context.WithValue(ctx, timingsKey{}, t)
}

// TimingsOf returns the Timings ctx carries, or nil. For backends.
func TimingsOf(ctx context.Context) *Timings {
	t, _ := ctx.Value(timingsKey{}).(*Timings)
	return t
}

// Since adds the time from start to op; a nil t records nothing. For
// backends.
func (t *Timings) Since(op Call, start time.Time) {
	if t != nil {
		t.nanos[op].Add(int64(time.Since(start)))
		t.n[op].Add(1)
	}
}

// Attrs is t as slog attributes: sys_<call>_ms and sys_<call>_n per Call.
// A nil t has none.
func (t *Timings) Attrs() []slog.Attr {
	if t == nil {
		return nil
	}
	out := make([]slog.Attr, 0, 2*callCount)
	for op := range callCount {
		out = append(out,
			slog.Int64("sys_"+op.String()+"_ms", t.nanos[op].Load()/int64(time.Millisecond)),
			slog.Int64("sys_"+op.String()+"_n", t.n[op].Load()))
	}
	return out
}
