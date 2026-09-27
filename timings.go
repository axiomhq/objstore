package objstore

import (
	"context"
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

// LogAttrs is t as slog key-value pairs: sys_<call>_ms and sys_<call>_n
// per Call.
func (t *Timings) LogAttrs() []any {
	out := make([]any, 0, 4*callCount)
	for op := range callCount {
		out = append(out, "sys_"+callNames[op]+"_ms", t.nanos[op].Load()/int64(time.Millisecond), "sys_"+callNames[op]+"_n", t.n[op].Load())
	}
	return out
}
