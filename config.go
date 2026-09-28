package objstore

import (
	"context"
)

// urgentKey marks a context whose requests bypass the pacer: a lease
// heartbeat is one request every few seconds and must never queue behind
// a merge's bulk traffic.
type urgentKey struct{}

// Urgent returns ctx marked as latency-critical (a lease heartbeat, a log
// commit, a manifest swap). Requests under it:
//
//   - skip request pacing (Config.RequestsPerSecond);
//   - skip the write bound (Config.MaxInflightWrites);
//   - on the file backend, are written whole instead of written back in
//     chunks, and so never wait for their own write-back.
func Urgent(ctx context.Context) context.Context { return context.WithValue(ctx, urgentKey{}, true) }

// IsUrgent reports whether ctx was marked Urgent. Backends consult it for
// their own fast paths (the file store writes an urgent object whole).
func IsUrgent(ctx context.Context) bool { v, _ := ctx.Value(urgentKey{}).(bool); return v }

// KMSKeyFunc names the KMS key an object is written with; "" writes it under
// the bucket's own encryption policy. It runs on every Put, PutIfAbsent and
// PutIfMatch, before the write takes a slot.
type KMSKeyFunc func(ctx context.Context, key string) (string, error)

type kmsKey struct{}

// WithKMSKey returns ctx whose writes are encrypted server-side with KMS key
// id: S3 SSE-KMS with that key id, GCS kmsKeyName. On a Store with a
// KMSKeyFunc (WithKMSKeys) the function decides; it can read id with KMSKey.
func WithKMSKey(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, kmsKey{}, id)
}

// KMSKey is the KMS key id a write under ctx carries, "" for none. Backends
// read it; so do backend wrappers (storetest.KMS).
func KMSKey(ctx context.Context) string { v, _ := ctx.Value(kmsKey{}).(string); return v }

// Config is what Open adds around any backend. Provider-specific settings
// (endpoint, credentials, bucket-wide server-side encryption) live in each
// provider package's own Config.
type Config struct {
	// RequestsPerSecond paces every request to the object store; 0 = no
	// pacing. A burst past what the provider serves does not fail, it
	// stalls: at ~900 requests/s against Hetzner Object Storage every
	// in-flight request hung for 13-15 s, the lease renewal among them, and
	// the namespace was fenced. Pacing below that keeps a prefetch burst
	// and the heartbeat both moving.
	RequestsPerSecond float64
	// MaxInflightWrites bounds the object writes and deletes in flight at
	// once, except those marked Urgent (lease heartbeats, log commits,
	// manifest heads); 0 = 16, negative = unbounded. A background job
	// publishes thousands of objects in a burst; without a bound they queue
	// ahead of the write path's own small requests on the store.
	MaxInflightWrites int
}
