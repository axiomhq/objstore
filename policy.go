package objstore

import "context"

// urgentKey marks a context whose requests bypass the pacer: a lease
// heartbeat is one request every few seconds and must never queue behind
// a merge's bulk traffic.
type urgentKey struct{}

// Urgent returns ctx marked to bypass request pacing and the write bound.
func Urgent(ctx context.Context) context.Context { return context.WithValue(ctx, urgentKey{}, true) }

// IsUrgent reports whether ctx was marked Urgent. Backends consult it for
// their own fast paths (the file store writes an urgent object whole).
func IsUrgent(ctx context.Context) bool { v, _ := ctx.Value(urgentKey{}).(bool); return v }

// Config is what Open adds around any backend. Provider-specific settings
// (endpoint, credentials, server-side encryption) live in each provider
// package's own Config.
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
	// manifest heads); 0 = 16. A background job publishes thousands of
	// objects in a burst; without a bound they queue ahead of the write
	// path's own small requests on the store.
	MaxInflightWrites int
}
