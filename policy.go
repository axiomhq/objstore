package objstore

import (
	"context"

	"github.com/axiomhq/objstore/kms"
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

// Config is what Open adds around any backend. Provider-specific settings
// (endpoint, credentials, server-side encryption) live in each provider
// package's own Config.
//
// Reserved key space: with encryption configured, every key under
// ns/<name>/ is encrypted with name's data key, and cmek/<name> holds that
// key's envelope record. Do not store unrelated objects under either
// prefix in a bucket that uses encryption.
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
	// KeyProvider, when set, enables envelope encryption of every object
	// under ns/<name>/ for each name with a key record (see
	// InstallNamespaceKey). It is the same as calling ConfigureCMEK before
	// first use.
	KeyProvider kms.KeyProvider
	// PlaintextKeys reports whether an object read from an encrypted
	// namespace is a deliberate plaintext object, returned as-is and
	// unauthenticated instead of decrypted. It sees the key and the stored
	// bytes. nil = DefaultPlaintextKeys. Whatever it accepts, anyone with
	// write access to the bucket can forge: keep it as narrow as the
	// application's layout allows.
	PlaintextKeys func(key string, data []byte) bool
}

// DefaultPlaintextKeys accepts the plaintext objects RetireNamespaceKey
// leaves behind: ns/<name>/manifest holding a JSON head with state
// "deleted" and a non-empty incarnation (the name-reuse fence), and
// ns/<name>/lease and ns/<name>/compactor holding any valid JSON. Compose
// it to add exemptions of your own.
func DefaultPlaintextKeys(key string, data []byte) bool {
	return plaintextDeletedManifest(key, data) || plaintextFence(key, data)
}
