package objstore

import (
	"context"
	"time"

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
	//
	// Cost in a namespace without a key record: every write and every range
	// read first reads the record (one GET, shared by concurrent callers of
	// the namespace), because trusting a cached "no record" there could
	// store plaintext in, or return ciphertext from, a namespace another
	// process has just keyed. Whole-object reads trust a cached "no record"
	// for one second (they re-check what they read).
	KeyProvider kms.KeyProvider
	// KeyRefreshInterval ties access and rotation probes for keys whose
	// provider is lease-cadenced (kms.LeaseCadencer, AWS KMS) to the lease
	// heartbeat cadence; 0 = no such probes. SetCMEKRefreshInterval sets it
	// after Open. It holds with or without KeyProvider, so a later
	// ConfigureCMEK uses it.
	KeyRefreshInterval time.Duration
	// AcceptPlaintext decides whether bytes read from an encrypted
	// namespace that do NOT carry the encrypted-object header are a
	// deliberate plaintext object, returned as-is and unauthenticated. It
	// sees the key and the stored bytes. Bytes carrying the header are
	// always decrypted, whatever it says; bytes it refuses fail to decrypt.
	// GetRange never consults it: a range of an encrypted namespace is
	// always decrypted. nil = DefaultAcceptPlaintext. Whatever it accepts,
	// anyone with write access to the bucket can forge: keep it as narrow
	// as the application's layout allows.
	AcceptPlaintext func(key string, data []byte) bool
}

// DefaultAcceptPlaintext accepts the plaintext objects RetireNamespaceKey
// leaves behind: ns/<name>/manifest holding a JSON head with state
// "deleted" and a non-empty incarnation (the name-reuse fence), and
// ns/<name>/lease and ns/<name>/compactor holding any valid JSON. Compose
// it to add exemptions of your own.
func DefaultAcceptPlaintext(key string, data []byte) bool {
	return plaintextDeletedManifest(key, data) || plaintextFence(key, data)
}
