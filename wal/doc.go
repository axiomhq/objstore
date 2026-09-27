// Package wal is a write-ahead log in object storage: ordered, immutable
// pages sequenced by conditional PUT. A Writer coalesces concurrent Appends
// into one entry per commit interval and bounds what it holds on behalf of
// callers the store has not acked: past the bound Append returns
// ErrOverloaded instead of blocking.
//
// The sequence numbers ARE the catalog: a walk GETs after+1, after+2, ...
// and stops at the first not-found, so nothing lists a prefix. That is
// exact because a writer claims sequentially and advances only on a proven
// outcome: a crashed, fenced or unresolved writer leaves the log short,
// never holey. An ambiguous PUT is resolved by reading the page back and
// comparing the batch nonce.
//
// # Durability
//
// Append returning nil means its records are in pages the store
// acknowledged (or that a read-back proved were ours), in append order,
// and onCommit has applied them: as durable as the store makes a completed
// PUT. ErrLostRace, ErrOverloaded, ErrRecordTooLarge, ErrInvalidRecord and
// ErrWriterClosed mean the records were not and will not be written.
// ErrUnresolved means the outcome is unknown: the records may be durable,
// so recover by replaying the log from the checkpoint rather than by
// appending them again. An Append whose context ends first returns the
// context's error, with the same unknown outcome.
//
// # One writer per prefix
//
// A prefix has at most one live Writer, and the caller enforces it (a
// lease, say). Conditional PUT and the nonce read-back detect a second
// writer (ErrLostRace) rather than prevent one; the floor oracle
// (Writer.SetFloor) is what keeps a stale writer from claiming sequences
// that were already truncated.
//
// Records are opaque. The WAL frames, checksums and pages them; the caller
// encodes rows (Record) and decodes pages (Decode, Scan) into its own type.
package wal
