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
// Records are opaque. The WAL frames, checksums and pages them; the caller
// encodes rows (Record) and decodes pages (Decode, Scan) into its own type.
package wal
