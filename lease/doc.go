// Package lease is a single-holder lease on one object in a Store: leader
// election, or a single writer, on S3 or any store with conditional PUT.
//
// The lease is one JSON object at a caller-chosen key. PutIfAbsent takes a
// free key, PutIfMatch on the ETag takes over an expired one and renews a
// held one. The owner string names the process; every write attempt carries
// a fresh nonce, so an ambiguous CAS (the PUT landed, the answer was lost)
// is resolved by reading the object back and finding our own nonce, never
// guessed at.
//
// CLOCKS. The object carries a wall-clock expiry; the holder's deadline uses
// its local monotonic clock and starts BEFORE the PUT. A taker waits until
// the persisted expiry plus half its TTL, so non-overlap requires relative
// clock error to stay below that margin: the same TTL across the fleet,
// disciplined clocks. An explicit Release sets the expiry to zero.
//
// Renewal runs every TTL/4 with a TTL/2 attempt timeout. An unresolved
// attempt does not fence a still-valid holder; a changed nonce or an elapsed
// local interval does, and a late successful CAS cannot fill the gap.
//
// A validity check cannot atomically fence a read or a multi-object
// mutation: a pause after the final check can still delay delivery. Durable
// mutations need their own CAS or epoch protocol, not a lease post-check.
package lease
