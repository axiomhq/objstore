// Package objstore is one object-storage bucket with compare-and-swap:
// conditional PUT (PutIfAbsent, PutIfMatch) and GET (GetIfChanged), the
// ETag with the body, paginated listing and batch delete. New picks the
// backend from the endpoint: an S3 URL (AWS, MinIO, any S3-compatible
// service) or file:// for a durable local store with chunked write-back.
//
// Objects are meant to be written once and never mutated except by
// deletion; the conditional writes cover the few that must change.
// ListPrefixes returns the child prefixes under a prefix (S3's delimiter),
// so discovering a thousand top-level names costs one request, not one
// per object.
//
// ConfigureCMEK adds envelope encryption of every object under
// ns/<name>/ with a per-name data key wrapped by a customer key (see
// package kms for the providers).
//
// Package wal is a write-ahead log on a Store, package cache a memory and
// disk cache in front of one, package rangeread a coalescing ranged reader
// over the cache, package lease a single-holder lease on one key.
package objstore
