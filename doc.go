// Package objstore is one object-storage bucket with compare-and-swap:
// conditional PUT (PutIfAbsent, PutIfMatch) and GET (GetIfChanged), the
// ETag with the body, paginated listing and batch delete. Each provider
// is its own package, so a binary links only the SDKs it uses: package
// aws/s3 (AWS S3, and R2, MinIO, Ceph and Hetzner, which speak the S3 API),
// package gcp/gcs (Google Cloud Storage) and package fs, a durable local
// store with chunked write-back. Each builds a Backend and hands it to Open.
//
// Objects are meant to be written once and never mutated except by
// deletion; the conditional writes cover the few that must change.
// ListPrefixes returns the child prefixes under a prefix (S3's delimiter),
// so discovering a thousand top-level names costs one request, not one
// per object.
//
// Config.KeyProvider (or ConfigureCMEK) adds envelope encryption of every
// object under ns/<name>/ with a per-name data key wrapped by a customer
// key (see package kms; the AWS and GCP providers are packages aws/kms
// and gcp/kms).
//
// Package wal is a write-ahead log on a Store, package cache a memory and
// disk cache in front of one, package rangeread a coalescing ranged reader
// over the cache, package lease a single-holder lease on one key.
package objstore
