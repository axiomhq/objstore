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
// Store.WithKMSKeys names a KMS key per object; S3 (SSE-KMS) and GCS
// (kmsKeyName) encrypt it under that key.
//
// Package wal is a write-ahead log on a Store, package lease a
// single-holder lease on one key. Caching reads is the caller's business.
package objstore
