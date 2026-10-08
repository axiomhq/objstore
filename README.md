# objstore

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/axiomhq/objstore)
[![CI](https://github.com/axiomhq/objstore/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/axiomhq/objstore/actions/workflows/ci.yml)

Object storage with compare-and-swap for Go. Supports S3, Google Cloud Storage,
and local files, with packages for write-ahead logs, leases, and cached reads.

[Quick start](#quick-start) · [Providers](#providers) · [Conditional writes](#conditional-writes) · [Leases](#leases) · [Tests](#tests)

## Quick start

Requires **Go 1.26 or later**. This example uses local files; no cloud account needed.

1. In a Go module, install:

   ```sh
   go get github.com/axiomhq/objstore
   ```

2. Save as `main.go`:

   ```go
   package main

   import (
       "context"
       "fmt"
       "log"

       "github.com/axiomhq/objstore"
       "github.com/axiomhq/objstore/fs"
   )

   func main() {
       ctx := context.Background()
       s := fs.Open("./data", "example", objstore.Config{})
       defer s.Close()

       if err := s.EnsureBucket(ctx); err != nil {
           log.Fatal(err)
       }
       if err := s.CheckConditionalWrites(ctx); err != nil {
           log.Fatal(err)
       }
       created, err := s.PutIfAbsent(ctx, "greeting", []byte("hello"))
       if err != nil {
           log.Fatal(err)
       }
       fmt.Println("created:", created)
   }
   ```

3. Run `go run .`. It prints `created: true` on the first run and `created: false`
   on later runs. The object stays in `./data/example`.

The file provider requires Unix with `flock`; Solaris and AIX are unsupported.

## Providers

Import only the provider you need. Each package has an `Open` function that
returns a `*objstore.Store`.

| Package | Storage |
| --- | --- |
| [aws/s3][s3] | AWS S3 and S3-compatible stores: R2, MinIO, Ceph, Hetzner |
| [gcp/gcs][gcs] | Google Cloud Storage |
| [fs][fs] | Local directory; writes are fsynced before acknowledgment |

```go
s, err := s3.Open(ctx, s3.Config{
	Endpoint: "https://s3.us-east-1.amazonaws.com",
	Bucket:   "my-bucket",
}, objstore.Config{})
if err != nil {
	return err
}
defer s.Close()
```

For GCS, use `gcs.Open(ctx, gcs.Config{Bucket: "my-bucket"}, objstore.Config{})`.
For R2, set the S3 endpoint to `https://<account-id>.r2.cloudflarestorage.com`
and `AWS_REGION=auto`. Custom backends use `objstore.Open(backend, cfg)`.

**Before writing:** call `s.EnsureBucket(ctx)` if the bucket may not exist, then
`s.CheckConditionalWrites(ctx)`. Check both errors. The probe creates a temporary
object, tries to create it again, and deletes it. It rejects stores that ignore
`If-None-Match` and would let competing writers both succeed.

## Conditional writes

Use `PutIfAbsent` to create a key once. Use `GetWithETag` and `PutIfMatch` to
update an existing object only if it has not changed:

```go
for {
	body, etag, err := s.GetWithETag(ctx, "manifest")
	if err != nil {
		return err
	}
	ok, err := s.PutIfMatch(ctx, "manifest", update(body), etag)
	switch {
	case errors.Is(err, objstore.ErrConflict):
		continue // unresolved conditional write; read again
	case err != nil:
		return err
	case !ok:
		continue // object changed; read again
	}
	return nil
}
```

`update` is your function for building the replacement body.

| Result | Meaning |
| --- | --- |
| `true, nil` | Write succeeded |
| `false, nil` | Condition failed: key exists (`PutIfAbsent`) or ETag no longer matches (`PutIfMatch`) |
| `ErrConflict` | S3 returned 409 for a concurrent conditional write; retry |
| `ErrNotFound` | Missing object or bucket; test with `errors.Is` |

`GetIfChanged` polls using the last ETag. S3 and GCS use a conditional request;
the file provider reads the whole file. See the [Store API][store] for ranged
reads, listing, and batch delete.

### Request limits and timings

- `Config.RequestsPerSecond` paces requests; zero disables pacing.
- `Config.MaxInflightWrites` bounds concurrent writes and deletes; zero means 16,
  negative means unbounded.
- `objstore.Urgent(ctx)` bypasses both limits for heartbeats and log commits.
  File writes also skip chunked write-back.
- `objstore.WithTimings(ctx, &t)` records request timings. Log them with `t.Attrs()`.
  S3 and GCS record only write-slot wait time; the file provider records each call.

## Write-ahead log

[wal][wal] batches concurrent appends into entries, committing once per second
by default. Each entry claims its sequence number with a conditional PUT.

```go
w := wal.NewWriter(s, "log/", 1, func(seq uint64, at time.Time, records []wal.Bytes) {
	// Apply the durable entry before Append returns.
})
defer w.Close()

if err := w.Append(ctx, []wal.Bytes{wal.Bytes("hello")}); err != nil {
	return err
}
```

Start at sequence 1 for a new log. Use `wal.Walk` to replay: it GETs consecutive
sequence numbers and stops at the first missing key, without listing the bucket.

**Recovery rules:**

- `Append` returning `nil` means the records are durable and `onCommit` has run.
- `ErrUnresolved` or a context error means the records **may be durable**.
  Replay from your checkpoint; do not blindly append them again.
- `ErrOverloaded` rejects an append when the pending-data limit is reached.
- Keep **one live writer per prefix**. Conditional writes detect a second writer
  (`ErrLostRace`); they do not replace leader election. See the [package docs][wal]
  for terminal errors and `SetFloor` when truncating a log.

## Leases

[lease][lease] holds one object key for leader election or a single writer.

```go
l, err := lease.Acquire(ctx, s, "jobs/leader", lease.OwnerID(), lease.DefaultTTL)
if errors.Is(err, lease.ErrNotOwner) {
	return nil // another process holds it; try later
}
if err != nil {
	return err
}
defer l.Release(context.WithoutCancel(ctx))

ctx, cancel := context.WithCancel(ctx)
defer cancel()
l.Start(cancel) // cancel work if the lease is lost

for {
	if err := l.Valid(); err != nil {
		return err
	}
	if err := doGuardedWork(ctx); err != nil {
		return err
	}
}
```

Renewal runs every TTL/4. `Release` allows immediate takeover; after a crash or
partition, takeover waits until the stored expiry plus TTL/2. All processes must
use the **same TTL**, with relative clock error **below TTL/2**.

**A lease cannot fence an in-flight write.** Durable writes still need their own
compare-and-swap or sequence check. For repeated acquisition attempts, keep one
handle from `lease.New`. Use `lease.Shared` to share a lease within a process.

## Cached reads

- [cache][cache] adds memory and optional disk storage. Concurrent misses for one
  key share a GET. `Cache.Put` writes through both tiers.
- [rangeread][rangeread] combines nearby byte ranges into fewer GETs through the cache.

**Cache only immutable objects.** Cached keys are never revalidated. Keys under
`ns/<name>/` belong to a namespace; `InvalidateNamespace` drops that namespace
from both tiers.

## Encryption

Choose a server-side KMS key per object:

```go
s = s.WithKMSKeys(func(ctx context.Context, key string) (string, error) {
	tenant, _, _ := strings.Cut(key, "/")
	return keyFor(tenant), nil // empty string uses the bucket policy
})
```

S3 uses SSE-KMS; GCS uses `kmsKeyName`. Reads need no caller-supplied key.
Disabling or deleting a KMS key makes reads fail with an error wrapping
`objstore.ErrAccessDenied`. KMS rotation needs no client changes.

**The file provider does not encrypt.** It ignores KMS keys and reports
`s.KMS() == false`. For a fixed S3 policy, use `s3.Config.SSE` and `KMSKeyID`.

## Tests

Use [storetest/bucket][bucket] for a fresh bucket per test, removed at cleanup.
It uses a temporary directory unless `OBJSTORE_TEST_S3` is set.

[storetest][storetest] injects failures before writes, lost responses after
writes, hangs, and pauses. It also provides latency and bandwidth limits, rewrite
detection, and a KMS test double. Use `bucket.NewFaulty(t)` to get a store and its
fault controls. Test a custom backend with `storetest.Conformance(t, s)`.

To run this repository's tests:

```sh
go test -race ./...
```

No cloud credentials are needed: tests use temporary files and fake servers.
Real-store tests are opt-in:

| Variable | Setup |
| --- | --- |
| `OBJSTORE_TEST_S3` | S3 endpoint. Unset AWS credentials and region default to MinIO's `minioadmin` / `minioadmin` and `us-east-1`. |
| `OBJSTORE_TEST_R2_ENDPOINT` | R2 endpoint, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION=auto`. Runs `TestR2` in `./aws/s3`. |
| `OBJSTORE_TEST_GCS_PROJECT` | GCP project with Application Default Credentials allowed to create buckets. Runs `TestConformanceReal` in `./gcp/gcs`. |

These tests create and delete test buckets. MinIO creates a missing key on
`PutIfMatch`, unlike AWS S3 and R2, so skip that case:

```sh
OBJSTORE_TEST_S3=http://localhost:9000 go test -race -skip '/ETagCASMissing' ./...
```

## Upgrading

See [CHANGELOG.md](CHANGELOG.md) for breaking changes and API replacements.
**v0.4 client-side encrypted objects remain ciphertext in v0.5 and later.**
Rewrite or delete them before upgrading; the new providers do not decrypt them.

## License

[MIT](LICENSE).

[s3]: https://pkg.go.dev/github.com/axiomhq/objstore/aws/s3
[gcs]: https://pkg.go.dev/github.com/axiomhq/objstore/gcp/gcs
[fs]: https://pkg.go.dev/github.com/axiomhq/objstore/fs
[store]: https://pkg.go.dev/github.com/axiomhq/objstore#Store
[wal]: https://pkg.go.dev/github.com/axiomhq/objstore/wal
[lease]: https://pkg.go.dev/github.com/axiomhq/objstore/lease
[cache]: https://pkg.go.dev/github.com/axiomhq/objstore/cache
[rangeread]: https://pkg.go.dev/github.com/axiomhq/objstore/rangeread
[storetest]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest
[bucket]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest/bucket
