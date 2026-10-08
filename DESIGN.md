# Design and usage

[Back to README](README.md) · [API reference](https://pkg.go.dev/github.com/axiomhq/objstore)

## Providers

Import only the provider you need. Each package has an `Open` function that
returns a `*objstore.Store`.

| Package | Storage |
| --- | --- |
| [aws/s3][s3] | AWS S3 and S3-compatible stores: R2, MinIO, Ceph, Hetzner |
| [gcp/gcs][gcs] | Google Cloud Storage |
| [azure/blob][azure] | Azure block blobs; caller-supplied container client |
| [fs][fs] | Local directory; writes are fsynced before acknowledgment |

The file provider requires Unix with `flock`; Solaris and AIX are unsupported.

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

For Azure, construct an SDK `*container.Client` with your credentials, retry and
transport options, then call `blob.Open(blob.Config{Client: client}, cfg)`.
The backend does not close that client. Azure listing preserves lexical cursors
by rescanning from the prefix start on each page: memory is page-bounded, but
deep pagination makes progressively more service requests.

S3 accepts caller-owned SDK configuration through `s3.Config.AWS`. GCS accepts
`gcs.Config.Client` for a caller-owned `*storage.Client` (including its retry
policy), or `Options` for a library-owned client; they are mutually exclusive.
Construct injected GCS clients with `storage.WithJSONReads()` for conditional
reads. `Store.Close` closes only library-owned GCS clients. SDK retries remain
SDK policy; arbitrary reader bodies cannot necessarily be replayed.

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

## Streaming and object information

`Backend` is unchanged. `Store` exposes optional `ReaderBackend`, `StatBackend`,
`UploadBackend`, `ConditionalUploadBackend`, and `SignBackend` capabilities;
unsupported operations return an error wrapping `errors.ErrUnsupported`.
There is no whole-object buffered fallback.

```go
// A nil Size consumes a pipe or other unknown-length source through EOF.
info, err := s.Upload(ctx, "archive", source, objstore.UploadOptions{})
if err != nil {
	return err // outcome may be ambiguous; reconcile before retrying
}
fmt.Println(info.Size, info.ETag) // token from this commit, not a later HEAD

r, err := s.NewReader(ctx, "archive", 5, 0) // offset 5 through EOF
if err != nil {
	return err
}
defer r.Close()
_, err = io.Copy(destination, r)
```

`UploadOptions.Size` is a `*int64`: nil means unknown, pointer to zero means an
empty object, and a positive value consumes exactly that prefix. Surplus input
remains unread; a short source fails. The library never closes the source;
callers must unblock a blocked `Read` on cancellation. **An error is not proof
that nothing committed**, including a canceled single PUT or a lost response.
An existing object at or beyond EOF yields an empty streaming reader. Positive
read lengths cap at EOF; zero reads to the end. Negative/overflowing ranges fail.
Existing `GetRange` still requires the exact range. Read to EOF and check errors
to observe terminal provider integrity checks; closing early does not verify it.

`Stat` returns `ObjectInfo`. Its `ETag` shares the `GetWithETag`/CAS token domain:
S3/Azure ETag, GCS generation, filesystem SHA256. **Tokens are opaque, not a
portable checksum**; multipart changes S3 ETags. `UploadIfAbsent` and
`UploadIfMatch` return `(ObjectInfo, bool, error)`: false/nil is a lost condition,
while an error may be ambiguous. Upload results guarantee committed size/token;
use `Stat` for other attributes (which may describe a newer revision).

| Provider | Upload policy | Options and limitations |
| --- | --- | --- |
| S3 | Unconditional: single PUT at known size ≤8 MiB; otherwise sequential 8 MiB multipart parts. Conditional: single PUT only, known size ≤5 GiB. | Multipart limit: 10,000 parts, 83,886,080,000 bytes (about 78 GiB); known excess rejected before requests, unknown excess aborted. Abort is best-effort; bucket lifecycle cleanup is recommended. |
| GCS | Bounded 16 MiB resumable chunks; known or unknown size, also for CAS. | SDK owns chunk retries. Generation is the CAS token, not the HTTP ETag. |
| Azure | Sequential 8 MiB staged blocks; conditions checked at commit. | SDK block-count limits apply (50,000 blocks, about 390 GiB). Per-object objstore KMS overrides are unsupported; configure account encryption externally. |
| Files | Stream to a temporary file, hash, then atomically publish and sync. | `Stat` reads/hashes the file. Nonempty metadata/content options and signing are unsupported. Existing filesystem KMS behavior is unchanged. |

Cloud uploads accept `Metadata`, `ContentType`, `CacheControl`, and
`StorageClass` (Azure access tier). Nonempty unsupported options fail before
writing. S3/Azure require lowercase metadata names and printable ASCII values
without surrounding whitespace; Azure names use letters/digits/underscores and
start with a letter or underscore. Rejected values are not silently normalized.
GCS JSON metadata preserves names and values. Storage-class names are provider
specific; S3 `Stat` reports `STANDARD` when its header is omitted.

`Sign(ctx, key, SignOptions{Method, Expires, Headers})` returns
`SignedRequest{URL, Method, Headers}`. Use the returned method and headers;
the URL contains credentials and must not be logged. S3 supports GET/HEAD/DELETE
without custom headers and PUT with Content-Type, Cache-Control and x-amz-meta-*
headers. GCS supports those methods and V4 signed headers using SDK credentials
capable of signing. Azure uses shared-key SAS; it rejects custom headers because
SAS does not authenticate arbitrary headers (PUT returns x-ms-blob-type).
S3/GCS signing expires within seven days. Signing performs no mutation, bypasses
write admission, and applies the configured KMS hook for PUT.

`DeleteMany` errors expose `*objstore.DeleteError` with ordered `Failures`, each
containing `Key`, `Err`, and `Unattempted`. Every failed or unattempted input is
included, including later batches after an early stop and duplicates. Errors
retain provider causes for `errors.Is`/`As`; an attempted failure can be ambiguous.
For a legacy backend without per-key errors, every key is conservatively listed.
`storetest.Fault`, `storetest.KMS`, pacing, and write admission preserve the new
capabilities; the KMS test double rejects signing since it cannot revoke an
exported credential.

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
| `OBJSTORE_TEST_AZURE` | Local Azurite Blob URL. Uses the disposable `objstore` emulator account below, not cloud credentials. |

These tests create and delete test buckets. MinIO creates a missing key on
`PutIfMatch`, unlike AWS S3 and R2, so skip that case:

```sh
OBJSTORE_TEST_S3=http://localhost:9000 go test -race -skip '/ETagCASMissing' ./...
```

Azure emulator setup (run the server in a separate terminal):

```sh
AZURITE_ACCOUNTS="objstore:b2Jqc3RvcmUtZW11bGF0b3Itbm90LXNlY3JldA==" \
  npx --yes --package azurite@3.37.0 azurite-blob --skipApiVersionCheck \
  --location /tmp/objstore-azurite --blobHost 127.0.0.1 --blobPort 10000
OBJSTORE_TEST_AZURE=http://127.0.0.1:10000 go test -race -count=1 ./azure/blob
```

`storetest.StreamingConformance(t, s, storageClass)` adds pipe uploads, CAS token
interoperability, sizes, EOF ranges and metadata round trips. Provider HTTP tests
cover commit-response tokens, late checksum errors, truncated inputs, conditional
failures, multipart aborts and complete delete outcomes. Emulator tests cannot
prove cloud IAM/KMS behavior or all third-party S3 semantics; validate those
against the target service before adoption.

## Upgrading

See [CHANGELOG.md](CHANGELOG.md) for breaking changes and API replacements.
Streaming capabilities are additive: existing byte methods, exact ranges,
mandatory `Backend`, and WAL/cache formats are unchanged. Custom wrappers must
forward optional capabilities or explicitly remain unsupported. The existing
conditional-write probe covers single PUTs, not multipart conditional writes;
S3 conditional streaming deliberately remains single-request. Dependencies are
pinned in `go.mod`/`go.sum`, including Azure azblob v1.8.2 and azcore v1.23.2.
**v0.4 client-side encrypted objects remain ciphertext in v0.5 and later.**
Rewrite or delete them before upgrading; the new providers do not decrypt them.

[s3]: https://pkg.go.dev/github.com/axiomhq/objstore/aws/s3
[gcs]: https://pkg.go.dev/github.com/axiomhq/objstore/gcp/gcs
[azure]: https://pkg.go.dev/github.com/axiomhq/objstore/azure/blob
[fs]: https://pkg.go.dev/github.com/axiomhq/objstore/fs
[store]: https://pkg.go.dev/github.com/axiomhq/objstore#Store
[wal]: https://pkg.go.dev/github.com/axiomhq/objstore/wal
[lease]: https://pkg.go.dev/github.com/axiomhq/objstore/lease
[cache]: https://pkg.go.dev/github.com/axiomhq/objstore/cache
[rangeread]: https://pkg.go.dev/github.com/axiomhq/objstore/rangeread
[storetest]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest
[bucket]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest/bucket
