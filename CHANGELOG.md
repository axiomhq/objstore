# Changelog

## Unreleased

### Added

- Optional streaming `NewReader`, `Upload`, `UploadIfAbsent`, `UploadIfMatch`, `Stat`, and header-aware `Sign` capabilities exposed through `Store`, without changing mandatory `Backend`. Upload results contain the committed size and backend CAS token.
- `UploadOptions` for explicit known or unknown length, metadata, content type, cache control, and provider storage class. Unsupported options fail explicitly; streaming ranges cap at EOF while byte `GetRange` remains exact.
- S3 bounded multipart unconditional uploads (8 MiB parts, about 78 GiB maximum). Conditional uploads remain single PUTs with known size up to 5 GiB. ETags remain opaque and are not portable checksums.
- `azure/blob` backend with caller-owned SDK container client, block uploads, CAS, SAS signing, HTTP regression tests and Azurite conformance. SAS cannot authenticate custom headers; per-object objstore KMS overrides are unsupported. Lexical pagination rescans earlier pages.
- `gcs.Config.Client` for caller-owned SDK clients, mutually exclusive with `Options`. `Close` preserves caller-owned clients.
- Streaming conformance and wrapper/gate/KMS, integrity, cancellation, commit-token and metadata checks. Azure SDK versions are pinned in `go.mod`/`go.sum`.

### Changed behaviour

- `DeleteMany` errors expose `DeleteError.Failures`, including all failed and unattempted inputs, preserving order, duplicates, and underlying provider errors. Legacy backend errors conservatively list every key. S3 per-key AccessDenied wraps `ErrAccessDenied`.
- No byte API, CAS token domain, exact range, WAL or cache format change. Upload sources remain caller-owned; surplus bytes beyond a declared size remain unread. Errors can have ambiguous commit outcomes.

## v0.9.0

### Breaking changes

- **wal page format**: pages are `OWAL\x02`, whose header carries a row count after the record weight. `OWAL\x01` pages still decode (each record one row); readers older than this release refuse v2 pages as corrupt, so upgrade every reader before any writer.

### Added

- `wal.Counter`: a `Record` with `Rows() int`, for a record that stands for several of the caller's rows. A page's header carries the sum (`Header.Rows`), so `WalkHeaders` counts rows, not records. Records that are not Counters count one.

### Changed behaviour

- **wal**: the floor recheck after a claim passes `retry` as the check before it does (true only once an attempt failed unverifiably). A first try's recheck no longer forces a store read on an oracle that can answer locally; v0.8.0 cost a lease-holding writer a GET on every acked entry. A retried batch still asks for a fresh watermark.

### Removed

API nothing calls, here or in its known importers (dotwerk, logwerk):

- cache: `AddRequestLookups`; `ByteCache.Hit`, `Misses`, `LowStats`, `LowCharge`, and the low-priority hit and miss counters behind them; `Cache.FetchCached` (use `FetchWith`).
- aws/s3: `Backend.SSE` (the caller set it in `s3.Config`).

## v0.8.0

### Changed behaviour

- **lease**: `Valid` returns `ErrNotOwner` after `Retire`, even before the stored expiry: a retired holder no longer renews, so it must not act. A released `Shared` ref is no longer valid while other refs hold the lease.
- **lease**: renewals keep the acquiring context's values (a `WithKMSKey`, say), not its cancellation. `Release` bounds its wait for an in-flight `Acquire` by the TTL, and `Retire` cancels in-flight acquisition and renewal.
- **wal**: `NewWriter` refuses `nextSeq` 0, and a batch whose sequences would pass `MaxUint64` is refused before any PUT and finishes the writer. With a floor oracle installed, a claim is checked against the floor again before it is acked; a covered claim is `ErrUnresolved`.
- **wal**: a record whose size overflows page framing is `ErrRecordTooLarge`; `Encode` refuses pages `Decode` would reject. `ErrInvalidRecord` from `AppendTo` now wraps the record's own error.
- **fs**: `Delete` fsyncs the parent directory; `DeleteMany` once per distinct directory. `EnsureBucket` fsyncs the bucket and its ancestors up to the first one this process may not open. `PutIfAbsent` that loses a race fsyncs the directory before reporting it.
- **aws/s3**, **gcp/gcs**: translated errors (`ErrNotFound`, `ErrRange`) also wrap the provider's error, which now appears in the message. S3 `EnsureBucket` sends the region's `LocationConstraint` on AWS outside us-east-1. GCS `GetRange` rejects a response starting at the wrong offset with `ErrRange`.
- **cache**: `Disk.Wipe` waits for fills already writing and refuses new ones until then. `Resident.Put` refuses entries of size zero or less. A follower of a shared load that got the leader's `ErrBudget` retries once under its own context.
- **rangeread**: requests never share a parent read across an `InvalidateNamespace`. `FetchRanges` stops decoding and publishing once its context ends.
- `DeleteMany` with no keys returns at once instead of waiting for a write slot.
- **wal**: `Walk` and `WalkParallel` fail with `ErrCorrupt` on a batch larger than a Writer can produce (more than nine pages, or over 512 MiB decoded). `WalkParallel` charges a page for its record descriptors as well as its bytes, and counts a batch being assembled against a separate 512 MiB budget; its documented memory bound says so.
- **rangeread**: the physical-byte limit holds for bytes a shared read still holds after its waiters are cancelled. Followers of a shared read retry only the leader's cancellation, deadline or `ErrBudget`, not store errors every retry would repeat.
- **wal**: a Writer keeps each batch's `Weigher` sum within uint64; an Append that would overflow it fails alone with `ErrInvalidRecord`. `Encode` refuses a non-zero `At` in the epoch's first millisecond, which would decode as unset. `Size`, `AppendTo` and `Weight` must not block; `Close`'s bound excludes them.
- **missing buckets**: every operation on a missing bucket wraps `ErrNotFound` on every backend, now also S3 and GCS (with the provider's error) and `fs` deletes. GCS `PutIfMatch` and `Delete` on a missing bucket no longer report `(false, nil)` and success: after a 404 they read the bucket once to tell a missing bucket from a missing object. `storetest.Conformance` checks it.
- **fs**: `DropBucket` fsyncs the directory that held the bucket.
- **cache**: `Disk.ExpireInactive` keeps a namespace's activity, and does not report it expired, while removing its files fails.
- **lease**: `Shared.Join` refuses a newly minted lease that is no longer valid, and releases it.

### Added

- `Store.Close` releases the provider's client (GCS); `gcs.Backend.Close`. `storetest.Fault` and `storetest.KMS` pass `Close` through.
- `s3.Config.AWS` injects an SDK `aws.Config` (credentials, region, HTTP client) instead of loading the default one.
- `wal.WithContext`: a Writer's store and floor calls carry the context's values, not its cancellation.
- `cache.DiskConfig` and `cache.OpenDisk`: a disk tier whose sweep of stale directories logs to the given logger.
- `lease.Shared.JoinContext`: `Join` whose wait for another mint or release ends with the context.
- `storetest.Fault.SupportsKMS`: a faulty wrapper keeps the wrapped store's KMS capability.
- `go.opentelemetry.io/otel/sdk` v1.45.0, for its advisory on exporter endpoint URLs in logs.

### Fixed

- `storetest.KMS` no longer passes KMS keys to the store underneath, so its tests run on S3 stores without KMS (MinIO). It forgets an object's key when the object is deleted, and an `Ambiguous` fault lands its write even when a `Shape` would fail it.

## v0.7.0

### Breaking changes

- **wal page format**: the header carries a record weight after the record count. Pages written by v0.6 do not decode; rewrite or drop them before upgrading.

### Added

- `wal.Weigher`: a `Record` with a `Weight() uint64`. A page's header carries the sum over its records, split pages each their own.
- `wal.Header.Records` and `wal.Header.Weight`, filled on read; `Encode` and the `Writer` compute them.
- `wal.DecodeHeader` reads a header from a page prefix, and `wal.WalkHeaders` walks a log by headers alone: one ranged GET of 128 bytes per page, on bounded workers, so accounting a log costs its page count, not its bytes.

## v0.6.1

- Tests only: `rangeread` pins that a cancelled follower of a shared fetch leaves while the leader's GET runs on, and that a panicking loader reaches every waiter as an error.

## v0.6.0

- `Store.ID()` names the bucket: `file://<dir>` (fs), `<endpoint>/<bucket>` or `s3://<bucket>` (S3), `gs://<bucket>` (GCS). Stores opened apart over one bucket, and Stores from `WithBackend` or pacing, share it. A `Backend` passed to `objstore.Open` may implement `ID() string`; one that does not gets a random ID per `Open`.

## v0.5.0

### Breaking changes

**Encryption moves to the store**

- The client-side envelope is gone: `ConfigureCMEK`, `SetCMEKRefreshInterval`, `InstallNamespaceKey`, `RetireNamespaceKey`, `RotateNamespaceKey`, `CheckNamespaceKey`, `Envelope`, `EncryptionCustomerManaged`, `Config.KeyProvider`, `Config.KeyRefreshInterval`, `Config.AcceptPlaintext`, `DefaultAcceptPlaintext`, and packages `kms`, `aws/kms` and `gcp/kms`.
- In its place, `Store.WithKMSKeys(fn)` names a KMS key per written object; S3 stores it with SSE-KMS under that key, GCS with that `kmsKeyName`. Reads need no key. `WithKMSKey(ctx, id)` keys one write; on a Store with a `WithKMSKeys` function, the function decides and can read it with `KMSKey(ctx)`.
- Objects a v0.4 store sealed client-side (`DWEK` header) are returned as stored, still ciphertext. Rewrite or delete them before upgrading.

**objstore**

- `objstore.New(ctx, endpoint, bucket)` and `NewConfigured(ctx, cfg)` are gone. Use `s3.Open` (`objstore/aws/s3`), `gcs.Open` (`objstore/gcp/gcs`), `fs.Open`, or `objstore.Open(backend, cfg)`.
- `Config.Endpoint`, `Bucket`, `AllowedEndpoints`, `SSE`, `KMSKeyID` and `RequestTimeout` moved to `s3.Config`.
- `Store.SSE()` → `(*s3.Backend).SSE()`. `objstore.ErrEndpointDenied` → `s3.ErrEndpointDenied`.
- `objstore.Backend` is an exported interface (was an alias of an unexported one). It may grow.
- `Timings.LogAttrs() []any` → `Timings.Attrs() []slog.Attr`.
- `ErrNotFound` and `ErrRange` messages now start with `store:`. `errors.Is` is unaffected.
- An S3 409 `ConditionalRequestConflict` now wraps `objstore.ErrConflict`. It means retry, not a lost race.

**fs** (was the `file://` endpoint of `objstore.New`)

- The file backend is package `fs`: `fs.Open(root, bucket, cfg)` or `fs.New(root, bucket)`.
- Keys containing `/wal/` no longer get lock stripes of their own. Set `fs.Backend.IsolatedKeys` before first use, the same in every process.
- Unix with flock only (not Solaris or AIX; illumos works). Elsewhere it builds and every call fails with `errors.ErrUnsupported`.
- Invalid keys wrap `objstore.ErrInvalidKey`.

**wal**

- `ErrLostRace`, and `ErrUnresolved` from a commit (covered floor, corrupt read-back), finish the writer. The claiming batch gets that error; queued and later Appends get `ErrWriterFailed`. `errors.Is(err, ErrLostRace)` / `ErrUnresolved` is true only for the claiming batch.
- An append no page or queue can hold is `ErrRecordTooLarge` at `Enqueue` (was `ErrOverloaded`).
- `WithCommitInterval`: `<= 0` keeps the default; other values are clamped to [`MinCommitInterval`, `MaxCommitInterval`] (were ignored).
- `SeqFromKey` parses the key's last 20 characters; unpadded keys such as `log/42` fail.

**cache**

- `ByteCache.Generation` and `Resident.Generation` fields → `GenerationOf(key)`. Generations are per namespace (`ns/<name>/`); keys outside `ns/` are always 0.
- `ByteCache` charges each entry `len(key) + EntryOverhead` on top of its bytes and decode.
- `NewByteCacheStripes` rounds stripes up to a power of two and panics on `stripes <= 0`.
- `Disk.Pin` refuses negative charges and copies the map.
- `NewDisk` locks its directory and removes unlocked stale directories under a non-empty root. Cached files live in `Disk.Dir()`, a subdirectory of the locked one.

**lease**

- `(*Lease).Release(ctx)` and `(*Ref).Release(ctx)` take a context.
- `Lease.Log` field → `(*Lease).SetLogger`.
- `(*Lease).Acquire(ctx)` returns `error` (was `(*Lease, error)`). It errors on a handle already acquired, retired or released. A failed Acquire is retried on the same handle.
- `Acquire`, `New`, `Load`, `Steal` take a `lease.Store` (satisfied by `*objstore.Store`). `(*Lease).Store()` returns `lease.Store`.
- `ErrNotOwner` text is `lease: not held`.
- A fence callback runs on its own goroutine (on the caller's only when `Start` finds the lease already lost). `Done()` waits for it; `Release` and `Retire` never run it.
- `ttl <= 0` means `DefaultTTL`.

**rangeread**

- `rangeread.New` returns `(*Reader, error)`. Zero `Config` fields take defaults; invalid ones are an error. A zero `Config` used to skip every `FetchRanges`.
- `Reader.Store`, `Objects` and `Cfg` fields → `(*Reader).Store()`, `Objects()`, `Config()`. `Memory` is unexported.
- Invalid caller input (bad extent, empty `Load.Key`, one Key with two extents) is `ErrInvalidExtent` (was `ErrCorrupt`). `ErrCorrupt` now means a wrong-length read only.

**storetest**

- `storetest.New`, `storetest.NewFaulty` → `bucket.New`, `bucket.NewFaulty` (package `storetest/bucket`). `storetest` imports no backend.
- `ErrFault` text is `storetest: injected fault`.
- `Fault.Set` panics on `From` or `MatchFrom` for any Op but `OpGetRange`.
- Each `Set` of a `Pause` plan arms a fresh pause.
- A shaped failure on a read returns no data and adds nothing to `ReadBytes`.

### Fixed

- wal: a batch whose first PUT the store refuses (`objstore.ErrAccessDenied`) on its first attempt fails its callers at once and leaves the sequence free, instead of retrying behind a revoked key.

- objstore: `Config.MaxInflightWrites < 0` means unbounded (was a deadlock).
- fs: a `PutIfAbsent` retry after a failed directory sync syncs the directory, so `(false, nil)` means durable.
- fs: `PutIfMatch` writes and fsyncs its temp file before taking the key lock.
- fs: a list page walks from the prefix's directory and stops after one page (was a whole-bucket walk).
- s3: `DropBucket` on a missing bucket returns nil.
- wal: a record that fails to encode fails only its own Append (`ErrInvalidRecord`).
- wal: a batch is paced once and its pages go out back to back; a retry resumes after the pages already proven.
- wal: adopting our own earlier PUT no longer delays the next batch.
- wal: `SetMaxUnacked` counts the incoming records.
- wal: a single page after an abandoned batch walks as marker plus entry whether `BatchPages` is 0 or 1 (was `ErrCorrupt` for 0).
- wal: `WalkParallel` fetches pages in parallel, up to `workers` GETs (it only decoded in parallel).
- wal: `Stats` no longer races a batch split.
- wal: the `Close` drain waits a commit interval between retries.
- cache: a shared load survives the leader's cancel; it is cancelled only when the last waiter leaves.
- cache: invalidating one namespace no longer strands every namespace's disk entries.
- cache: a disk Put that cannot fit refuses up front instead of evicting everything first.
- cache: `Cache.Put` reads the generation before the store write.
- cache: a `NewDisk` sweep cannot remove a live Disk's directory; a root with glob metacharacters sweeps correctly.
- cache: `Wipe` and `Close` no longer hold the cache lock across `RemoveAll`.
- cache: `RestorePin` keeps the reservation's original time.
- cache: every `*Disk` method is nil-safe.
- cache: `Recharge` refuses a comparable type holding an uncomparable value instead of panicking.
- lease: `Retire` or `Release` on a lease driven by `New` + `Take` no longer blocks forever.
- lease: a retried Acquire after an unresolved first write adopts its landed record (was locked out for 1.5 TTL).
- lease: `Release` during an in-flight renewal no longer fences, and the handover lands on the renewal's nonce.
- lease: `Release` racing the first Acquire makes it fail with `ErrNotOwner`, not return a retired handle.
- lease: calling `Release` or `Retire` from the fence callback no longer hangs.
- lease: `Shared.Join` does no store I/O under its mutex; a panicking mint no longer wedges it.
- rangeread: a lone `Transient` load with `Decode` is decoded.
- rangeread: loads are deduped by key and validated before the budget check.
- rangeread: `Prefetch` checks each key's own memory tier (`Cache.ByteCacheFor`).
- storetest: `Pause` honours the call's context.
- storetest: `Faulty` cleanup resumes paused calls and waits up to 5 s for every call in flight.
- storetest: `bucket.NewS3` bucket names get a random suffix; setup is bounded to one minute.

### Added

- Packages `fs`, `aws/s3`, `gcp/gcs` (Google Cloud Storage), `storetest/bucket`. Provider packages live under `aws/` and `gcp/`; R2, MinIO, Ceph and Hetzner use `aws/s3`.
- objstore: `Open`, `OpErr`, `IsUrgent`, `TimingsOf`, `Timings.Since`, `Call.String`, `ErrConflict`, `ErrInvalidKey`, `ErrAccessDenied` (a 403, or a KMS key the store cannot use).
- objstore: `Store.WithKMSKeys`, `KMSKeyFunc`, `WithKMSKey`, `KMSKey`, `Store.KMS`.
- objstore: `Store.CheckConditionalWrites(ctx)`: PutIfAbsent a fresh `_probe/ifnonematch/` key twice (must create, then must not), then delete it; any other outcome is an error naming the backend.
- storetest: `NewKMS` stands in for SSE-KMS: it records each object's key and revokes one.
- wal: `ErrRecordTooLarge`, `ErrInvalidRecord`, `ErrWriterFailed`, `Stats.Lost`, `Stats.Terminal`, `WalkParallelWithGet`. `WriteError` implements `error` and `Unwrap`.
- cache: `ByteCache.GenerationOf`, `Resident.GenerationOf`, `EntryOverhead`, `Cache.Logger`, `Disk.Logger` (nil = `slog.Default()`), `Outcome.String`, `Class.String`. Windows gets a real directory lock (`LockFileEx`).
- lease: `lease.Store`, `(*Lease).SetLogger`.
- rangeread: `ErrInvalidExtent`, `(*Reader).Config`.
- storetest: `Conformance`, `Faulty`, `Plan.MatchFrom`. `OpListPrefixes` is plannable, not only counted.
- `go.mod` requires Go 1.26.0 (was 1.27.1).
- CI: gofmt, `go mod tidy -diff`, vet, staticcheck, race tests; a MinIO job; a macOS job; vet cross-compiles for windows, darwin, illumos, solaris, aix/ppc64.

### Known limitations / accepted debt

- cache: several overlapping fetch entry points (`FetchWith`, `FetchCached`, `FetchCachedRange`, `FromDisk`, `CachedRange`, `Gated`), and stage results and budgets travel in the context (`WithResults`, `WithBudget`). A later `FetchRanges` replaces the earlier scope.
- MinIO creates the object on a `PutIfMatch` of a missing key. The MinIO CI job skips `ETagCASMissing`.
- Test hooks stay exported: `lease.Before`, `lease.Steal`, `Lease.Expire`, `Lease.Continuous`, `Lease.FloorProven`, `Disk.FirstFile`, `Disk.KeysForTest`, `Disk.SetMaxPinnedNamespaces`.
- The Windows disk-cache lock is vetted in CI, never run there.
