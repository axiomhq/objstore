# Changelog

## v0.5.0 (unreleased)

### Breaking changes

**Encrypted object format**

- Every encrypted object is now written with header version 2. A non-empty v2 object has the v1 layout except the version byte; an empty v2 object carries a GCM tag (41 bytes, was 25).
- Readers older than v0.5.0 cannot read v2 objects: they fail with `invalid encrypted object header`. Worse, an old reader holding a stale cached "no key record" for a namespace treats v2 ciphertext as plaintext and returns it. Roll out v0.5.0 readers everywhere before any v0.5.0 writer; do not run a mixed fleet.
- v0.5.0 reads v1 non-empty objects unchanged, but rejects a v1 empty object with an error wrapping `objstore.ErrLegacyEmptyObject`. Such objects are 25 bytes, starting `DWEK` and version byte 1. Rewrite (Put the empty value again) or delete every one. Only a v0.5.0 writer produces the v2 form.

**objstore**

- `objstore.New(ctx, endpoint, bucket)` and `NewConfigured(ctx, cfg)` are gone. Use `s3.Open`, `gcs.Open`, `fs.Open`, or `objstore.Open(backend, cfg)`.
- `Config.Endpoint`, `Bucket`, `AllowedEndpoints`, `SSE`, `KMSKeyID` and `RequestTimeout` moved to `s3.Config`.
- `Store.SSE()` → `(*s3.Backend).SSE()`. `objstore.ErrEndpointDenied` → `s3.ErrEndpointDenied`.
- `objstore.Backend` is an exported interface (was an alias of an unexported one). It may grow.
- `Timings.LogAttrs() []any` → `Timings.Attrs() []slog.Attr`.
- `ErrNotFound` and `ErrRange` messages now start with `store:`. `errors.Is` is unaffected.
- An S3 409 `ConditionalRequestConflict` now wraps `objstore.ErrConflict`. It means retry, not a lost race.
- The built-in plaintext exemptions are now `Config.AcceptPlaintext` (nil = `DefaultAcceptPlaintext`, same exemptions). Bytes carrying the encrypted header are always decrypted. `GetRange` never consults it.
- `InstallNamespaceKey` returns `ErrInvalidEnvelope` for a bad envelope (was `kms.ErrKeyUnavailable`), and an error wrapping `ErrKeyRecordExists` when a different record exists.
- A key-lookup timeout reads `key lookup timed out after …` and no longer matches `errors.Is(err, context.DeadlineExceeded)`. A caller's own cancel still returns `ctx.Err()`.

**fs** (was the `file://` endpoint of `objstore.New`)

- The file backend is package `fs`: `fs.Open(root, bucket, cfg)` or `fs.New(root, bucket)`.
- Keys containing `/wal/` no longer get lock stripes of their own. Set `fs.Backend.IsolatedKeys` before first use, the same in every process.
- Unix with flock only (not Solaris or AIX; illumos works). Elsewhere it builds and every call fails with `errors.ErrUnsupported`.
- Invalid keys wrap `objstore.ErrInvalidKey`.

**kms**

- `kms` imports only the standard library.
- `kms.AWSKMS`, `kms.AWSClient` → `awskms.Provider`, `awskms.Client` (package `kms/awskms`).
- `kms.GCPKMS`, `kms.GCPClient` → `gcpkms.Provider`, `gcpkms.Client` (package `kms/gcpkms`).
- `kms.Router{Local, AWS, GCP, Default}` → `kms.Router{Routes, Default, DefaultScheme}`. The longest scheme prefix in `Routes` wins. An unmatched name goes to `Default` as `DefaultScheme+name`; the scheme is no longer guessed from `Default`'s type.
- `ErrKeyUnavailable` text is `kms: customer-managed encryption key unavailable`.
- A DEK that is not `kms.DEKSize` bytes is a caller error (`kms.CheckDEK`), not `ErrKeyUnavailable`.

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
- `Reader.Store`, `Objects`, `Cfg`, `Memory` are unexported. Use `(*Reader).Config()`.
- Invalid caller input (bad extent, empty `Load.Key`, one Key with two extents) is `ErrInvalidExtent` (was `ErrCorrupt`). `ErrCorrupt` now means a wrong-length read only.

**storetest**

- `storetest.New`, `storetest.NewFaulty` → `bucket.New`, `bucket.NewFaulty` (package `storetest/bucket`). `storetest` imports no backend.
- `ErrFault` text is `storetest: injected fault`.
- `Fault.Set` panics on `From` or `MatchFrom` for any Op but `OpGetRange`.
- Each `Set` of a `Pause` plan arms a fresh pause.
- A shaped failure on a read returns no data and adds nothing to `ReadBytes`.

### Fixed

- objstore: a cancelled caller no longer opens a namespace's KMS backoff or reports `kms.ErrKeyUnavailable`.
- objstore: concurrent cold key lookups of one namespace share one record GET and one Unwrap. `Urgent` callers share a lookup of their own, so they never wait behind the pacer.
- objstore: a failed or unparseable key-record read is a storage error, not `kms.ErrKeyUnavailable`, and keeps a still-valid cached key.
- objstore: `Delete` and `DeleteMany` no longer need the namespace key; crypto-shredding works after revocation.
- objstore: a cached "no key record" is trusted only by whole-object reads, so another process installing a key cannot make this one write plaintext or return ciphertext.
- objstore: a lookup that began before an Install or Retire cannot cache its stale answer.
- objstore: an empty encrypted object is authenticated; a bare header no longer passes as one.
- objstore: small encrypted ranges are copied out instead of pinning decrypted blocks.
- objstore: `Config.MaxInflightWrites < 0` means unbounded (was a deadlock).
- fs: a `PutIfAbsent` retry after a failed directory sync syncs the directory, so `(false, nil)` means durable.
- fs: `PutIfMatch` writes and fsyncs its temp file before taking the key lock.
- fs: a list page walks from the prefix's directory and stops after one page (was a whole-bucket walk).
- s3: `DropBucket` on a missing bucket returns nil.
- kms: `LocalFile` opens keys through `os.OpenRoot` and refuses symlinks; a missing rotated key file reports why.
- kms: AWS and GCP SDK errors are wrapped with `%w`.
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

- Packages `fs`, `s3`, `gcs` (Google Cloud Storage), `kms/awskms`, `kms/gcpkms`, `storetest/bucket`.
- objstore: `Open`, `OpErr`, `IsUrgent`, `TimingsOf`, `Timings.Since`, `Call.String`, `ErrConflict`, `ErrInvalidKey`, `ErrInvalidEnvelope`, `ErrKeyRecordExists`, `ErrLegacyEmptyObject`.
- objstore: `Config.KeyProvider`, `Config.KeyRefreshInterval`, `Config.AcceptPlaintext`, `DefaultAcceptPlaintext`.
- kms: `DEKSize`, `CheckDEK`, `LeaseCadencer`; `awskms.Scheme` (`aws:`), `gcpkms.Scheme` (`gcp:`).
- wal: `ErrRecordTooLarge`, `ErrInvalidRecord`, `ErrWriterFailed`, `Stats.Lost`, `Stats.Terminal`, `WalkParallelWithGet`. `WriteError` implements `error` and `Unwrap`.
- cache: `ByteCache.GenerationOf`, `Resident.GenerationOf`, `EntryOverhead`, `Cache.Logger`, `Disk.Logger` (nil = `slog.Default()`), `Outcome.String`, `Class.String`. Windows gets a real directory lock (`LockFileEx`).
- lease: `lease.Store`, `(*Lease).SetLogger`.
- rangeread: `ErrInvalidExtent`, `(*Reader).Config`.
- storetest: `Conformance`, `Faulty`, `Plan.MatchFrom`. `OpListPrefixes` is plannable, not only counted.
- `go.mod` requires Go 1.26.0 (was 1.27.1).
- CI: gofmt, `go mod tidy -diff`, vet, staticcheck, race tests; a MinIO job; a macOS job; vet cross-compiles for windows, darwin, illumos, solaris, aix/ppc64.

### Known limitations / accepted debt

- AWS and GCP KMS wraps carry no encryption context (AAD). Binding the key name would break existing wrapped DEKs.
- AES-GCM uses a random 96-bit nonce per object under one DEK per namespace: keep each namespace well under 2^32 encrypted writes. `RotateNamespaceKey` re-wraps the same DEK, so it does not reset the count.
- `GetRange` on an encrypted object does two GETs: the header, then the covering blocks.
- cache: several overlapping fetch entry points (`FetchWith`, `FetchCached`, `FetchCachedRange`, `FromDisk`, `CachedRange`, `Gated`), and stage results and budgets travel in the context (`WithResults`, `WithBudget`). A later `FetchRanges` replaces the earlier scope.
- While a `KeyProvider` is configured, every write and range read in a namespace without a key record first GETs its record (shared by concurrent callers).
- MinIO creates the object on a `PutIfMatch` of a missing key. The MinIO CI job skips `ETagCASMissing`.
- Test hooks stay exported: `lease.Before`, `lease.Steal`, `Lease.Expire`, `Lease.Continuous`, `Lease.FloorProven`, `Disk.FirstFile`, `Disk.KeysForTest`, `Disk.SetMaxPinnedNamespaces`.
- The Windows disk-cache lock is vetted in CI, never run there.
