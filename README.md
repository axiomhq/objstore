# objstore

```sh
go get github.com/axiomhq/objstore
```

Object storage with compare-and-swap, for Go. One `Store` over an S3 bucket
(`objstore/s3`: AWS, MinIO, Ceph, Hetzner, and Cloudflare R2 at
`https://<account-id>.r2.cloudflarestorage.com` with `AWS_REGION=auto`),
Google Cloud Storage (`objstore/gcs`), or a durable local directory
(`objstore/fs`), with:

- conditional PUT and GET, and the ETag with the body
- paginated list, delimited list, batch delete
- one package per provider, so a binary links only the SDKs it uses
- a file store (`objstore/fs`) that fsyncs every object and writes large ones back in chunks, so a small urgent write never waits behind a big one
- envelope encryption per name (`objstore/kms`, stdlib only), with AWS KMS and GCP KMS providers in `objstore/kms/awskms` and `objstore/kms/gcpkms`
- a conformance suite, fault injection and metering for tests (`objstore/storetest`)
- a write-ahead log: one entry per second, conditional PUT, nonce read-back (`objstore/wal`)
- a memory and disk object cache with singleflight and per-request stats (`objstore/cache`)
- a ranged reader that coalesces nearby reads into one GET (`objstore/rangeread`)
- a lease for leader election or a single writer: acquire, renew, fence (`objstore/lease`)

## Use

1. Open a store: `s, err := s3.Open(ctx, s3.Config{Endpoint: "https://s3.us-east-1.amazonaws.com", Bucket: "my-bucket"}, objstore.Config{})`.
   GCS: `s, err := gcs.Open(ctx, gcs.Config{Bucket: "my-bucket"}, objstore.Config{})`. Local: `s := fs.Open("/var/lib/data", "bucket", objstore.Config{})` (Unix only).
   Any other `objstore.Backend`: `s := objstore.Open(b, objstore.Config{})`.
   `s3.Config` adds SSE, an endpoint allow-list and a request timeout; `objstore.Config` adds pacing (`RequestsPerSecond`), a write bound (`MaxInflightWrites`) and encryption (`KeyProvider`).
2. Create the bucket if you need to: `s.EnsureBucket(ctx)`.
3. Write once: `created, err := s.PutIfAbsent(ctx, key, data)`. `created` is false when the key existed.
4. Swap: `body, etag, err := s.GetWithETag(ctx, key)`, then `ok, err := s.PutIfMatch(ctx, key, next, etag)`. `ok` false means someone else won: re-read and retry. `objstore.ErrConflict` (S3's 409) means the store never decided: retry.
5. Poll: `body, etag, unchanged, err := s.GetIfChanged(ctx, key, etag)`. One conditional request on S3 and GCS; the file store reads the whole file either way, so it is not cheaper than `Get` there.

A missing key returns an error wrapping `objstore.ErrNotFound`. Mark a
context with `objstore.Urgent(ctx)` for heartbeats and log commits: its calls
skip pacing and the write bound, and on the file store are written whole
instead of in chunks.

### Migrating from v0.4

| v0.4 | v0.5 |
| --- | --- |
| `objstore.New(ctx, endpoint, bucket)` | `s3.Open(ctx, s3.Config{Endpoint: endpoint, Bucket: bucket}, objstore.Config{})`; for `file://root`, `fs.Open(root, bucket, objstore.Config{})` |
| `Config.Endpoint`, `Bucket`, `SSE`, `KMSKeyID`, `AllowedEndpoints`, `RequestTimeout` | `s3.Config` |
| `Store.SSE()`, `objstore.ErrEndpointDenied` | `(*s3.Backend).SSE()` (from `s3.New`), `s3.ErrEndpointDenied` |
| `s.ConfigureCMEK(p)` | still works; or `objstore.Config{KeyProvider: p}` |
| built-in plaintext exemptions | `Config.AcceptPlaintext` (nil = `objstore.DefaultAcceptPlaintext`) |
| `Timings.LogAttrs()` | `Timings.Attrs()` |
| `kms.AWSKMS{Client: c}`, `kms.GCPKMS{Client: c}` | `awskms.Provider{Client: c}`, `gcpkms.Provider{Client: c}` |
| `kms.Router{Local: l, AWS: a, GCP: g, Default: d}` | `kms.Router{Routes: map[string]kms.KeyProvider{"local:": l, awskms.Scheme: a, gcpkms.Scheme: g}, Default: d, DefaultScheme: "local:"}` (the scheme of `d`) |
| `l.Release()`, `ref.Release()` | `l.Release(ctx)`, `ref.Release(ctx)` |
| `l.Log = logger` | `l.SetLogger(logger)` |
| `l, err = l.Acquire(ctx)` | `err = l.Acquire(ctx)` |
| `ByteCache.Generation.Load()`, `Resident.Generation.Load()` | `GenerationOf(key)` (per namespace) |
| `r := rangeread.New(...)`; `r.Cfg` | `r, err := rangeread.New(...)`; `r.Config()` |
| `storetest.New(t)`, `storetest.NewFaulty(t)` | `bucket.New(t)`, `bucket.NewFaulty(t)` (`objstore/storetest/bucket`) |

Encrypted objects are now written in format v2, which v0.4 readers cannot
read: upgrade every reader before any writer. See [CHANGELOG.md](CHANGELOG.md)
for the rest, including the v1 empty-object rule.

## Methods

| method | does |
| --- | --- |
| `Put(ctx, key, data)` | write, overwriting |
| `PutIfAbsent(ctx, key, data) (bool, error)` | write only if the key is absent |
| `PutIfMatch(ctx, key, data, etag) (bool, error)` | write only if the ETag still matches |
| `Get(ctx, key)` | read the object |
| `GetRange(ctx, key, off, n)` | read exactly `n` bytes from `off` |
| `GetWithETag(ctx, key)` | read the object and its ETag |
| `GetIfChanged(ctx, key, etag)` | read only if the ETag moved |
| `List(ctx, prefix)` | every key under a prefix |
| `ListPage(ctx, prefix, after, limit)` | one page of keys (`limit` 1 to `MaxListPage`, 1,000) |
| `ListPrefixes(ctx, prefix)` | child prefixes under a prefix (S3 delimiter `/`) |
| `ListPrefixesPage(ctx, prefix, after, limit)` | one page of child prefixes |
| `Delete(ctx, key)` | delete one key; never needs the namespace key |
| `DeleteMany(ctx, keys...)` | batch delete |
| `EnsureBucket(ctx)`, `DropBucket(ctx)` | create or remove the bucket |

`ctx = objstore.WithTimings(ctx, &t)` has the store add the wall time and
count of every call made under `ctx` to `t`, per `objstore.Call`;
`logger.LogAttrs(ctx, slog.LevelInfo, "job", t.Attrs()...)` logs them. The
file store fills every Call; S3 and GCS fill only the write gate.

## Write-ahead log

1. Make your row a `wal.Record`: `Size() int` and `AppendTo(b []byte) ([]byte, error)`. `wal.Bytes` is one already.
2. Start a writer: `w := wal.NewWriter(s, "log/", 1, onCommit)`. `onCommit(seq, at, records)` runs for each durable entry, before callers are acked.
3. Write: `err := w.Append(ctx, records)`. It returns once the entry holding them is durable. If `ctx` ends first the outcome is unknown; `w.Enqueue` returns a receipt that always gets the verdict.
4. Read back: `wal.Walk(ctx, s, "log/", after, 0, wal.Decode, visit)`. `visit` gets one `wal.Entry` per entry; a batch cut into several pages arrives whole. `wal.WalkParallel` fetches on `workers` goroutines; `wal.WalkWithGet` and `wal.WalkParallelWithGet` read through your own fetch (a cache, say).
5. Stop: `w.Close()` drains, then returns.

| error | means |
| --- | --- |
| `ErrOverloaded` | 128 MiB is already waiting for the store; retry later |
| `ErrRecordTooLarge` | a record no page can hold, or an append bigger than the queue; refused at once |
| `ErrInvalidRecord` | negative `Size`, or `AppendTo` failed or disagreed with `Size`; only that Append fails |
| `ErrLostRace` | another writer took the sequence (split brain). Finishes the writer |
| `ErrUnresolved` | the outcome is unknown; the records may be durable. Reopen from your checkpoint and the log. From a commit (covered floor, corrupt page) it finishes the writer |
| `ErrWriterFailed` | the writer had already finished; these records were not written. Open a new writer from the checkpoint |
| `ErrCorrupt` | a page failed its checksum or framing, or a page is missing inside a bounded walk |

A finished writer never writes again: `w.Stats().Terminal` says why, and
every later Append gets `ErrWriterFailed`. Concurrent `Append`s share one
entry, at most one per `WithCommitInterval` (default 1 s, clamped to 1 ms to
5 s). An idle writer commits at once. Entries over 32 MiB are cut into pages.
Nothing is listed: the walk GETs `after+1`, `after+2`, ... and stops at the
first missing key. One live writer per prefix, enforced by the caller (a
lease); `SetFloor` refuses claims at or below a truncated watermark.

## Cache and ranged reads

1. Build the tiers: `disk, _ := cache.NewDisk(dir, 10<<30)`, then `c := cache.New(s, 1<<30, disk, cache.Keys{})`.
2. Read through them: `b, err := c.FetchWith(ctx, key, func(ctx context.Context) ([]byte, error) { return s.Get(ctx, key) })`.
3. For byte ranges: `r, err := rangeread.New(s, c, rangeread.Config{})` (zero fields take defaults, invalid ones are an error; `r.Config()` shows the result), then `scope, err := r.FetchRanges(ctx, loads)` and `cache.Scoped(scope, key)` per load.

| piece | does |
| --- | --- |
| `cache.ByteCache` | striped LRU under one byte budget, each entry charged `len(key) + cache.EntryOverhead` extra; decoded values ride on their bytes (`PutDecoded`, charged via `cache.Sizer`; `Recharge` re-sizes one you still hold) |
| `ByteCache.GenerationOf(key)` | the key's namespace generation: read it before loading and pass it to `Put`, so an invalidation in between drops the fill |
| `cache.Disk` | disposable disk tier, 4 KiB block checksums, pins, inactivity expiry; `Disk.Logger` (nil = `slog.Default()`) |
| `cache.Keys` | tells the cache which keys are log pages (own share), low priority, or ranged |
| `cache.WithRequestStats` | per-request lookups through every tier as memory hits, disk hits and loads (`ClassCounts`, `HitRatio`); `Cache.ClassCounts` is the process's |
| `rangeread.Plan` | unions and coalesces extents under gap, extra-byte and range limits |

`Cache.Put` writes through to the memory and disk tiers, so ranged reads of an
object this process wrote are cache hits. Objects are immutable: the cache
never re-validates a key. A key under `ns/<name>/` belongs to namespace
`<name>` for `InvalidateNamespace`, pins and expiry; after
`InvalidateNamespace`, Unpin or re-Pin that namespace's disk pins. A loader
panic comes back as an error, its stack to `Cache.Logger`. `NewDisk` locks its
directory and removes stale ones a crashed process left under `dir`.

## Leader election / single writer on S3

1. Acquire: `l, err := lease.Acquire(ctx, s, "jobs/leader", lease.OwnerID(), lease.DefaultTTL)`. `ErrNotOwner` means someone else holds it; retry later. A long-lived caller that retries keeps one handle (`l := lease.New(...)`, then `err := l.Acquire(ctx)` in its loop): the handle remembers its unresolved writes and adopts one that landed late, where a fresh handle waits it out (1.5 TTL).
2. Say what losing it costs: `l.Start(func() { cancelWork() })`. The lease renews itself every TTL/4 from `Acquire` on. `l.SetLogger(logger)` logs renewal failures and fences.
3. Before every guarded action: `if err := l.Valid(); err != nil { stop }`.
4. Hand it over when done: `l.Release(ctx)`. The next process takes it at once instead of waiting out the TTL. If `ctx` ends first, the lease stops locally and the handover is skipped.

| event | what happens |
| --- | --- |
| renewal CAS loses to another owner | `ErrNotOwner`, the lease fences: your `Start` callback runs once on its own goroutine (`Done()` waits for it), `Valid` refuses from then on |
| renewal fails without proof (timeout, 5xx) | one immediate retry; still a holder until the local deadline, fenced once it passes |
| holder partitioned or crashed | nobody takes the key until its stored expiry plus TTL/2 (the clock-skew margin) |
| PUT landed but the answer was lost | the object is read back: our own nonce there means held, not lost |
| an earlier lost PUT lands late | its nonce was recorded as pending, so the renewal adopts it instead of fencing itself |

Every write carries a fresh nonce, so the read-back tells "my write landed" from "someone else's did" without guessing. The holder's deadline runs on its local monotonic clock from before the PUT, so it stops acting no later than a taker may start. Same TTL across the fleet, clock error below TTL/2.

- The fence callback may call `Release` or `Retire`; neither runs it or waits for it. It must not wait on `Done()`.
- `lease.Shared` shares one lease among several holders in a process by reference count; the last `Ref.Release(ctx)` hands it back.
- `l.CheckHeadOnRenewal(fn)` runs `fn` after each renewal. It fences only when `fn`'s error wraps `ErrNotOwner`; any other error is logged and retried, never a fence, since the renewal already extended the lease.
- A lease check cannot fence a write already in flight. Guard durable writes with their own CAS (the ETag, or a sequence), not a post-check.

## Encryption

1. Pick a provider: `kms.LocalFile{Dir: dir}` (key names `local:<file>`), `awskms.Provider{Client: c}` (`aws:arn:aws:kms:...`), `gcpkms.Provider{Client: c}` (`gcp:projects/.../cryptoKeys/<k>`), or a `kms.Router` over several:
   ```go
   p := kms.Router{Routes: map[string]kms.KeyProvider{
       "local:":      kms.LocalFile{Dir: "/etc/objstore/keys"},
       awskms.Scheme: awskms.Provider{Client: awsClient},
       gcpkms.Scheme: gcpkms.Provider{Client: gcpClient},
   }}
   ```
   The longest matching scheme wins; a name matching none goes to `Default` as `DefaultScheme+name`. Import only the provider packages you use.
2. Turn it on: `objstore.Config{KeyProvider: p}` at open (or `s.ConfigureCMEK(p)` before first use). Every object under `ns/<name>/` of a name with a key record is now encrypted with that name's data key.
3. Before the first write for a name, wrap a fresh `kms.DEKSize`-byte key and install it: `wrapped, ver, err := p.Wrap(ctx, keyName, dek)`, then `s.InstallNamespaceKey(ctx, name, objstore.Envelope{Mode: objstore.EncryptionCustomerManaged, KeyName: keyName, KeyVersion: ver, DEKWrapped: wrapped})`.
4. Rotate with `s.RotateNamespaceKey(ctx, name)`. It re-wraps the data key, so no object is rewritten.
5. Check access with `s.CheckNamespaceKey(ctx, name)`. A revoked key returns `kms.ErrKeyUnavailable`. `Delete` and `DeleteMany` never need the key, so crypto-shredding works after revocation.

Reserved key space: `ns/<name>/` is encrypted per name and `cmek/<name>` holds
its key record; store nothing else there. `Config.AcceptPlaintext` decides
which header-less bytes in an encrypted namespace pass as plaintext (nil =
`objstore.DefaultAcceptPlaintext`); anything it accepts can be forged by
whoever can write the bucket. While a provider is set, every write and range
read in a namespace without a key record first reads `cmek/<name>` (one GET,
shared). `LocalFile` rotation: copy `Dir/<name>` to `Dir/<name>.<version>`,
then replace `Dir/<name>`; keep the old file until every key is re-wrapped.

## Tests with faults

1. `s := bucket.New(t)` (`objstore/storetest/bucket`) gives a fresh bucket per test, dropped at cleanup.
2. `s, f := bucket.NewFaulty(t)` wraps it in a `*storetest.Fault`; `storetest.Faulty(t, s)` wraps a Store you opened yourself.
3. Arm one crash point: `f.Set(storetest.Plan{Op: storetest.OpPut, N: 2, Key: "manifest", Mode: storetest.Ambiguous})`.
   `Fail` errors before the write, `Ambiguous` writes and then errors, `Hang` blocks until the call's context ends (use `t.Context()`), `Pause` blocks until `f.Resume()`.
4. Read what happened: `f.Fired()`, `f.Ops()`, `f.ReadKeys()`, `f.WriteKeys()`, `f.ReadBytes()`, `f.WriteBytes()`.

`f.SetShape(storetest.Shape{Latency, BytesPerSecond, ErrorRate, Seed})` adds
seeded latency, bandwidth and errors to every call. `f.WatchRewrites()` flags
any key rewritten with different bytes (ciphertext, under encryption).

## Test

```sh
go test -race ./...
OBJSTORE_TEST_S3=http://localhost:9000 go test -race -skip '/ETagCASMissing' ./...   # the same suite against MinIO
```

Without `OBJSTORE_TEST_S3` every suite runs on a file bucket in a temp
directory; the S3 client's error and paging mapping runs against a fake
server either way. `storetest.Conformance(t, s)` is the suite every backend
passes; run it on your own `objstore.Backend` via `objstore.Open`.
`storetest` imports no provider, so that links neither the AWS nor the GCS
SDK; the per-test buckets live in `storetest/bucket`. MinIO creates the
object on a `PutIfMatch` of a missing key, hence the skip.

CI runs gofmt, `go mod tidy -diff`, vet, staticcheck and the race tests on
Linux; the same tests against MinIO; the tests on macOS; and `go vet` cross-compiles
for windows, darwin, illumos, solaris and aix/ppc64.

Real providers, each skipped when its variable is unset:

- `OBJSTORE_TEST_S3`: S3 endpoint, e.g. MinIO at `http://localhost:9000` (each of `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` left unset defaults to MinIO's `minioadmin`, `minioadmin`, `us-east-1`).
- `OBJSTORE_TEST_R2_ENDPOINT`: `https://<account-id>.r2.cloudflarestorage.com`, with an R2 API token in `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and `AWS_REGION=auto`. Runs `TestR2` in `./s3`.
- `OBJSTORE_TEST_GCS_PROJECT`: a GCP project where Application Default Credentials can create buckets. Runs `TestConformanceReal` in `./gcs` against a fresh `objstore-test-<nanos>` bucket, dropped afterwards. `TestConformance` runs on an in-process fake-gcs-server with no variable.

## License

MIT, see [LICENSE](LICENSE).
