# objstore

```sh
go get github.com/axiomhq/objstore
```

Object storage with compare-and-swap, for Go. One `Store` over an S3 bucket
(AWS, MinIO, any S3-compatible service) or a durable local directory, with:

- conditional PUT and GET, and the ETag with the body
- paginated list, delimited list, batch delete
- a `file://` store that fsyncs every object and writes large ones back in chunks, so a small urgent write never waits behind a big one
- envelope encryption per name, with local, AWS KMS and GCP KMS key providers (`objstore/kms`)
- fault injection and metering for tests (`objstore/storetest`)
- a write-ahead log: one entry per second, conditional PUT, nonce read-back (`objstore/wal`)
- a memory and disk object cache with singleflight and per-request stats (`objstore/cache`)
- a ranged reader that coalesces nearby reads into one GET (`objstore/rangeread`)

## Use

1. Open a store: `s, err := objstore.New(ctx, "https://s3.us-east-1.amazonaws.com", "my-bucket")`.
   Use `"file:///var/lib/data"` for a local store. `objstore.NewConfigured(ctx, objstore.Config{...})` adds SSE, rate limits, timeouts and a write bound.
2. Create the bucket if you need to: `s.EnsureBucket(ctx)`.
3. Write once: `created, err := s.PutIfAbsent(ctx, key, data)`. `created` is false when the key existed.
4. Swap: `body, etag, err := s.GetWithETag(ctx, key)`, then `ok, err := s.PutIfMatch(ctx, key, next, etag)`. `ok` false means someone else won: re-read and retry.
5. Poll cheaply: `body, etag, changed, err := s.GetIfChanged(ctx, key, etag)`.

A missing key returns an error wrapping `objstore.ErrNotFound`. Mark a
context with `objstore.Urgent(ctx)` to skip pacing and the write bound, for
heartbeats and log commits.

## Methods

| method | does |
| --- | --- |
| `Put(ctx, key, data)` | write, overwriting |
| `PutIfAbsent(ctx, key, data) (bool, error)` | write only if the key is absent |
| `PutIfMatch(ctx, key, data, etag) (bool, error)` | write only if the ETag still matches |
| `Get(ctx, key)` | read the object |
| `GetRange(ctx, key, off, n)` | read `n` bytes from `off` |
| `GetWithETag(ctx, key)` | read the object and its ETag |
| `GetIfChanged(ctx, key, etag)` | read only if the ETag moved |
| `List(ctx, prefix)` | every key under a prefix |
| `ListPage(ctx, prefix, after, limit)` | one page of keys (`limit` ≤ `MaxListPage`, 1,000) |
| `ListPrefixes(ctx, prefix)` | child prefixes under a prefix (S3 delimiter `/`) |
| `ListPrefixesPage(ctx, prefix, after, limit)` | one page of child prefixes |
| `Delete(ctx, key)` | delete one key |
| `DeleteMany(ctx, keys...)` | batch delete |
| `EnsureBucket(ctx)`, `DropBucket(ctx)` | create or remove the bucket |

`ctx = objstore.WithTimings(ctx, &t)` has the store add the wall time of
every call made under `ctx` to `t`, per call kind; `t.LogAttrs()` returns
them as `slog` arguments.

## Write-ahead log

1. Make your row a `wal.Record`: `Size() int` and `AppendTo(b []byte) ([]byte, error)`. `wal.Bytes` is one already.
2. Start a writer: `w := wal.NewWriter(s, "log/", 1, onCommit)`. `onCommit(seq, at, records)` runs for each durable entry, before callers are acked.
3. Write: `err := w.Append(ctx, records)`. It returns once the entry holding them is durable.
4. Read back: `wal.Walk(ctx, s, "log/", after, 0, wal.Decode, visit)`. `visit` gets one `wal.Entry` per entry; a batch cut into several pages arrives whole.
5. Stop: `w.Close()` drains, then returns.

| error | means |
| --- | --- |
| `ErrOverloaded` | 128 MiB is already waiting for the store; retry later |
| `ErrLostRace` | another writer took the sequence (split brain) |
| `ErrUnresolved` | the outcome is unknown; reopen from your checkpoint and the log |
| `ErrCorrupt` | a page failed its checksum or framing, or a page is missing inside a bounded walk |

Concurrent `Append`s share one entry, at most one per `WithCommitInterval` (default 1 s). An idle writer commits at once. Entries over 32 MiB are cut into pages. Nothing is listed: the walk GETs `after+1`, `after+2`, ... and stops at the first missing key. `SetFloor` refuses claims at or below a truncated watermark.

## Cache and ranged reads

1. Build the tiers: `disk, _ := cache.NewDisk(dir, 10<<30)`, then `c := cache.New(s, 1<<30, disk, cache.Keys{})`.
2. Read through them: `b, err := c.FetchWith(ctx, key, func(ctx context.Context) ([]byte, error) { return s.Get(ctx, key) })`.
3. For byte ranges: `cfg, _ := rangeread.Config{}.Normalized()`, `r := rangeread.New(s, c, cfg)`, then `scope, err := r.FetchRanges(ctx, loads)` and `cache.Scoped(scope, key)` per load.

| piece | does |
| --- | --- |
| `cache.ByteCache` | striped LRU under one byte budget; decoded values ride on their bytes (`PutDecoded`, charged via `cache.Sizer`) |
| `cache.Disk` | disposable disk tier, 4 KiB block checksums, pins, inactivity expiry |
| `cache.Keys` | tells the cache which keys are log pages (own share), low priority, or ranged |
| `cache.WithRequestStats` | per-request hits and misses through every tier |
| `rangeread.Plan` | unions and coalesces extents under gap, extra-byte and range limits |

Objects are immutable: the cache never re-validates a key. A key under `ns/<name>/` belongs to namespace `<name>` for `InvalidateNamespace`, pins and expiry.

## Encryption

1. Pick a provider: `kms.LocalFile{Dir: dir}`, `kms.AWSKMS{Client: c}`, `kms.GCPKMS{Client: c}`, or `kms.Router{Local, AWS, GCP, Default}` to route by key name.
2. Turn it on: `s.ConfigureCMEK(provider)`. Every object under `ns/<name>/` is now encrypted with that name's data key.
3. Before the first write for a name, wrap a fresh 32-byte key with `provider.Wrap` and install it: `s.InstallNamespaceKey(ctx, name, objstore.Envelope{Mode: objstore.EncryptionCustomerManaged, KeyName, KeyVersion, DEKWrapped})`.
4. Rotate with `s.RotateNamespaceKey(ctx, name)`. It re-wraps the data key, so no object is rewritten.
5. Check access with `s.CheckNamespaceKey(ctx, name)`. A revoked key returns `kms.ErrKeyUnavailable`.

## Tests with faults

1. `s := storetest.New(t)` gives a fresh bucket per test, dropped at cleanup.
2. `s, f := storetest.NewFaulty(t)` wraps it in a `*storetest.Fault`.
3. Arm one crash point: `f.Set(storetest.Plan{Op: storetest.OpPut, N: 2, Key: "manifest", Mode: storetest.Ambiguous})`.
   `Fail` errors before the write, `Ambiguous` writes and then errors, `Hang` blocks until cancel, `Pause` blocks until `f.Resume()`.
4. Read what happened: `f.Fired()`, `f.Ops()`, `f.ReadKeys()`, `f.WriteKeys()`, `f.ReadBytes()`, `f.WriteBytes()`.

`f.SetShape(storetest.Shape{Latency, BytesPerSecond, ErrorRate, Seed})` adds
seeded latency, bandwidth and errors to every call. `f.WatchRewrites()` flags
any key rewritten with different bytes.

## Test

```sh
go test -race ./...
OBJSTORE_TEST_S3=http://localhost:9000 go test -race ./...   # the same suite against MinIO
```

Without `OBJSTORE_TEST_S3` every suite runs on a `file://` bucket in a temp
directory; the S3 client's error and paging mapping runs against a fake
server either way.

## License

MIT, see [LICENSE](LICENSE).
