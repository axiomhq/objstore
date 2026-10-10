# objstore

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/axiomhq/objstore)
[![CI](https://github.com/axiomhq/objstore/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/axiomhq/objstore/actions/workflows/ci.yml)

objstore is object storage with compare-and-swap, for Go.

S3 and Google Cloud Storage can write an object only if it doesn't exist yet,
or only if it hasn't changed since you read it. That's compare-and-swap, and
it's enough to build a write-ahead log, a leader lease, or a manifest that many
processes update, with nothing but a bucket. objstore gives you a Store with
those conditional writes, and builds the log, the lease and a read cache on top
of it.

Objects are meant to be written once and never changed, except by deletion.
The conditional writes cover the few that must change.

## Install

```sh
go get github.com/axiomhq/objstore
```

Each provider is its own package, so a binary links only the SDKs it uses.

- [aws/s3][s3] is AWS S3, and anything that speaks the S3 API: Cloudflare R2, MinIO, Ceph, Hetzner.
- [gcp/gcs][gcs] is Google Cloud Storage.
- [fs][fs] is a local directory. Every object is fsynced before it's acknowledged. Unix only, and not Solaris or AIX.

[s3]: https://pkg.go.dev/github.com/axiomhq/objstore/aws/s3
[gcs]: https://pkg.go.dev/github.com/axiomhq/objstore/gcp/gcs
[fs]: https://pkg.go.dev/github.com/axiomhq/objstore/fs

## Usage

### Open a store

```go
s, err := s3.Open(ctx, s3.Config{
	Endpoint: "https://s3.us-east-1.amazonaws.com",
	Bucket:   "my-bucket",
}, objstore.Config{})
if err != nil {
	return err
}
```

The other providers look the same.

```go
s, err := gcs.Open(ctx, gcs.Config{Bucket: "my-bucket"}, objstore.Config{})
```

```go
s := fs.Open("/var/lib/data", "my-bucket", objstore.Config{})
```

For R2, set Endpoint to `https://<account-id>.r2.cloudflarestorage.com` and
`AWS_REGION=auto`. Anything else that implements objstore.Backend becomes a
Store with objstore.Open.

objstore.Config paces requests (RequestsPerSecond) and bounds the writes in
flight (MaxInflightWrites). Both exist because a burst past what a provider
serves doesn't fail, it stalls, and takes your heartbeats down with it.

Call s.EnsureBucket(ctx) if the bucket might not exist yet. Then, before the
first write, call s.CheckConditionalWrites(ctx). It spends three requests
proving the store actually honours If-None-Match. A store that ignores the
header would let two log writers both win the same sequence, and you'd rather
find that out at startup.

### Compare-and-swap

```go
for {
	body, etag, err := s.GetWithETag(ctx, "manifest")
	if err != nil {
		return err
	}
	ok, err := s.PutIfMatch(ctx, "manifest", update(body), etag)
	switch {
	case errors.Is(err, objstore.ErrConflict):
		continue // the store never decided; retry
	case err != nil:
		return err
	case !ok:
		continue // someone else won; read again
	}
	return nil
}
```

PutIfMatch returns false when someone else got there first. ErrConflict is
different: S3 answered 409 because another conditional write on the same key
was in flight, and never decided yours. Retry both.

PutIfAbsent is the write-once version: it returns false if the key already
existed. GetIfChanged polls a key against the ETag you last saw. On S3 and GCS
that's one conditional request; the file store reads the whole file either way.
A missing key returns an error wrapping objstore.ErrNotFound.

Heartbeats and log commits shouldn't wait behind bulk uploads. Mark their
context with objstore.Urgent(ctx), and their calls skip pacing and the write
bound. On the file store they're also written whole, instead of in chunks.

To see where a request spent its time, attach a Timings to its context.

```go
var t objstore.Timings
ctx = objstore.WithTimings(ctx, &t)
// ... calls on s ...
logger.LogAttrs(ctx, slog.LevelInfo, "job", t.Attrs()...)
```

The file store fills in every call; S3 and GCS fill in only the time spent
waiting on the write bound.

Ranged reads, paginated and delimited listing, and batch delete are on
[pkg.go.dev](https://pkg.go.dev/github.com/axiomhq/objstore#Store).

### Write-ahead log

[Package wal][wal] is a write-ahead log on a Store. Concurrent appends are
batched into one entry per commit interval, one second by default, and each
entry claims the next sequence number with a conditional PUT.

[wal]: https://pkg.go.dev/github.com/axiomhq/objstore/wal

```go
w := wal.NewWriter(s, "log/", 1, func(seq uint64, at time.Time, records []wal.Bytes) {
	// Runs for every durable entry, before its callers are acked.
})
defer w.Close()

if err := w.Append(ctx, []wal.Bytes{wal.Bytes("hello")}); err != nil {
	return err
}
```

A record is anything with Size and AppendTo; wal.Bytes is one that's already
encoded. Read the log back with Walk.

```go
err := wal.Walk(ctx, s, "log/", 0, 0, wal.Decode, func(e wal.Entry[[][]byte]) error {
	for _, page := range e.Pages {
		for _, record := range page {
			fmt.Printf("%d: %s\n", e.Seq, record)
		}
	}
	return nil
})
```

Nothing is listed. The sequence numbers are the catalog: a walk GETs after+1,
after+2, and so on, and stops at the first missing key. That works because a
writer advances only on a proven outcome, so a crashed writer leaves the log
short, never holey.

Append returning nil means the records are durable and onCommit has run. Most
errors mean they weren't written and won't be. Two don't: ErrUnresolved, and
the context's own error when ctx ends first. Then the records may be durable,
so recover by replaying the log from your checkpoint, not by appending them
again. When the store can't keep up, Append fails fast with ErrOverloaded
instead of queueing without bound.

A prefix has one live writer, and keeping it that way is your job (a lease,
say). Conditional PUT detects a second writer, as ErrLostRace; it doesn't
prevent one. The package docs cover every error, and when each one finishes
the writer.

### Leases

[Package lease][lease] is a single-holder lease on one key, for leader election
or a single writer.

[lease]: https://pkg.go.dev/github.com/axiomhq/objstore/lease

```go
l, err := lease.Acquire(ctx, s, "jobs/leader", lease.OwnerID(), lease.DefaultTTL)
if errors.Is(err, lease.ErrNotOwner) {
	return nil // someone else holds it; try again later
}
if err != nil {
	return err
}
defer l.Release(context.WithoutCancel(ctx))

ctx, cancel := context.WithCancel(ctx)
defer cancel()
l.Start(cancel) // runs once, if we lose the lease

for {
	if err := l.Valid(); err != nil {
		return err
	}
	if err := doGuardedWork(ctx); err != nil {
		return err
	}
}
```

The lease renews itself every TTL/4. Release hands it over at once, so the next
process doesn't wait out the TTL.

Every write carries a fresh nonce. When a PUT lands but its answer is lost, the
lease reads the object back, and its own nonce there means it still holds the
lease. Nothing is guessed. A holder that's partitioned or crashed blocks
takeover until its stored expiry plus TTL/2, and the holder's own deadline runs
on its monotonic clock from before the PUT. So it stops acting no later than a
taker may start, as long as the fleet shares one TTL and clocks disagree by
less than TTL/2.

A lease check can't fence a write that's already in flight. Guard durable
writes with their own compare-and-swap, on an ETag or a sequence number, not
with a check of the lease.

A long-lived caller that retries should keep one handle, from lease.New, and
call its Acquire method in a loop. lease.Shared shares one lease among holders
in a process by reference count.

### Cache and ranged reads

[Package cache][cache] puts memory and an optional disk tier in front of a
Store. Concurrent misses of one key share one GET.

[cache]: https://pkg.go.dev/github.com/axiomhq/objstore/cache

```go
disk, err := cache.NewDisk(dir, 10<<30) // 10 GiB on disk
if err != nil {
	return err
}
c := cache.New(s, 1<<30, disk, cache.Keys{}) // 1 GiB in memory

b, err := c.FetchWith(ctx, key, func(ctx context.Context) ([]byte, error) {
	return s.Get(ctx, key)
})
```

Objects are immutable, so the cache never revalidates a key. Keys under
`ns/<name>/` belong to namespace `<name>`, and InvalidateNamespace drops one
namespace from every tier without touching the others.

[Package rangeread][rangeread] reads many byte ranges through the cache,
coalescing nearby ones into one GET.

[rangeread]: https://pkg.go.dev/github.com/axiomhq/objstore/rangeread

```go
r, err := rangeread.New(s, c, rangeread.Config{})
if err != nil {
	return err
}
scope, err := r.FetchRanges(ctx, loads)
if err != nil {
	return err
}
b, ok := cache.Scoped(scope, loads[0].Key)
```

### Encryption

Tell the store which KMS key each object gets.

```go
s = s.WithKMSKeys(func(ctx context.Context, key string) (string, error) {
	tenant, _, _ := strings.Cut(key, "/")
	return keyFor(tenant), nil // "" means the bucket's own policy
})
```

Then write and read as usual. S3 stores each keyed object with SSE-KMS under
that key, GCS with that kmsKeyName. Reads need no key. Rotating a key in KMS
needs nothing from you; disabling or deleting one makes its objects fail with
an error wrapping objstore.ErrAccessDenied.

The file store ignores keys and encrypts nothing; s.KMS() reports false there.
s3.Config's SSE and KMSKeyID still set one policy for the whole bucket.

### Testing your code

[Package storetest][storetest] injects faults, and
[storetest/bucket][bucket] opens a fresh bucket per test, dropped at cleanup.
That's a temp directory by default, or S3 when `OBJSTORE_TEST_S3` is set.

[storetest]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest
[bucket]: https://pkg.go.dev/github.com/axiomhq/objstore/storetest/bucket

```go
func TestManifestSurvivesLostAnswer(t *testing.T) {
	s, f := bucket.NewFaulty(t)

	// The first PutIfMatch on the manifest lands, then reports an error.
	f.Set(storetest.Plan{Op: storetest.OpPutIfMatch, N: 1, Key: "manifest", Mode: storetest.Ambiguous})

	// ... run the code under test against s ...

	if f.Fired() != 1 {
		t.Fatal("fault never fired")
	}
}
```

Fail errors before the write, Ambiguous writes and then errors, Hang blocks
until the call's context ends, and Pause blocks until f.Resume(). f.SetShape
adds seeded latency, bandwidth limits and errors to every call, and
f.WatchRewrites flags any key rewritten with different bytes.
storetest.NewKMS records which key encrypted each object, and can revoke one.

Writing your own Backend? Wrap it with objstore.Open and run
storetest.Conformance(t, s) on it. It's the suite every provider here passes.
storetest imports no provider, so it links neither the AWS nor the GCS SDK.

## Migrating from v0.4

| v0.4 | v0.5 |
| --- | --- |
| S3 in package `objstore` | package `s3` at `objstore/aws/s3`; GCS (new) is `objstore/gcp/gcs` |
| `objstore.New(ctx, endpoint, bucket)` | `s3.Open(ctx, s3.Config{Endpoint: endpoint, Bucket: bucket}, objstore.Config{})`; for `file://root`, `fs.Open(root, bucket, objstore.Config{})` |
| `Config.Endpoint`, `Bucket`, `SSE`, `KMSKeyID`, `AllowedEndpoints`, `RequestTimeout` | `s3.Config` |
| `Store.SSE()`, `objstore.ErrEndpointDenied` | gone: set it with `s3.Config.SSE`; `s3.ErrEndpointDenied` |
| `s.ConfigureCMEK(p)`, `InstallNamespaceKey`, `RotateNamespaceKey`, `CheckNamespaceKey`, package `kms` | gone: `s = s.WithKMSKeys(fn)`, and the store encrypts (see [Encryption](#encryption)) |
| `Timings.LogAttrs()` | `Timings.Attrs()` |
| `l.Release()`, `ref.Release()` | `l.Release(ctx)`, `ref.Release(ctx)` |
| `l.Log = logger` | `l.SetLogger(logger)` |
| `l, err = l.Acquire(ctx)` | `err = l.Acquire(ctx)` |
| `ByteCache.Generation.Load()`, `Resident.Generation.Load()` | `GenerationOf(key)` (per namespace) |
| `r := rangeread.New(...)`; `r.Cfg` | `r, err := rangeread.New(...)`; `r.Config()` |
| `storetest.New(t)`, `storetest.NewFaulty(t)` | `bucket.New(t)`, `bucket.NewFaulty(t)` (`objstore/storetest/bucket`) |

Objects that v0.4 sealed client-side stay ciphertext: v0.5 doesn't decrypt
them. See [CHANGELOG.md](CHANGELOG.md) for the rest.

## Development

```sh
go test -race ./...
```

Without any configuration, every suite runs on a file bucket in a temp
directory, and the S3 and GCS clients run against fake servers. Set these to
run against real stores; each test is skipped when its variable is unset.

- `OBJSTORE_TEST_S3` is an S3 endpoint, e.g. MinIO at `http://localhost:9000`. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION` default to MinIO's `minioadmin`, `minioadmin` and `us-east-1` when unset.
- `OBJSTORE_TEST_R2_ENDPOINT` is `https://<account-id>.r2.cloudflarestorage.com`, with an R2 API token in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION=auto`. It runs `TestR2` in `./aws/s3`.
- `OBJSTORE_TEST_GCS_PROJECT` is a GCP project where Application Default Credentials can create buckets. It runs `TestConformanceReal` in `./gcp/gcs` against a fresh `objstore-test-<nanos>` bucket, dropped afterwards.

MinIO creates the object on a PutIfMatch of a missing key, where AWS S3 and R2
answer 404, so skip that subtest there.

```sh
OBJSTORE_TEST_S3=http://localhost:9000 go test -race -skip '/ETagCASMissing' ./...
```

CI runs gofmt, `go mod tidy -diff`, vet, staticcheck and the race tests on
Linux, the same tests against MinIO, and the tests on macOS. It also cross-vets
for windows, darwin, illumos, solaris and aix/ppc64.

## License

MIT, see [LICENSE](LICENSE).
