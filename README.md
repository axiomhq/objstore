# objstore

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/axiomhq/objstore)
[![CI](https://github.com/axiomhq/objstore/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/axiomhq/objstore/actions/workflows/ci.yml)

Object storage with compare-and-swap for Go. Supports S3, Google Cloud Storage,
Azure Blob Storage, and local files, with packages for write-ahead logs, leases,
and cached reads.

## Quick start

Requires **Go 1.26 or later**. This example uses local files on Unix; no cloud
account needed. Solaris and AIX are unsupported by the file provider.

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

## Documentation

- [Design and usage](DESIGN.md): providers, conditional writes, WAL recovery,
  leases, caching, encryption, and tests.
- [API reference](https://pkg.go.dev/github.com/axiomhq/objstore)
- [Changelog and upgrading](CHANGELOG.md)

## License

[MIT](LICENSE).
