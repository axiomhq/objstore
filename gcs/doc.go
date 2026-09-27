// Package gcs is the Google Cloud Storage backend for objstore.
//
// The ETag this backend hands out is the object generation as a decimal
// string, never the HTTP ETag: generations are what GCS preconditions
// compare (ifGenerationMatch, ifGenerationNotMatch), so PutIfAbsent,
// PutIfMatch and GetIfChanged are each one conditional request. Callers
// must treat it as opaque. New selects the JSON read API because the XML
// read path drops ifGenerationNotMatch, which would turn every
// GetIfChanged into a full download.
//
// GCS allows roughly one mutation per second to a single object name;
// faster updates are answered 429 (the client retries conditional writes
// with backoff). Distinct keys are unaffected, so WAL appends are fine.
// For leases: lease.DefaultTTL (10 s) renews every TTL/4 = 2.5 s, well
// inside the limit; a TTL under 4 s renews more than once a second and
// will be throttled.
//
// Config.Options is passed to storage.NewClient. Point it at an emulator
// such as fake-gcs-server with option.WithEndpoint("http://host:4443/storage/v1/")
// and option.WithoutAuthentication(); by default the client uses
// Application Default Credentials.
package gcs // import "github.com/axiomhq/objstore/gcs"
