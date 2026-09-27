// Package fs is the file backend for objstore: one bucket as a directory
// of write-once files. Every object is fsynced before it is acknowledged,
// and large objects are written back in chunks so a small urgent write
// never waits behind a big one. Unix-only (flock): elsewhere the package
// builds but every operation fails with errors.ErrUnsupported. One writer
// process per bucket, as on S3.
package fs
