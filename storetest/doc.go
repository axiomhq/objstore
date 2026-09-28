// Package storetest provides provider-free test helpers: a
// fault-injecting, metering wrapper around a Store, and Conformance, the
// suite every backend passes. It imports no backend, so a third-party
// Backend's tests link no cloud SDK; package storetest/bucket opens a
// fresh file- or S3-backed bucket per test.
package storetest
