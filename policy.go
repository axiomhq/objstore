package objstore

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"time"
)

var (
	ErrEndpointDenied = errors.New("store: endpoint denied by allow-list")
)

// urgentKey marks a context whose requests bypass the pacer: a lease
// heartbeat is one request every few seconds and must never queue behind
// a merge's bulk traffic.
type urgentKey struct{}

// Urgent returns ctx marked to bypass request pacing.
func Urgent(ctx context.Context) context.Context { return context.WithValue(ctx, urgentKey{}, true) }

func isUrgent(ctx context.Context) bool { v, _ := ctx.Value(urgentKey{}).(bool); return v }

type Config struct {
	Endpoint         string
	Bucket           string
	AllowedEndpoints []string
	SSE              string
	KMSKeyID         string
	// RequestsPerSecond paces every request to the object store; 0 = no
	// pacing. A burst past what the provider serves does not fail, it
	// stalls: at ~900 requests/s against Hetzner Object Storage every
	// in-flight request hung for 13-15 s, the lease renewal among them, and
	// the namespace was fenced. Pacing below that keeps a prefetch burst
	// and the heartbeat both moving.
	RequestsPerSecond float64
	// RequestTimeout bounds one S3 request end to end; 0 = 60 s. A store
	// whose queue is deeper than that truncates large uploads mid-body
	// (MinIO answers 400 IncompleteBody) rather than finishing them.
	RequestTimeout time.Duration
	// MaxInflightWrites bounds the object writes and deletes in flight at
	// once, except those marked Urgent (lease heartbeats, log commits,
	// manifest heads); 0 = 16. A background job publishes thousands of
	// objects in a burst; without a bound they queue ahead of the write
	// path's own small requests on the store.
	MaxInflightWrites int
}

func endpointAllowed(endpoint string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	u.Path, u.RawQuery, u.Fragment = "", "", ""
	return slices.Contains(allowed, u.String())
}
