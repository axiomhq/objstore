package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/axiomhq/objstore/storetest"
)

func TestSharedRefCount(t *testing.T) {
	s := storetest.New(t)
	ctx := context.Background()
	const ttl = time.Minute
	var sh Shared
	minted := 0
	mint := func() (*Lease, error) {
		minted++
		return Acquire(ctx, s, "shared/lease", "owner-a", ttl)
	}
	r1, err := sh.Join(mint)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := sh.Join(mint)
	if err != nil {
		t.Fatal(err)
	}
	if minted != 1 || r1.Lease() != r2.Lease() || !sh.Held() {
		t.Fatalf("second Join minted a new lease: minted=%d held=%v", minted, sh.Held())
	}
	r1.Release()
	r1.Release() // a double release must not drop r2's reference
	if err := r2.Valid(); err != nil || !sh.Held() {
		t.Fatalf("first release dropped the shared lease: %v", err)
	}
	r2.Release()
	if sh.Held() {
		t.Fatal("last release left the lease held")
	}
	// The last release hands the object back: expiry zero, free at once.
	if cur, _, err := Load(ctx, s, "shared/lease"); err != nil || !cur.Expiry.IsZero() {
		t.Fatalf("last release did not hand the lease back: %+v (%v)", cur, err)
	}

	// A fenced lease is not joined, and mint is not called for it.
	r3, err := sh.Join(mint)
	if err != nil {
		t.Fatal(err)
	}
	defer r3.Release()
	r3.Lease().Fence()
	if _, err := sh.Join(mint); !errors.Is(err, ErrNotOwner) || minted != 2 {
		t.Fatalf("joined a fenced lease: err=%v minted=%d", err, minted)
	}
	var none *Ref
	if err := none.Valid(); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("nil Ref is valid: %v", err)
	}
	none.Release()
}
