package storetest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/storetest"
	"github.com/axiomhq/objstore/storetest/bucket"
)

type deadlineBackend struct{ objstore.Backend }

func (b deadlineBackend) Put(ctx context.Context, key string, data []byte) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("conformance operation has no deadline")
	}
	return b.Backend.Put(ctx, key, data)
}

func TestConformanceContextsBounded(t *testing.T) {
	s := bucket.NewFS(t).WithBackend(func(b objstore.Backend) objstore.Backend {
		return deadlineBackend{b}
	})
	storetest.Conformance(t, s)
}
