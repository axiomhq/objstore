//go:build js && wasm && integration

package r2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall/js"
	"testing"

	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/cloudflare/r2"
	"github.com/axiomhq/objstore/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Conformance(t, r2.Open(js.Global().Get("r2Bucket"), objstore.Config{}))
}

func TestWriteOutcomeUnknown(t *testing.T) {
	b := r2.New(js.Global().Get("r2LostReply"))
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	for _, condition := range []string{"absent", "match"} {
		t.Run(condition, func(t *testing.T) {
			key := "lost-reply/" + condition
			var ok bool
			var err error
			if condition == "absent" {
				ok, err = b.PutIfAbsent(t.Context(), key, []byte("committed"))
			} else {
				if err := s.Put(t.Context(), key, []byte("before")); err != nil {
					t.Fatal(err)
				}
				_, tag, readErr := s.GetWithETag(t.Context(), key)
				if readErr != nil {
					t.Fatal(readErr)
				}
				ok, err = b.PutIfMatch(t.Context(), key, []byte("committed"), tag)
			}
			if ok || err == nil || errors.Is(err, objstore.ErrConflict) {
				t.Fatalf("lost reply must be an unknown outcome, not a win or retryable conflict: %v, %v", ok, err)
			}
			if got, err := s.Get(t.Context(), key); err != nil || string(got) != "committed" {
				t.Fatalf("injected lost reply must follow an actual commit: %q, %v", got, err)
			}
		})
	}
}

func TestBindingException(t *testing.T) {
	b := r2.New(js.Global().Get("r2Throwing"))
	if _, err := b.Get(t.Context(), "exception"); err == nil || !strings.Contains(err.Error(), "store: get exception") {
		t.Fatalf("synchronous binding exception must return a contextual error: %v", err)
	}
}

func TestBindingRejection(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want error
	}{
		{"missing-bucket", 10006, objstore.ErrNotFound},
		{"missing-key", 10007, objstore.ErrNotFound},
		{"access-denied", 10003, objstore.ErrAccessDenied},
		{"invalid-key", 10020, objstore.ErrInvalidKey},
		{"invalid-range", 10039, objstore.ErrRange},
		{"internal-error", 10001, nil},
		{"rejected-precondition", 10031, nil},
	} {
		for _, shape := range []string{"code", "suffix"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				b := r2.New(js.Global().Call("r2Rejected", tc.code, shape == "code"))
				_, getErr := b.Get(t.Context(), "rejected")
				_, _, listErr := b.ListPage(t.Context(), "", "", 1)
				ok, putErr := b.PutIfMatch(t.Context(), "rejected", nil, "etag")
				if ok {
					t.Fatal("rejected write reported success")
				}
				for op, err := range map[string]error{
					"get": getErr, "list": listErr, "put-if-match": putErr,
					"delete": b.Delete(t.Context(), "rejected"),
				} {
					if err == nil || !strings.Contains(err.Error(), "R2 rejection") || errors.Is(err, objstore.ErrConflict) {
						t.Errorf("%s lost the rejection or classified it as retryable: %v", op, err)
					}
					for _, sentinel := range []error{objstore.ErrNotFound, objstore.ErrAccessDenied, objstore.ErrInvalidKey, objstore.ErrRange} {
						if errors.Is(err, sentinel) != (sentinel == tc.want) {
							t.Errorf("%s: Is(%v, %v), want identity %v", op, err, sentinel, tc.want)
						}
					}
				}
			})
		}
	}
}

func TestCancelledGetReleasesBody(t *testing.T) {
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	if err := s.Put(t.Context(), "cancelled-get", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"get", "range"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			onReady := js.FuncOf(func(js.Value, []js.Value) any { cancel(); return nil })
			defer onReady.Release()
			binding := js.Global().Call("r2CancelGet", onReady)
			b := r2.New(binding)
			var err error
			if op == "get" {
				_, err = b.Get(ctx, "cancelled-get")
			} else {
				_, err = b.GetRange(ctx, "cancelled-get", 2, 3)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation before body consumption: %v", err)
			}
			if !binding.Get("object").Get("bodyUsed").Bool() {
				t.Fatal("cancelled read left the acquired R2 body unused")
			}
		})
	}
}

func TestExactRange(t *testing.T) {
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	if err := s.Put(t.Context(), "exact-range", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	for _, span := range [][2]int64{{10, 10}, {11, 10}, {9, 10}, {1 << 53, 1}} {
		t.Run(fmt.Sprint(span), func(t *testing.T) {
			if got, err := s.GetRange(t.Context(), "exact-range", span[0], span[1]); !errors.Is(err, objstore.ErrRange) || got != nil {
				t.Fatalf("must reject a partial or ignored range, not return the whole object: %q, %v", got, err)
			}
		})
	}
}

func TestListPrefixesAfter(t *testing.T) {
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	for _, key := range []string{"after/a/1", "after/a/9", "after/b/1", "after/loose", "after/z/1"} {
		if err := s.Put(t.Context(), key, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, after := range []string{"after/a/", "after/a/5", "after/az"} {
		t.Run(after, func(t *testing.T) {
			var got []string
			for i := 0; ; i++ {
				if i > 5 {
					t.Fatal("listing did not terminate")
				}
				page, next, err := s.ListPrefixesPage(t.Context(), "after/", after, 1)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, page...)
				if next == "" {
					break
				}
				if next <= after {
					t.Fatalf("non-advancing cursor: %q after %q", next, after)
				}
				after = next
			}
			if !slices.Equal(got, []string{"after/b/", "after/z/"}) {
				t.Fatalf("prefixes past an arbitrary key: %v", got)
			}
		})
	}
}

func TestDeleteManyBatches(t *testing.T) {
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = fmt.Sprintf("batch/%04d", i)
	}
	for _, i := range []int{0, 999, 1000} {
		if err := s.Put(t.Context(), keys[i], nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteMany(t.Context(), keys...); err != nil {
		t.Fatal(err)
	}
	if got, err := s.List(t.Context(), "batch/"); err != nil || len(got) != 0 {
		t.Fatalf("batch deletion left %v: %v", got, err)
	}
	for _, i := range []int{0, 999, 1000} {
		if err := s.Put(t.Context(), keys[i], nil); err != nil {
			t.Fatal(err)
		}
	}
	lost := r2.Open(js.Global().Get("r2DeleteLostReply"), objstore.Config{})
	var failure *objstore.DeleteError
	if err := lost.DeleteMany(t.Context(), keys...); !errors.As(err, &failure) || len(failure.Failures) != len(keys) {
		t.Fatalf("lost batch reply must retain all ambiguous/unattempted outcomes: %v", err)
	}
	for i, f := range failure.Failures {
		if f.Key != keys[i] || f.Unattempted != (i == 1000) {
			t.Fatalf("wrong failure at %d: %+v", i, f)
		}
	}
	if got, err := s.List(t.Context(), "batch/"); err != nil || !slices.Equal(got, keys[1000:]) {
		t.Fatalf("first batch should commit and the second remain untouched: %v, %v", got, err)
	}
}

func TestUnsupportedOperations(t *testing.T) {
	s := r2.Open(js.Global().Get("r2Bucket"), objstore.Config{})
	if err := s.Put(t.Context(), "keep-on-drop", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	if err := s.DropBucket(t.Context()); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("DropBucket must explicitly refuse: %v", err)
	}
	if got, err := s.Get(t.Context(), "keep-on-drop"); err != nil || string(got) != "keep" {
		t.Fatalf("unsupported DropBucket changed data: %q, %v", got, err)
	}
	ctx := objstore.WithKMSKey(t.Context(), "unsupported-key")
	if err := s.Put(ctx, "kms-refused", nil); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("KMS must not be silently ignored: %v", err)
	}
	if _, err := s.Get(t.Context(), "kms-refused"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("refused KMS write landed: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.EnsureBucket(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
