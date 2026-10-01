package gcs_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/gcp/gcs"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestGCSTranslationsPreserveCause(t *testing.T) {
	for _, op := range []string{"Get", "GetWithETag", "GetIfChanged", "GetRange", "InvalidRange"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch op {
				case "InvalidRange":
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					fmt.Fprint(w, `{"error":{"code":416,"message":"bad range"}}`)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(srv.Close)
			b, err := gcs.New(ctx, gcs.Config{Bucket: "b", Options: []option.ClientOption{
				option.WithEndpoint(srv.URL + "/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()),
			}})
			if err != nil {
				t.Fatal(err)
			}
			switch op {
			case "Get":
				_, err = b.Get(ctx, "k")
			case "GetWithETag":
				_, _, err = b.GetWithETag(ctx, "k")
			case "GetIfChanged":
				_, _, _, err = b.GetIfChanged(ctx, "k", "1")
			default:
				_, err = b.GetRange(ctx, "k", 1, 2)
			}
			if op == "InvalidRange" {
				var provider *googleapi.Error
				if !errors.Is(err, objstore.ErrRange) || !errors.As(err, &provider) || provider.Code != 416 {
					t.Fatalf("lost range provider cause: %v", err)
				}
			} else if !errors.Is(err, objstore.ErrNotFound) || !errors.Is(err, storage.ErrObjectNotExist) {
				t.Fatalf("lost not-found provider cause: %v", err)
			}
		})
	}
}

func TestGCSGetRangeRejectsWrongOffset(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=4-5" {
			t.Errorf("Range = %q", got)
		}
		w.Header().Set("Content-Range", "bytes 0-1/10")
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, "01")
	}))
	t.Cleanup(srv.Close)
	b, err := gcs.New(ctx, gcs.Config{Bucket: "b", Options: []option.ClientOption{
		option.WithEndpoint(srv.URL + "/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if data, err := b.GetRange(ctx, "k", 4, 2); !errors.Is(err, objstore.ErrRange) || data != nil {
		t.Fatalf("wrong-offset response accepted: %q %v", data, err)
	}
}
