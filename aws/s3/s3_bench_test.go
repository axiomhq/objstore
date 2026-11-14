package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func BenchmarkReadBodyKnownLength(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i*37 + 11)
			}
			n := int64(size)
			b.SetBytes(n)
			b.ReportAllocs()
			for b.Loop() {
				got, err := readBody("get-range", "pack", io.NopCloser(bytes.NewReader(data)), &n)
				if err != nil || !bytes.Equal(got, data) {
					b.Fatalf("body mismatch: %v", err)
				}
			}
		})
	}
}

// BenchmarkGetRange is one GetRange against a local endpoint in region
// "auto", R2's: the SDK's endpoint rules compile each partition's region
// regexp for a region they do not list, on every request.
func BenchmarkGetRange(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("x"))
	}))
	defer srv.Close()
	s, err := New(b.Context(), Config{Endpoint: srv.URL, Bucket: "b", AWS: &aws.Config{Region: "auto",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, nil
		})}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.GetRange(b.Context(), "k", 0, 1); err != nil {
			b.Fatal(err)
		}
	}
}
