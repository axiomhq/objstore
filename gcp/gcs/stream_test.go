package gcs_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/axiomhq/objstore"
	"github.com/axiomhq/objstore/gcp/gcs"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type streamTransport func(*http.Request) (*http.Response, error)

func (f streamTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type errorReader struct{ error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.error }

func TestStreamingDownloadIntegrity(t *testing.T) {
	for _, tc := range []struct {
		name, span, body string
		offset, length   int64
		terminal, want   error
	}{
		{"checksum", "", "abcde", 0, 0, io.EOF, nil},
		{"incomplete", "bytes 2-3/10", "cd", 2, 4, io.EOF, objstore.ErrRange},
		{"incomplete open", "bytes 2-3/10", "cd", 2, 0, io.EOF, objstore.ErrRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			tr := streamTransport(func(req *http.Request) (*http.Response, error) {
				h := http.Header{"Content-Type": {"application/octet-stream"}, "X-Goog-Generation": {"123"}, "Content-Length": {"5"}}
				if tc.name == "checksum" {
					h.Set("X-Goog-Hash", "crc32c=AAAAAA==")
				}
				status := 200
				if tc.span != "" {
					status = 206
					h.Set("Content-Range", tc.span)
					h.Set("Content-Length", "2")
				}
				return &http.Response{StatusCode: status, Header: h, ContentLength: int64(len(tc.body)), Body: io.NopCloser(io.MultiReader(strings.NewReader(tc.body), errorReader{tc.terminal})), Request: req}, nil
			})
			b := newBackend(t, ctx, gcs.Config{Bucket: "b", Options: []option.ClientOption{option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: tr})}})
			r, err := b.NewReader(ctx, "key", tc.offset, tc.length)
			if err == nil {
				_, err = io.ReadAll(r)
				r.Close()
			}
			if tc.name == "checksum" {
				if err == nil || !strings.Contains(err.Error(), "CRC") {
					t.Fatalf("checksum error suppressed: %v", err)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCallerOwnedClientAndSigning(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "fixture@example.invalid", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))})
	tr := &closeTransport{RoundTripper: streamTransport(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":404}}`)), Request: req}, nil
	})}
	c, err := storage.NewClient(t.Context(), storage.WithJSONReads(), option.WithCredentials(&google.Credentials{JSON: encoded, TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fixture"})}), option.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b := newBackend(t, t.Context(), gcs.Config{Bucket: "b", Client: c})
	if _, err := gcs.New(t.Context(), gcs.Config{Bucket: "b", Client: c, Options: []option.ClientOption{option.WithoutAuthentication()}}); err == nil {
		t.Fatal("ambiguous client configuration accepted")
	}
	req, err := b.Sign(objstore.WithKMSKey(t.Context(), "projects/p/key"), "key", objstore.SignOptions{Method: "PUT", Expires: time.Minute, Headers: http.Header{"Content-Type": {"text/plain"}, "X-Goog-Meta-Owner": {"test"}}})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(req.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"content-type", "x-goog-meta-owner", "x-goog-encryption-kms-key-name"} {
		if !strings.Contains(u.Query().Get("X-Goog-SignedHeaders"), h) {
			t.Fatalf("missing signed header %s", h)
		}
	}
	if req.Method != "PUT" || req.Headers.Get("Content-Type") != "text/plain" || req.Headers.Get("x-goog-encryption-kms-key-name") != "projects/p/key" {
		t.Fatalf("request headers: %v", req.Headers)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if tr.closed.Load() != 0 {
		t.Fatal("closed caller-owned transport")
	}
	if _, err := b.Stat(t.Context(), "still-usable"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatal(err)
	}
}
