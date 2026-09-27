package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeS3 serves canned S3 responses so the backend's error and paging
// mapping is testable without MinIO. Each handler sees the raw request.
func fakeS3(t *testing.T, handler http.HandlerFunc) *s3Store {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	s, err := newS3(context.Background(), srv.URL, "b", "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

type objectBody struct {
	data     []byte
	finalErr error
	withData bool
	closed   int
}

func (b *objectBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 && (n == 0 || b.withData) {
		return n, b.finalErr
	}
	return n, nil
}

func (b *objectBody) Close() error { b.closed++; return nil }

func TestReadBodyLengthAndFinalErrors(t *testing.T) {
	late := errors.New("checksum failed")
	for _, tc := range []struct {
		name     string
		length   int64
		unknown  bool
		data     string
		finalErr error
		withData bool
	}{
		{name: "exact", length: 5, data: "aZ123", finalErr: io.EOF},
		{name: "empty", length: 0, finalErr: io.EOF},
		{name: "unknown", unknown: true, data: "aZ123", finalErr: io.EOF},
		{name: "short", length: 6, data: "aZ123", finalErr: io.EOF},
		{name: "long", length: 4, data: "aZ123", finalErr: io.EOF},
		{name: "negative", length: -1, data: "aZ123", finalErr: io.EOF},
		{name: "absurd header", length: 1<<63 - 1, data: "aZ123", finalErr: io.EOF},
		{name: "error after exact bytes", length: 5, data: "aZ123", finalErr: late},
		{name: "error with exact bytes", length: 5, data: "aZ123", finalErr: late, withData: true},
		{name: "eof with exact bytes", length: 5, data: "aZ123", finalErr: io.EOF, withData: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &objectBody{data: []byte(tc.data), finalErr: tc.finalErr, withData: tc.withData}
			length := &tc.length
			if tc.unknown {
				length = nil
			}
			got, err := readBody("get-range", "pack", body, length)
			wantErr := tc.finalErr != io.EOF || (!tc.unknown && tc.length != int64(len(tc.data)))
			if (err != nil) != wantErr || body.closed != 1 || len(body.data) != 0 {
				t.Fatalf("err=%v closed=%d remaining=%d", err, body.closed, len(body.data))
			}
			if err == nil && string(got) != tc.data {
				t.Fatalf("body=%q, want %q", got, tc.data)
			}
			if err != nil && (got != nil || !strings.Contains(err.Error(), "get-range pack")) {
				t.Fatalf("data=%q err=%v", got, err)
			}
			if tc.finalErr == late && !errors.Is(err, late) {
				t.Fatalf("lost terminal read error: %v", err)
			}
		})
	}
}

func TestS3EnsureBucketReusesExistingAndOnlyCreatesMissing(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var heads, creates atomic.Int32
			s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					heads.Add(1)
					w.WriteHeader(status)
				case http.MethodPut:
					creates.Add(1)
					w.WriteHeader(http.StatusOK)
				default:
					t.Errorf("unexpected bucket request: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			err := s.EnsureBucket(context.Background())
			if (err != nil) != (status == http.StatusForbidden) {
				t.Fatalf("head status %d: %v", status, err)
			}
			wantCreates := int32(0)
			if status == http.StatusNotFound {
				wantCreates = 1
			}
			if heads.Load() != 1 || creates.Load() != wantCreates {
				t.Fatalf("HEAD=%d CREATE=%d, want 1 and %d", heads.Load(), creates.Load(), wantCreates)
			}
		})
	}
}

// TestS3ConditionalPutVerdicts: only PreconditionFailed is "the other side
// won". A 409 ConditionalRequestConflict means the request was never
// evaluated, and reporting it as (false, nil) would send the WAL writer to a
// read-back that finds nothing — or worse, a lease taker to a spurious
// "someone else holds it".
func TestS3ConditionalPutVerdicts(t *testing.T) {
	var code atomic.Value
	s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		c := code.Load().(string)
		switch c {
		case "PreconditionFailed":
			s3Error(w, http.StatusPreconditionFailed, c)
		case "ConditionalRequestConflict":
			s3Error(w, http.StatusConflict, c)
		case "NoSuchKey":
			s3Error(w, http.StatusNotFound, c)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	ctx := context.Background()
	for _, tc := range []struct {
		code   string
		ok     bool
		errCmp string
	}{
		{"", true, ""},
		{"PreconditionFailed", false, ""},
		{"ConditionalRequestConflict", false, "ConditionalRequestConflict"},
	} {
		code.Store(tc.code)
		ok, err := s.PutIfAbsent(ctx, "k", []byte("x"))
		if ok != tc.ok || (err != nil) != (tc.errCmp != "") || (err != nil && !strings.Contains(err.Error(), tc.errCmp)) {
			t.Fatalf("PutIfAbsent on %q: ok=%v err=%v", tc.code, ok, err)
		}
		ok, err = s.PutIfMatch(ctx, "k", []byte("x"), "etag")
		if ok != tc.ok || (err != nil) != (tc.errCmp != "") || (err != nil && !strings.Contains(err.Error(), tc.errCmp)) {
			t.Fatalf("PutIfMatch on %q: ok=%v err=%v", tc.code, ok, err)
		}
	}
	code.Store("NoSuchKey")
	if ok, err := s.PutIfMatch(ctx, "k", []byte("x"), "etag"); ok || err != nil {
		t.Fatalf("PutIfMatch on a vanished key: ok=%v err=%v", ok, err)
	}
}

// TestS3DeleteManyReportsPerKeyErrors: a batch DELETE is 200 even when keys
// failed; the failures ride in the body and must not be swallowed, or
// DropBucket's list-delete loop spins on an undeletable key forever.
func TestS3DeleteManyReportsPerKeyErrors(t *testing.T) {
	var calls atomic.Int32
	s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			calls.Add(1)
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `<?xml version="1.0"?><DeleteResult><Error><Key>d/stuck</Key><Code>AccessDenied</Code><Message>no</Message></Error></DeleteResult>`)
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>d/stuck</Key></Contents></ListBucketResult>`)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	ctx := context.Background()
	err := s.DeleteMany(ctx, "d/ok", "d/stuck")
	if err == nil || !strings.Contains(err.Error(), "d/stuck") || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("per-key delete failure swallowed: %v", err)
	}
	if err := s.DropBucket(ctx); err == nil {
		t.Fatal("DropBucket succeeded over an undeletable key")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("DropBucket issued %d batch deletes after the first failed, want 1", n-1)
	}
}

// TestS3ListPrefixesPageSkipsPrefixlessPages: MaxKeys counts objects at the
// prefix level, so a truncated page can hold zero CommonPrefixes. That page
// is not the end of the listing; the backend must follow the continuation
// token until a prefix shows up, or the caller concludes "no namespaces".
func TestS3ListPrefixesPageSkipsPrefixlessPages(t *testing.T) {
	var tokens []string
	s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("list-type") != "2" || q.Get("delimiter") != "/" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			s3Error(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		tok := q.Get("continuation-token")
		tokens = append(tokens, tok)
		w.WriteHeader(http.StatusOK)
		switch tok {
		case "":
			fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>t1</NextContinuationToken><Contents><Key>ns/loose</Key></Contents></ListBucketResult>`)
		case "t1":
			fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>t2</NextContinuationToken><CommonPrefixes><Prefix>ns/one/</Prefix></CommonPrefixes></ListBucketResult>`)
		default:
			fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>ns/two/</Prefix></CommonPrefixes></ListBucketResult>`)
		}
	})
	page, next, err := s.ListPrefixesPage(context.Background(), "ns/", "", 1)
	if err != nil || len(page) != 1 || page[0] != "ns/one/" || next != "ns/one/" {
		t.Fatalf("page=%v next=%q err=%v", page, next, err)
	}
}

func TestS3ConditionalRead(t *testing.T) {
	for _, tc := range []struct {
		name, etag string
		status     int
		body       string
	}{
		{"changed", `"old"`, 200, "new"},
		{"unconditional", "", 200, "new"},
		{"unchanged", `"old"`, 304, ""},
		{"absent", `"old"`, 404, ""},
		{"failed", `"old"`, 403, ""},
		{"unexpected304", "", 304, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.Header.Get("If-None-Match") != tc.etag {
					t.Errorf("request: %s %v", r.Method, r.Header)
				}
				if tc.status == 404 {
					s3Error(w, 404, "NoSuchKey")
					return
				}
				if tc.status == 403 {
					s3Error(w, 403, "AccessDenied")
					return
				}
				w.Header().Set("ETag", `"new"`)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			})
			data, etag, unchanged, err := s.GetIfChanged(context.Background(), "k", tc.etag)
			if calls.Load() != 1 {
				t.Fatalf("calls = %d", calls.Load())
			}
			switch tc.name {
			case "changed", "unconditional":
				if err != nil || unchanged || string(data) != "new" || etag != `"new"` {
					t.Fatalf("%q %q %v %v", data, etag, unchanged, err)
				}
			case "unchanged":
				if err != nil || !unchanged || data != nil || etag != tc.etag {
					t.Fatalf("%q %q %v %v", data, etag, unchanged, err)
				}
			default:
				if err == nil || unchanged || data != nil || etag != "" {
					t.Fatalf("%q %q %v %v", data, etag, unchanged, err)
				}
			}
		})
	}
}
