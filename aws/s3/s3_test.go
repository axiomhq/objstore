package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"

	"github.com/axiomhq/objstore"
)

// fakeS3 serves canned S3 responses so the backend's error and paging
// mapping is testable without MinIO. Each handler sees the raw request.
func fakeS3(t *testing.T, handler http.HandlerFunc) *Backend {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	s, err := New(ctx, Config{Endpoint: srv.URL, Bucket: "b", AWS: &aws.Config{
		Region: "us-east-1", HTTPClient: srv.Client(),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestS3InjectedAWSConfig(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "Credential=injected/") || !strings.Contains(auth, "/eu-west-1/s3/") {
			t.Error("request did not use injected credentials and region")
		}
		if r.URL.Path != "/b/k" {
			t.Errorf("path = %q, want /b/k", r.URL.Path)
		}
		fmt.Fprint(w, "v")
	}))
	t.Cleanup(srv.Close)
	awsCfg := aws.Config{
		Region: "eu-west-1", HTTPClient: srv.Client(),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "injected", SecretAccessKey: "test"}, nil
		}),
	}
	b, err := New(ctx, Config{Endpoint: srv.URL, Bucket: "b", AWS: &awsCfg})
	if err != nil {
		t.Fatal(err)
	}
	if b.client.Options().HTTPClient != awsCfg.HTTPClient {
		t.Fatal("injected HTTP client was replaced")
	}
	if data, err := b.Get(ctx, "k"); err != nil || string(data) != "v" {
		t.Fatalf("Get = %q, %v", data, err)
	}
	// A nil injected client keeps the backend's bounded transport default.
	awsCfg.HTTPClient = nil
	b, err = New(ctx, Config{Endpoint: srv.URL, Bucket: "b", AWS: &awsCfg, RequestTimeout: 7 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client, ok := b.client.Options().HTTPClient.(*awshttp.BuildableClient)
	if !ok || client.GetTimeout() != 7*time.Second || awsCfg.HTTPClient != nil {
		t.Fatal("default HTTP client lost timeout or mutated injected config")
	}
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
		{name: "nothing arrived", length: 6, finalErr: io.EOF},
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
			if err == nil && !tc.unknown && cap(got) != len(got) {
				t.Fatalf("cap=%d, len=%d: callers cache the slice, slack stays pinned", cap(got), len(got))
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

// TestS3ConditionalRequestConflictIsErrConflict: the 409 surfaces as
// objstore.ErrConflict so callers above the backend can tell "retry" from
// a lost race without importing the SDK.
func TestS3ConditionalRequestConflictIsErrConflict(t *testing.T) {
	s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		s3Error(w, http.StatusConflict, "ConditionalRequestConflict")
	})
	ctx := context.Background()
	if ok, err := s.PutIfAbsent(ctx, "k", []byte("x")); ok || !errors.Is(err, objstore.ErrConflict) {
		t.Fatalf("PutIfAbsent: ok=%v err=%v", ok, err)
	}
	if ok, err := s.PutIfMatch(ctx, "k", []byte("x"), "etag"); ok || !errors.Is(err, objstore.ErrConflict) {
		t.Fatalf("PutIfMatch: ok=%v err=%v", ok, err)
	}
}

// TestS3PutIfMatchSendsUnquotedETag pins the exact If-Match header: the
// ETag goes out without its quotes (the Ceph RGW workaround on PutIfMatch).
// R2 documents the quoted RFC 9110 form; changing this must be deliberate
// and re-verified with TestR2 and TestConformance.
func TestS3PutIfMatchSendsUnquotedETag(t *testing.T) {
	var got atomic.Value
	s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Values("If-Match"))
		w.WriteHeader(http.StatusOK)
	})
	for _, etag := range []string{`"abc123"`, "abc123"} {
		if ok, err := s.PutIfMatch(context.Background(), "k", []byte("x"), etag); !ok || err != nil {
			t.Fatalf("PutIfMatch(%s): ok=%v err=%v", etag, ok, err)
		}
		if h := got.Load().([]string); len(h) != 1 || h[0] != "abc123" {
			t.Fatalf("PutIfMatch(%s) sent If-Match %q, want [abc123]", etag, h)
		}
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
	if !slices.Equal(tokens, []string{"", "t1"}) {
		t.Fatalf("continuation tokens sent %q, want [\"\" t1]", tokens)
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

func TestSSEPutInput(t *testing.T) {
	for _, tc := range []struct {
		mode, key string
	}{
		{"AES256", ""},
		{"aws:kms", "arn:aws:kms:us-east-1:1:key/x"},
	} {
		s := &Backend{sse: tc.mode, kmsKeyID: tc.key}
		in := s.putInput(context.Background(), "k", []byte("v"))
		if string(in.ServerSideEncryption) != tc.mode || tc.key != "" && *in.SSEKMSKeyId != tc.key {
			t.Fatalf("mode %q input = %+v", tc.mode, in)
		}
	}
}

// TestS3GetRangeContentRangeGuard: a ranged GET is accepted only when the
// server answered that exact range. A server ignoring Range (200, whole
// body) or answering another range must be ErrRange, never data.
func TestS3GetRangeContentRangeGuard(t *testing.T) {
	const object = "0123456789"
	for _, tc := range []struct {
		name   string
		answer func(w http.ResponseWriter)
		ok     bool
	}{
		{"exact", func(w http.ResponseWriter) {
			w.Header().Set("Content-Range", "bytes 2-5/10")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, object[2:6])
		}, true},
		{"range ignored", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, object)
		}, false},
		{"other range", func(w http.ResponseWriter) {
			w.Header().Set("Content-Range", "bytes 3-6/10")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, object[3:7])
		}, false},
		{"garbled", func(w http.ResponseWriter) {
			w.Header().Set("Content-Range", "items 2-5/10")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, object[2:6])
		}, false},
		{"invalid range", func(w http.ResponseWriter) {
			s3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Range"); got != "bytes=2-5" {
					t.Errorf("Range header %q", got)
				}
				tc.answer(w)
			})
			data, err := s.GetRange(context.Background(), "k", 2, 4)
			if tc.ok {
				if err != nil || string(data) != "2345" {
					t.Fatalf("got %q, %v", data, err)
				}
				return
			}
			if !errors.Is(err, objstore.ErrRange) || data != nil {
				t.Fatalf("got %q, %v; want ErrRange", data, err)
			}
		})
	}
}

// TestS3DropBucketMissingBucket: DropBucket on a bucket that is already
// gone succeeds, whether the list or the final DeleteBucket finds it
// missing; any other failure still surfaces.
func TestS3DropBucketMissingBucket(t *testing.T) {
	for _, tc := range []struct {
		name         string
		list, delete int
		code         string
		wantErr      bool
	}{
		{"gone before list", http.StatusNotFound, 0, "NoSuchBucket", false},
		{"gone before delete", http.StatusOK, http.StatusNotFound, "NoSuchBucket", false},
		{"denied", http.StatusOK, http.StatusForbidden, "AccessDenied", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
					if tc.list != http.StatusOK {
						s3Error(w, tc.list, tc.code)
						return
					}
					fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				case r.Method == http.MethodDelete:
					s3Error(w, tc.delete, tc.code)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			if err := s.DropBucket(context.Background()); (err != nil) != tc.wantErr {
				t.Fatalf("DropBucket: %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

// TestS3KMSKeyPerObject: the Store's KMSKeys rides on the PUT as SSE-KMS
// headers, an unkeyed object carries none, and a 403 or KMS.* error comes
// back as ErrAccessDenied.
func TestS3KMSKeyPerObject(t *testing.T) {
	var sse, keyID, fail atomic.Value
	fail.Store("")
	b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
		switch code := fail.Load().(string); code {
		case "AccessDenied":
			s3Error(w, http.StatusForbidden, code)
			return
		case "KMS.DisabledException":
			s3Error(w, http.StatusBadRequest, code)
			return
		}
		sse.Store(r.Header.Get("X-Amz-Server-Side-Encryption"))
		keyID.Store(r.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))
		w.WriteHeader(http.StatusOK)
	})
	s := objstore.Open(b, objstore.Config{}).WithKMSKeys(func(_ context.Context, key string) (string, error) {
		if strings.HasPrefix(key, "a/") {
			return "key-a", nil
		}
		return "", nil
	})
	if !s.KMS() || !objstore.Open(b, objstore.Config{RequestsPerSecond: 100}).KMS() {
		t.Fatal("S3 store, paced or not, must report per-object keys")
	}
	ctx := context.Background()
	if err := s.Put(ctx, "a/x", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if sse.Load() != "aws:kms" || keyID.Load() != "key-a" {
		t.Fatalf("keyed PUT headers: sse=%v key=%v", sse.Load(), keyID.Load())
	}
	if _, err := s.PutIfMatch(ctx, "b/x", []byte("x"), "etag"); err != nil {
		t.Fatal(err)
	}
	if sse.Load() != "" || keyID.Load() != "" {
		t.Fatalf("unkeyed PUT headers: sse=%v key=%v", sse.Load(), keyID.Load())
	}
	for _, code := range []string{"AccessDenied", "KMS.DisabledException"} {
		fail.Store(code)
		if _, err := s.Get(ctx, "a/x"); !errors.Is(err, objstore.ErrAccessDenied) {
			t.Fatalf("GET on %s: %v", code, err)
		}
		if err := s.Put(ctx, "a/x", []byte("x")); !errors.Is(err, objstore.ErrAccessDenied) {
			t.Fatalf("PUT on %s: %v", code, err)
		}
	}
}

func TestS3TranslationsPreserveCause(t *testing.T) {
	for _, op := range []string{"PutIfAbsent", "PutIfMatch", "Get", "GetWithETag", "GetIfChanged", "GetRange", "InvalidRange"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			code, status, sentinel := "NoSuchKey", http.StatusNotFound, objstore.ErrNotFound
			if strings.HasPrefix(op, "Put") {
				code, status, sentinel = "ConditionalRequestConflict", http.StatusConflict, objstore.ErrConflict
			} else if op == "InvalidRange" {
				code, status, sentinel = "InvalidRange", http.StatusRequestedRangeNotSatisfiable, objstore.ErrRange
			}
			b := fakeS3(t, func(w http.ResponseWriter, _ *http.Request) { s3Error(w, status, code) })
			var err error
			switch op {
			case "PutIfAbsent":
				_, err = b.PutIfAbsent(ctx, "k", []byte("v"))
			case "PutIfMatch":
				_, err = b.PutIfMatch(ctx, "k", []byte("v"), "tag")
			case "Get":
				_, err = b.Get(ctx, "k")
			case "GetWithETag":
				_, _, err = b.GetWithETag(ctx, "k")
			case "GetIfChanged":
				_, _, _, err = b.GetIfChanged(ctx, "k", "tag")
			default:
				_, err = b.GetRange(ctx, "k", 1, 2)
			}
			var provider smithy.APIError
			if !errors.Is(err, sentinel) || !errors.As(err, &provider) || provider.ErrorCode() != code {
				t.Fatalf("lost sentinel or provider cause: %v", err)
			}
		})
	}
}

func TestS3MissingBucketPreservesCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	b := fakeS3(t, func(w http.ResponseWriter, _ *http.Request) {
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
	})
	for _, op := range []string{"Get", "GetWithETag", "GetIfChanged", "GetRange", "ListPage", "ListPrefixesPage", "Put", "PutIfAbsent", "PutIfMatch", "Delete", "DeleteMany"} {
		t.Run(op, func(t *testing.T) {
			var err error
			switch op {
			case "Get":
				_, err = b.Get(ctx, "k")
			case "GetWithETag":
				_, _, err = b.GetWithETag(ctx, "k")
			case "GetIfChanged":
				_, _, _, err = b.GetIfChanged(ctx, "k", "tag")
			case "GetRange":
				_, err = b.GetRange(ctx, "k", 0, 1)
			case "ListPage":
				_, _, err = b.ListPage(ctx, "", "", 10)
			case "ListPrefixesPage":
				_, _, err = b.ListPrefixesPage(ctx, "", "", 10)
			case "Put":
				err = b.Put(ctx, "k", []byte("v"))
			case "PutIfAbsent":
				_, err = b.PutIfAbsent(ctx, "k", []byte("v"))
			case "PutIfMatch":
				_, err = b.PutIfMatch(ctx, "k", []byte("v"), "tag")
			case "Delete":
				err = b.Delete(ctx, "k")
			case "DeleteMany":
				err = b.DeleteMany(ctx, "k")
			}
			var provider smithy.APIError
			if !errors.Is(err, objstore.ErrNotFound) || !errors.As(err, &provider) || provider.ErrorCode() != "NoSuchBucket" {
				t.Fatalf("lost missing-bucket sentinel or provider cause: %v", err)
			}
		})
	}
}

func TestS3EnsureBucketLocationConstraint(t *testing.T) {
	for _, tc := range []struct {
		region string
		custom bool
	}{
		{"us-east-1", false}, {"eu-west-1", false}, {"eu-west-1", true},
	} {
		t.Run(tc.region+map[bool]string{true: "-custom", false: "-aws"}[tc.custom], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			var body atomic.Value
			b := fakeS3(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				body.Store(string(data))
				w.WriteHeader(http.StatusOK)
			})
			// Keep transport pointed at the fake while selecting the AWS policy.
			endpoint := b.endpoint
			b.client = awss3.NewFromConfig(aws.Config{Region: tc.region, Credentials: b.client.Options().Credentials}, func(o *awss3.Options) {
				if tc.custom {
					o.BaseEndpoint = &endpoint
				} else {
					o.EndpointResolverV2 = fakeAWSResolver{endpoint}
				}
				o.UsePathStyle = true
			})
			if !tc.custom {
				b.endpoint = ""
			}
			if err := b.EnsureBucket(ctx); err != nil {
				t.Fatal(err)
			}
			got := body.Load().(string)
			want := !tc.custom && tc.region != "us-east-1"
			if strings.Contains(got, "<LocationConstraint>"+tc.region+"</LocationConstraint>") != want || (!want && got != "") {
				t.Fatalf("CreateBucket body = %q, want location constraint=%v", got, want)
			}
		})
	}
}

type fakeAWSResolver struct{ endpoint string }

func (r fakeAWSResolver) ResolveEndpoint(ctx context.Context, params awss3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	params.Endpoint = &r.endpoint
	return awss3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}

func TestS3EnsureBucketEnvironmentEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var body atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		body.Store(string(data))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-west-1"), config.WithHTTPClient(srv.Client()),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		})))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, Config{Bucket: "b", AWS: &awsCfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if got := body.Load().(string); got != "" {
		t.Fatalf("environment endpoint CreateBucket body = %q, want empty", got)
	}
}
