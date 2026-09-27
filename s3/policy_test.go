package s3

import (
	"context"
	"errors"
	"testing"
)

func TestEndpointAllowList(t *testing.T) {
	if _, err := New(context.Background(), Config{Endpoint: "https://evil.example", Bucket: "x", AllowedEndpoints: []string{"https://s3.example"}}); !errors.Is(err, ErrEndpointDenied) {
		t.Fatalf("denied endpoint = %v", err)
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
		in := s.putInput("k", []byte("v"))
		if string(in.ServerSideEncryption) != tc.mode || tc.key != "" && *in.SSEKMSKeyId != tc.key {
			t.Fatalf("mode %q input = %+v", tc.mode, in)
		}
	}
}
