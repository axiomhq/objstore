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
