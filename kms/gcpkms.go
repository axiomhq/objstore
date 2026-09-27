package kms

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// GCPClient is implemented by cloud.google.com/go/kms/apiv1.KeyManagementClient.
type GCPClient interface {
	Encrypt(context.Context, *kmspb.EncryptRequest, ...gax.CallOption) (*kmspb.EncryptResponse, error)
	Decrypt(context.Context, *kmspb.DecryptRequest, ...gax.CallOption) (*kmspb.DecryptResponse, error)
}

type GCPKMS struct{ Client GCPClient }

var crc32c = crc32.MakeTable(crc32.Castagnoli)

func checksum(data []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(data, crc32c)))
}

func (p GCPKMS) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if p.Client == nil || !strings.HasPrefix(keyName, "gcp:projects/") || len(dek) != 32 {
		return nil, "", ErrKeyUnavailable
	}
	out, err := p.Client.Encrypt(ctx, &kmspb.EncryptRequest{Name: strings.TrimPrefix(keyName, "gcp:"), Plaintext: dek, PlaintextCrc32C: checksum(dek)})
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Ciphertext) == 0 || out.Name == "" || !out.VerifiedPlaintextCrc32C || out.CiphertextCrc32C == nil || out.CiphertextCrc32C.Value != checksum(out.Ciphertext).Value {
		return nil, "", ErrKeyUnavailable
	}
	return out.Ciphertext, out.Name, nil
}

func (p GCPKMS) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	if p.Client == nil || !strings.HasPrefix(keyName, "gcp:projects/") {
		return nil, ErrKeyUnavailable
	}
	out, err := p.Client.Decrypt(ctx, &kmspb.DecryptRequest{Name: strings.TrimPrefix(keyName, "gcp:"), Ciphertext: wrapped, CiphertextCrc32C: checksum(wrapped)})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Plaintext) != 32 || out.PlaintextCrc32C == nil || out.PlaintextCrc32C.Value != checksum(out.Plaintext).Value {
		return nil, ErrKeyUnavailable
	}
	return out.Plaintext, nil
}
