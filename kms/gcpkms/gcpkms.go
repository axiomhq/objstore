// Package gcpkms is a kms.KeyProvider backed by Google Cloud KMS. Key
// names have the form
// gcp:projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>.
package gcpkms

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/axiomhq/objstore/kms"
)

// Scheme prefixes every key name Provider accepts.
const Scheme = "gcp:"

// Client is implemented by cloud.google.com/go/kms/apiv1.KeyManagementClient.
type Client interface {
	Encrypt(context.Context, *kmspb.EncryptRequest, ...gax.CallOption) (*kmspb.EncryptResponse, error)
	Decrypt(context.Context, *kmspb.DecryptRequest, ...gax.CallOption) (*kmspb.DecryptResponse, error)
}

// Provider wraps data encryption keys with Cloud KMS Encrypt and Decrypt,
// verifying CRC32C checksums in both directions. Its keyVersion is the
// CryptoKeyVersion resource name that encrypted the key.
type Provider struct{ Client Client }

var _ kms.KeyProvider = Provider{}

var crc32c = crc32.MakeTable(crc32.Castagnoli)

func checksum(data []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(data, crc32c)))
}

func (p Provider) name(keyName string) (string, error) {
	if p.Client == nil {
		return "", fmt.Errorf("%w: gcpkms: nil client", kms.ErrKeyUnavailable)
	}
	name, ok := strings.CutPrefix(keyName, Scheme)
	if !ok || !strings.HasPrefix(name, "projects/") {
		return "", fmt.Errorf("%w: gcpkms: invalid key name %q", kms.ErrKeyUnavailable, keyName)
	}
	return name, nil
}

// Wrap encrypts dek under the CryptoKey named by keyName.
func (p Provider) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if err := kms.CheckDEK(dek); err != nil {
		return nil, "", err
	}
	name, err := p.name(keyName)
	if err != nil {
		return nil, "", err
	}
	out, err := p.Client.Encrypt(ctx, &kmspb.EncryptRequest{Name: name, Plaintext: dek, PlaintextCrc32C: checksum(dek)})
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Ciphertext) == 0 || out.Name == "" || !out.VerifiedPlaintextCrc32C || out.CiphertextCrc32C == nil || out.CiphertextCrc32C.Value != checksum(out.Ciphertext).Value {
		return nil, "", fmt.Errorf("%w: gcpkms: encrypt response failed integrity check", kms.ErrKeyUnavailable)
	}
	return out.Ciphertext, out.Name, nil
}

// Unwrap decrypts wrapped with the CryptoKey named by keyName.
func (p Provider) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	name, err := p.name(keyName)
	if err != nil {
		return nil, err
	}
	out, err := p.Client.Decrypt(ctx, &kmspb.DecryptRequest{Name: name, Ciphertext: wrapped, CiphertextCrc32C: checksum(wrapped)})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Plaintext) != kms.DEKSize || out.PlaintextCrc32C == nil || out.PlaintextCrc32C.Value != checksum(out.Plaintext).Value {
		return nil, fmt.Errorf("%w: gcpkms: decrypt response failed integrity check", kms.ErrKeyUnavailable)
	}
	return out.Plaintext, nil
}
