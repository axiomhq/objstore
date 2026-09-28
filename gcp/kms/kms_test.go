package kms

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"

	"github.com/axiomhq/objstore/kms"
)

var errRevoked = errors.New("revoked")

type fakeGCP struct {
	revoked     bool
	version     string
	badChecksum bool
}

func (f *fakeGCP) Encrypt(_ context.Context, in *kmspb.EncryptRequest, _ ...gax.CallOption) (*kmspb.EncryptResponse, error) {
	if f.revoked {
		return nil, errRevoked
	}
	sum := sha256.Sum256(in.Plaintext)
	ciphertext := append(append([]byte("gcp"), in.Plaintext...), sum[:]...)
	crc := checksum(ciphertext)
	if f.badChecksum {
		crc.Value++
	}
	return &kmspb.EncryptResponse{Name: in.Name + "/cryptoKeyVersions/" + f.version, Ciphertext: ciphertext, VerifiedPlaintextCrc32C: true, CiphertextCrc32C: crc}, nil
}

func (f *fakeGCP) Decrypt(_ context.Context, in *kmspb.DecryptRequest, _ ...gax.CallOption) (*kmspb.DecryptResponse, error) {
	if f.revoked {
		return nil, errRevoked
	}
	if !bytes.HasPrefix(in.Ciphertext, []byte("gcp")) || len(in.Ciphertext) != 67 {
		return nil, errors.New("bad ciphertext")
	}
	sum := sha256.Sum256(in.Ciphertext[3:35])
	if !bytes.Equal(sum[:], in.Ciphertext[35:]) {
		return nil, errors.New("bad ciphertext")
	}
	crc := checksum(in.Ciphertext[3:35])
	if f.badChecksum {
		crc.Value++
	}
	return &kmspb.DecryptResponse{Plaintext: in.Ciphertext[3:35], PlaintextCrc32C: crc}, nil
}

const keyName = "gcp:projects/p/locations/l/keyRings/r/cryptoKeys/k"

func TestProvider(t *testing.T) {
	ctx := t.Context()
	dek := bytes.Repeat([]byte{42}, kms.DEKSize)

	t.Run("RoundTrip", func(t *testing.T) {
		p := Provider{Client: &fakeGCP{version: "1"}}
		wrapped, version, err := p.Wrap(ctx, keyName, dek)
		if err != nil || version != "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1" || bytes.Equal(wrapped, dek) {
			t.Fatalf("wrap: version=%q err=%v", version, err)
		}
		if got, err := p.Unwrap(ctx, keyName, wrapped); err != nil || !bytes.Equal(got, dek) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("Tamper", func(t *testing.T) {
		p := Provider{Client: &fakeGCP{version: "1"}}
		wrapped, _, err := p.Wrap(ctx, keyName, dek)
		if err != nil {
			t.Fatal(err)
		}
		wrapped[len(wrapped)-1] ^= 1
		if _, err := p.Unwrap(ctx, keyName, wrapped); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("tampered: %v", err)
		}
	})

	t.Run("Revoked", func(t *testing.T) {
		client := &fakeGCP{version: "1"}
		p := Provider{Client: client}
		wrapped, _, err := p.Wrap(ctx, keyName, dek)
		if err != nil {
			t.Fatal(err)
		}
		client.revoked = true
		if _, _, err := p.Wrap(ctx, keyName, dek); !errors.Is(err, kms.ErrKeyUnavailable) || !errors.Is(err, errRevoked) {
			t.Fatalf("wrap: %v", err)
		}
		if _, err := p.Unwrap(ctx, keyName, wrapped); !errors.Is(err, kms.ErrKeyUnavailable) || !errors.Is(err, errRevoked) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("ChecksumMismatch", func(t *testing.T) {
		client := &fakeGCP{version: "1"}
		p := Provider{Client: client}
		wrapped, _, err := p.Wrap(ctx, keyName, dek)
		if err != nil {
			t.Fatal(err)
		}
		client.badChecksum = true
		if _, _, err := p.Wrap(ctx, keyName, dek); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("wrap: %v", err)
		}
		if _, err := p.Unwrap(ctx, keyName, wrapped); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("BadInput", func(t *testing.T) {
		p := Provider{Client: &fakeGCP{version: "1"}}
		if _, _, err := p.Wrap(ctx, keyName, dek[:16]); err == nil || errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("short DEK must be a plain caller error: %v", err)
		}
		if _, _, err := p.Wrap(ctx, "aws:arn:x", dek); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("foreign scheme: %v", err)
		}
		if _, _, err := (Provider{}).Wrap(ctx, keyName, dek); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("nil client: %v", err)
		}
	})

	t.Run("NotLeaseCadenced", func(t *testing.T) {
		r := kms.Router{Routes: map[string]kms.KeyProvider{Scheme: Provider{Client: &fakeGCP{}}}}
		if r.LeaseCadenced(keyName) {
			t.Fatal("GCP exposes key versions and must not be lease-cadenced")
		}
	})
}
