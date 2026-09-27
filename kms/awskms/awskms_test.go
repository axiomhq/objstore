package awskms

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/axiomhq/objstore/kms"
)

var errRevoked = errors.New("revoked")

type fakeAWS struct{ revoked bool }

func (f *fakeAWS) Encrypt(_ context.Context, in *awssdk.EncryptInput, _ ...func(*awssdk.Options)) (*awssdk.EncryptOutput, error) {
	if f.revoked {
		return nil, errRevoked
	}
	sum := sha256.Sum256(in.Plaintext)
	return &awssdk.EncryptOutput{CiphertextBlob: append(append([]byte("aws"), in.Plaintext...), sum[:]...), KeyId: in.KeyId}, nil
}

func (f *fakeAWS) Decrypt(_ context.Context, in *awssdk.DecryptInput, _ ...func(*awssdk.Options)) (*awssdk.DecryptOutput, error) {
	if f.revoked {
		return nil, errRevoked
	}
	if !bytes.HasPrefix(in.CiphertextBlob, []byte("aws")) || len(in.CiphertextBlob) != 67 {
		return nil, errors.New("bad ciphertext")
	}
	sum := sha256.Sum256(in.CiphertextBlob[3:35])
	if !bytes.Equal(sum[:], in.CiphertextBlob[35:]) {
		return nil, errors.New("bad ciphertext")
	}
	return &awssdk.DecryptOutput{Plaintext: in.CiphertextBlob[3:35], KeyId: aws.String("arn:aws:kms:us-east-1:123:key/test")}, nil
}

const keyName = "aws:arn:aws:kms:us-east-1:123:key/test"

func TestProvider(t *testing.T) {
	ctx := t.Context()
	dek := bytes.Repeat([]byte{42}, kms.DEKSize)

	t.Run("RoundTrip", func(t *testing.T) {
		p := Provider{Client: &fakeAWS{}}
		wrapped, version, err := p.Wrap(ctx, keyName, dek)
		if err != nil || version != "arn:aws:kms:us-east-1:123:key/test" || bytes.Equal(wrapped, dek) {
			t.Fatalf("wrap: version=%q err=%v", version, err)
		}
		if got, err := p.Unwrap(ctx, keyName, wrapped); err != nil || !bytes.Equal(got, dek) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("Tamper", func(t *testing.T) {
		p := Provider{Client: &fakeAWS{}}
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
		client := &fakeAWS{}
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

	t.Run("BadInput", func(t *testing.T) {
		p := Provider{Client: &fakeAWS{}}
		if _, _, err := p.Wrap(ctx, keyName, dek[:16]); err == nil || errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("short DEK must be a plain caller error: %v", err)
		}
		if _, _, err := p.Wrap(ctx, "gcp:projects/p", dek); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("foreign scheme: %v", err)
		}
		if _, _, err := (Provider{}).Wrap(ctx, keyName, dek); !errors.Is(err, kms.ErrKeyUnavailable) {
			t.Fatalf("nil client: %v", err)
		}
	})

	t.Run("LeaseCadenced", func(t *testing.T) {
		r := kms.Router{Routes: map[string]kms.KeyProvider{Scheme: Provider{Client: &fakeAWS{}}}, Default: Provider{Client: &fakeAWS{}}, DefaultScheme: Scheme}
		if !r.LeaseCadenced(keyName) || !r.LeaseCadenced("arn:aws:kms:us-east-1:123:key/test") {
			t.Fatal("AWS keys must be lease-cadenced")
		}
	})
}
