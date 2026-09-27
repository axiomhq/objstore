package kms

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/googleapis/gax-go/v2"
)

type fakeAWS struct{ revoked bool }

func (f *fakeAWS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if f.revoked {
		return nil, errors.New("revoked")
	}
	sum := sha256.Sum256(in.Plaintext)
	return &kms.EncryptOutput{CiphertextBlob: append(append([]byte("aws"), in.Plaintext...), sum[:]...), KeyId: in.KeyId}, nil
}
func (f *fakeAWS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	if f.revoked {
		return nil, errors.New("revoked")
	}
	if !bytes.HasPrefix(in.CiphertextBlob, []byte("aws")) || len(in.CiphertextBlob) != 67 {
		return nil, errors.New("bad ciphertext")
	}
	sum := sha256.Sum256(in.CiphertextBlob[3:35])
	if !bytes.Equal(sum[:], in.CiphertextBlob[35:]) {
		return nil, errors.New("bad ciphertext")
	}
	return &kms.DecryptOutput{Plaintext: in.CiphertextBlob[3:35], KeyId: aws.String("arn:aws:kms:us-east-1:123:key/test")}, nil
}

type fakeGCP struct {
	revoked bool
	version string
}

func (f *fakeGCP) Encrypt(_ context.Context, in *kmspb.EncryptRequest, _ ...gax.CallOption) (*kmspb.EncryptResponse, error) {
	if f.revoked {
		return nil, errors.New("revoked")
	}
	sum := sha256.Sum256(in.Plaintext)
	ciphertext := append(append([]byte("gcp"), in.Plaintext...), sum[:]...)
	return &kmspb.EncryptResponse{Name: in.Name + "/cryptoKeyVersions/" + f.version, Ciphertext: ciphertext, VerifiedPlaintextCrc32C: true, CiphertextCrc32C: checksum(ciphertext)}, nil
}
func (f *fakeGCP) Decrypt(_ context.Context, in *kmspb.DecryptRequest, _ ...gax.CallOption) (*kmspb.DecryptResponse, error) {
	if f.revoked {
		return nil, errors.New("revoked")
	}
	if !bytes.HasPrefix(in.Ciphertext, []byte("gcp")) || len(in.Ciphertext) != 67 {
		return nil, errors.New("bad ciphertext")
	}
	sum := sha256.Sum256(in.Ciphertext[3:35])
	if !bytes.Equal(sum[:], in.Ciphertext[35:]) {
		return nil, errors.New("bad ciphertext")
	}
	return &kmspb.DecryptResponse{Plaintext: in.Ciphertext[3:35], PlaintextCrc32C: checksum(in.Ciphertext[3:35])}, nil
}

func TestKeyProviders(t *testing.T) {
	ctx := context.Background()
	dek := bytes.Repeat([]byte{42}, 32)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "test"), bytes.Repeat([]byte{7}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	awsClient := &fakeAWS{}
	gcpClient := &fakeGCP{version: "1"}
	router := Router{Local: LocalFile{Dir: dir}, AWS: AWSKMS{Client: awsClient}, GCP: GCPKMS{Client: gcpClient}}
	if !router.LeaseCadenced("aws:arn:aws:kms:us-east-1:123:key/test") || router.LeaseCadenced("local:test") {
		t.Fatal("AWS-only lease cadence selection")
	}
	if !(Router{Default: router.AWS}).LeaseCadenced("arn:aws:kms:us-east-1:123:key/test") {
		t.Fatal("default AWS provider must use lease cadence")
	}
	router.Default = router.Local
	if wrapped, _, err := router.Wrap(ctx, "test", dek); err != nil {
		t.Fatalf("default provider wrap: %v", err)
	} else if got, err := router.Unwrap(ctx, "test", wrapped); err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("default provider unwrap: %v", err)
	}
	for _, keyName := range []string{"local:test", "aws:arn:aws:kms:us-east-1:123:key/test", "gcp:projects/p/locations/l/keyRings/r/cryptoKeys/k"} {
		t.Run(keyName, func(t *testing.T) {
			wrapped, version, err := router.Wrap(ctx, keyName, dek)
			if err != nil || version == "" || bytes.Equal(wrapped, dek) {
				t.Fatalf("wrap: version=%q err=%v", version, err)
			}
			got, err := router.Unwrap(ctx, keyName, wrapped)
			if err != nil || !bytes.Equal(got, dek) {
				t.Fatalf("unwrap: %v", err)
			}
			wrapped[len(wrapped)-1] ^= 1
			if _, err := router.Unwrap(ctx, keyName, wrapped); err == nil {
				t.Fatal("tampered ciphertext accepted")
			}
		})
	}
	awsClient.revoked = true
	if _, _, err := router.Wrap(ctx, "aws:arn:aws:kms:us-east-1:123:key/test", dek); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("revoked AWS: %v", err)
	}
	gcpClient.revoked = true
	if _, _, err := router.Wrap(ctx, "gcp:projects/p/locations/l/keyRings/r/cryptoKeys/k", dek); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("revoked GCP: %v", err)
	}
	if _, _, err := router.Wrap(ctx, "local:../escape", dek); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("path traversal: %v", err)
	}
}
