// Package awskms is a kms.KeyProvider backed by AWS KMS. Key names have
// the form aws:arn:aws:kms:<region>:<account>:key/<id>.
package awskms

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/axiomhq/objstore/kms"
)

// Scheme prefixes every key name Provider accepts.
const Scheme = "aws:"

// Client is the subset of the AWS KMS SDK client (*kms.Client) that
// Provider uses.
type Client interface {
	Encrypt(context.Context, *awssdk.EncryptInput, ...func(*awssdk.Options)) (*awssdk.EncryptOutput, error)
	Decrypt(context.Context, *awssdk.DecryptInput, ...func(*awssdk.Options)) (*awssdk.DecryptOutput, error)
}

// Provider wraps data encryption keys with AWS KMS Encrypt and Decrypt.
// Its keyVersion is the key ARN, since AWS does not expose the backing
// material version; Provider is therefore lease-cadenced.
type Provider struct{ Client Client }

var (
	_ kms.KeyProvider   = Provider{}
	_ kms.LeaseCadencer = Provider{}
)

// LeaseCadenced always reports true: AWS rotation is invisible in the
// keyVersion, so rotation checks follow the lease renewal interval.
func (p Provider) LeaseCadenced(string) bool { return true }

func (p Provider) arn(keyName string) (string, error) {
	if p.Client == nil {
		return "", fmt.Errorf("%w: awskms: nil client", kms.ErrKeyUnavailable)
	}
	arn, ok := strings.CutPrefix(keyName, Scheme)
	if !ok || !strings.HasPrefix(arn, "arn:") {
		return "", fmt.Errorf("%w: awskms: invalid key name %q", kms.ErrKeyUnavailable, keyName)
	}
	return arn, nil
}

// Wrap encrypts dek under the KMS key named by keyName.
func (p Provider) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if err := kms.CheckDEK(dek); err != nil {
		return nil, "", err
	}
	arn, err := p.arn(keyName)
	if err != nil {
		return nil, "", err
	}
	out, err := p.Client.Encrypt(ctx, &awssdk.EncryptInput{KeyId: aws.String(arn), Plaintext: dek})
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
	}
	if out == nil || len(out.CiphertextBlob) == 0 {
		return nil, "", fmt.Errorf("%w: awskms: empty ciphertext", kms.ErrKeyUnavailable)
	}
	return out.CiphertextBlob, aws.ToString(out.KeyId), nil
}

// Unwrap decrypts wrapped with the KMS key named by keyName.
func (p Provider) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	arn, err := p.arn(keyName)
	if err != nil {
		return nil, err
	}
	out, err := p.Client.Decrypt(ctx, &awssdk.DecryptInput{KeyId: aws.String(arn), CiphertextBlob: wrapped})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Plaintext) != kms.DEKSize {
		return nil, fmt.Errorf("%w: awskms: decrypted key is not %d bytes", kms.ErrKeyUnavailable, kms.DEKSize)
	}
	return out.Plaintext, nil
}
