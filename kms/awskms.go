package kms

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// AWSClient is the subset of the AWS KMS SDK needed by AWSKMS.
type AWSClient interface {
	Encrypt(context.Context, *kms.EncryptInput, ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

type AWSKMS struct{ Client AWSClient }

func (p AWSKMS) LeaseCadenced(string) bool { return true }

func (p AWSKMS) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if p.Client == nil || !strings.HasPrefix(keyName, "aws:arn:") || len(dek) != 32 {
		return nil, "", ErrKeyUnavailable
	}
	arn := strings.TrimPrefix(keyName, "aws:")
	out, err := p.Client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(arn), Plaintext: dek})
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if out == nil || len(out.CiphertextBlob) == 0 {
		return nil, "", ErrKeyUnavailable
	}
	// AWS KMS deliberately does not expose the backing material version.
	return out.CiphertextBlob, aws.ToString(out.KeyId), nil
}

func (p AWSKMS) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	if p.Client == nil || !strings.HasPrefix(keyName, "aws:arn:") {
		return nil, ErrKeyUnavailable
	}
	out, err := p.Client.Decrypt(ctx, &kms.DecryptInput{KeyId: aws.String(strings.TrimPrefix(keyName, "aws:")), CiphertextBlob: wrapped})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if out == nil || len(out.Plaintext) != 32 {
		return nil, ErrKeyUnavailable
	}
	return out.Plaintext, nil
}
