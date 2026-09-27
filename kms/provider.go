// Package kms wraps and unwraps data encryption keys under a customer key:
// LocalFile (32-byte keys in a directory), AWSKMS and GCPKMS, and a Router
// that picks one by the key name's scheme (local:, aws:arn:, gcp:projects/).
package kms

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// KeyProvider wraps a 256-bit data encryption key under a customer key.
// Unwrap must reject a revoked or unavailable key, including on a warm cache.
type KeyProvider interface {
	Wrap(ctx context.Context, keyName string, dek []byte) (wrapped []byte, keyVersion string, err error)
	Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error)
}

var ErrKeyUnavailable = errors.New("customer-managed encryption key unavailable")

// Router selects a provider from the key name. Default is used for names
// without a recognized scheme; no provider is silently substituted.
type Router struct {
	Local, AWS, GCP, Default KeyProvider
}

func (r Router) provider(keyName string) (KeyProvider, string, error) {
	var p KeyProvider
	switch {
	case strings.HasPrefix(keyName, "local:"):
		p = r.Local
	case strings.HasPrefix(keyName, "aws:arn:"):
		p = r.AWS
	case strings.HasPrefix(keyName, "gcp:projects/"):
		p = r.GCP
	default:
		p = r.Default
		switch p.(type) {
		case LocalFile, *LocalFile:
			keyName = "local:" + keyName
		case AWSKMS, *AWSKMS:
			keyName = "aws:" + keyName
		case GCPKMS, *GCPKMS:
			keyName = "gcp:" + keyName
		}
	}
	if keyName == "" || p == nil {
		return nil, "", fmt.Errorf("%w: no provider for %q", ErrKeyUnavailable, keyName)
	}
	return p, keyName, nil
}

func (r Router) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	p, resolved, err := r.provider(keyName)
	if err != nil {
		return nil, "", err
	}
	return p.Wrap(ctx, resolved, dek)
}

func (r Router) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	p, resolved, err := r.provider(keyName)
	if err != nil {
		return nil, err
	}
	return p.Unwrap(ctx, resolved, wrapped)
}

// LeaseCadenced reports whether rotation checks should be bounded by the
// namespace lease renewal interval. AWS does not reveal a material version.
func (r Router) LeaseCadenced(keyName string) bool {
	p, _, err := r.provider(keyName)
	if err != nil {
		return false
	}
	_, value := p.(AWSKMS)
	_, pointer := p.(*AWSKMS)
	return value || pointer
}
