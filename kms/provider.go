// Package kms wraps and unwraps data encryption keys under a customer key.
//
// This package imports only the standard library. It defines KeyProvider,
// ErrKeyUnavailable, LocalFile (32-byte keys in a directory) and Router,
// which picks a provider by the key name's scheme. The cloud providers live
// in their own packages so a binary links only the SDK it uses: package
// github.com/axiomhq/objstore/aws/kms (aws:arn:...) and
// github.com/axiomhq/objstore/gcp/kms (gcp:projects/...).
package kms

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// DEKSize is the size in bytes of a data encryption key (AES-256).
const DEKSize = 32

// KeyProvider wraps a DEKSize-byte data encryption key under a customer key.
//
// Wrap returns the wrapped key and keyVersion, an opaque string naming the
// customer key material that did the wrapping. A change in keyVersion means
// the customer key was rotated. Per provider:
//
//   - LocalFile: the first 8 bytes of the SHA-256 of the key file, in hex.
//   - aws/kms Provider: the key ARN reported by AWS KMS. AWS does not expose
//     the backing material version, so rotation is not visible in it.
//   - gcp/kms Provider: the full CryptoKeyVersion resource name that
//     encrypted the key.
//
// Unwrap must reject a revoked or unavailable key, including on a warm
// cache. Both methods report such keys with an error wrapping
// ErrKeyUnavailable; a DEK of the wrong size is a caller error and does not.
type KeyProvider interface {
	Wrap(ctx context.Context, keyName string, dek []byte) (wrapped []byte, keyVersion string, err error)
	Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error)
}

// LeaseCadencer is implemented by providers whose key rotation cannot be
// observed through keyVersion, so rotation checks should be bounded by the
// namespace lease renewal interval instead.
type LeaseCadencer interface {
	LeaseCadenced(keyName string) bool
}

// ErrKeyUnavailable reports that the customer key is missing, revoked,
// misconfigured or refused the operation, or that a wrapped key failed to
// authenticate.
var ErrKeyUnavailable = errors.New("kms: customer-managed encryption key unavailable")

// CheckDEK returns a caller error, not wrapping ErrKeyUnavailable, if dek
// is not DEKSize bytes. Providers call it at the top of Wrap.
func CheckDEK(dek []byte) error {
	if len(dek) != DEKSize {
		return fmt.Errorf("kms: DEK must be %d bytes, got %d", DEKSize, len(dek))
	}
	return nil
}

// Router is a KeyProvider that selects a provider by key name.
//
// Routes maps a scheme prefix such as "local:", "aws:" or "gcp:" to its
// provider; the longest matching prefix wins and the full key name is
// passed through. A name matching no route goes to Default as
// DefaultScheme+keyName. No provider is silently substituted: a missing or
// nil provider yields ErrKeyUnavailable.
type Router struct {
	Routes        map[string]KeyProvider
	Default       KeyProvider
	DefaultScheme string
}

func (r Router) provider(keyName string) (KeyProvider, string, error) {
	var (
		p       KeyProvider
		matched = -1
	)
	for scheme, rp := range r.Routes {
		if len(scheme) > matched && strings.HasPrefix(keyName, scheme) {
			p, matched = rp, len(scheme)
		}
	}
	if matched < 0 {
		p, keyName = r.Default, r.DefaultScheme+keyName
	}
	if keyName == "" || p == nil {
		return nil, "", fmt.Errorf("%w: no provider for %q", ErrKeyUnavailable, keyName)
	}
	return p, keyName, nil
}

// Wrap wraps dek with the provider routed for keyName.
func (r Router) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	p, resolved, err := r.provider(keyName)
	if err != nil {
		return nil, "", err
	}
	return p.Wrap(ctx, resolved, dek)
}

// Unwrap unwraps wrapped with the provider routed for keyName.
func (r Router) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	p, resolved, err := r.provider(keyName)
	if err != nil {
		return nil, err
	}
	return p.Unwrap(ctx, resolved, wrapped)
}

// LeaseCadenced reports whether the provider routed for keyName implements
// LeaseCadencer and returns true for the resolved name. It is false when no
// provider matches.
func (r Router) LeaseCadenced(keyName string) bool {
	p, resolved, err := r.provider(keyName)
	if err != nil {
		return false
	}
	lc, ok := p.(LeaseCadencer)
	return ok && lc.LeaseCadenced(resolved)
}
