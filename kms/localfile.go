package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

const (
	// versionLen is the length of a LocalFile keyVersion: 8 bytes of the
	// key's SHA-256, hex encoded. It prefixes every wrapped key.
	versionLen = 2 * 8
	// gcmNonceSize and gcmTagSize are the AES-GCM defaults.
	gcmNonceSize = 12
	gcmTagSize   = 16
	// wrappedLen is the length of a LocalFile wrapped key:
	// version || nonce || sealed DEK || tag.
	wrappedLen = versionLen + gcmNonceSize + DEKSize + gcmTagSize
)

// LocalFile reads DEKSize-byte keys from Dir. The key name local:example
// resolves only to the file Dir/example; slashes, path traversal and
// symlinks are refused, and on non-Windows systems the file must not be
// readable or writable by group or others. It is intended for tests and
// single-host deployments with a separately protected key ring.
//
// Rotation: every wrapped key records the version (see KeyProvider) of the
// key file that wrapped it. To rotate local:example, copy the current
// Dir/example to Dir/example.<version> and then replace Dir/example with
// the new key. Unwrap falls back to Dir/example.<version> for keys wrapped
// before the rotation; keep that file until every such key has been
// rewrapped under the new key.
type LocalFile struct{ Dir string }

var _ KeyProvider = LocalFile{}

func localName(keyName string) (string, error) {
	name, ok := strings.CutPrefix(keyName, "local:")
	if !ok || name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%w: invalid local key name %q", ErrKeyUnavailable, keyName)
	}
	return name, nil
}

// readKey reads the key file name in Dir and returns the key and its version.
func (p LocalFile) readKey(name string) ([]byte, string, error) {
	root, err := os.OpenRoot(p.Dir)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	defer root.Close()
	linfo, err := root.Lstat(name)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	if linfo.Mode()&os.ModeSymlink != 0 {
		return nil, "", fmt.Errorf("%w: local key %q is a symlink", ErrKeyUnavailable, name)
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	if !os.SameFile(linfo, info) || !info.Mode().IsRegular() {
		return nil, "", fmt.Errorf("%w: local key %q must be a regular file", ErrKeyUnavailable, name)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("%w: local key %q must not be accessible by group or others (mode %v)", ErrKeyUnavailable, name, info.Mode().Perm())
	}
	key, err := io.ReadAll(io.LimitReader(file, DEKSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	if len(key) != DEKSize {
		clear(key)
		return nil, "", fmt.Errorf("%w: local key %q must be %d bytes", ErrKeyUnavailable, name, DEKSize)
	}
	sum := sha256.Sum256(key)
	return key, hex.EncodeToString(sum[:versionLen/2]), nil
}

// newAEAD returns AES-GCM under key and clears key.
func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	clear(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Wrap seals dek under the key file named by keyName, authenticating
// keyName. The result is version || nonce || ciphertext || tag.
func (p LocalFile) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := CheckDEK(dek); err != nil {
		return nil, "", err
	}
	name, err := localName(keyName)
	if err != nil {
		return nil, "", err
	}
	key, version, err := p.readKey(name)
	if err != nil {
		return nil, "", err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, "", err
	}
	wrapped := make([]byte, versionLen+gcmNonceSize, wrappedLen)
	copy(wrapped, version)
	nonce := wrapped[versionLen:]
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", err
	}
	return aead.Seal(wrapped, nonce, dek, []byte(keyName)), version, nil
}

// Unwrap opens a key produced by Wrap for the same keyName, falling back to
// the rotated key file name.<version> when the current key has another
// version.
func (p LocalFile) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, err := localName(keyName)
	if err != nil {
		return nil, err
	}
	if len(wrapped) != wrappedLen {
		return nil, fmt.Errorf("%w: wrapped key is %d bytes, want %d", ErrKeyUnavailable, len(wrapped), wrappedLen)
	}
	version := string(wrapped[:versionLen])
	if _, err := hex.DecodeString(version); err != nil {
		return nil, fmt.Errorf("%w: malformed key version", ErrKeyUnavailable)
	}
	key, activeVersion, err := p.readKey(name)
	if err != nil {
		return nil, err
	}
	if activeVersion != version {
		clear(key)
		rotated := name + "." + version
		key, activeVersion, err = p.readKey(rotated)
		if err != nil {
			return nil, fmt.Errorf("kms: rotated key %q: %w", rotated, err)
		}
		if activeVersion != version {
			clear(key)
			return nil, fmt.Errorf("%w: rotated key %q has version %s, want %s", ErrKeyUnavailable, rotated, activeVersion, version)
		}
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce, sealed := wrapped[versionLen:versionLen+gcmNonceSize], wrapped[versionLen+gcmNonceSize:]
	dek, err := aead.Open(nil, nonce, sealed, []byte(keyName))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKeyUnavailable, err)
	}
	return dek, nil
}
