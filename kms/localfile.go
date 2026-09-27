package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// LocalFile reads 32-byte keys from Dir. A key name local:example resolves
// only to Dir/example; slashes and path traversal are refused. It is intended
// for tests and single-host deployments with a separately protected key ring.
type LocalFile struct{ Dir string }

func localName(keyName string) (string, error) {
	name, ok := strings.CutPrefix(keyName, "local:")
	if !ok || name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%w: invalid local key name", ErrKeyUnavailable)
	}
	return name, nil
}

func (p LocalFile) readKey(name string) ([]byte, string, error) {
	path := filepath.Join(p.Dir, name)
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("%w: local key must be a private regular file", ErrKeyUnavailable)
	}
	key, err := io.ReadAll(io.LimitReader(file, 33))
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if len(key) != 32 {
		return nil, "", fmt.Errorf("%w: local key must be 32 bytes", ErrKeyUnavailable)
	}
	sum := sha256.Sum256(key)
	return key, hex.EncodeToString(sum[:8]), nil
}

func (p LocalFile) key(keyName string) ([]byte, string, error) {
	name, err := localName(keyName)
	if err != nil {
		return nil, "", err
	}
	return p.readKey(name)
}

func (p LocalFile) Wrap(ctx context.Context, keyName string, dek []byte) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if len(dek) != 32 {
		return nil, "", errors.New("DEK must be 32 bytes")
	}
	key, version, err := p.key(keyName)
	if err != nil {
		return nil, "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, "", err
	}
	wrapped := append([]byte(version), nonce...)
	return aead.Seal(wrapped, nonce, dek, []byte(keyName)), version, nil
}

func (p LocalFile) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, err := localName(keyName)
	if err != nil {
		return nil, err
	}
	if len(wrapped) < 16+12+16 {
		return nil, ErrKeyUnavailable
	}
	version := string(wrapped[:16])
	if _, err := hex.DecodeString(version); err != nil {
		return nil, ErrKeyUnavailable
	}
	key, activeVersion, err := p.readKey(name)
	if err != nil {
		return nil, err
	}
	if activeVersion != version {
		// The operator must retain the previous private key as name.version
		// until its envelope has been rewrapped on the next write.
		key, activeVersion, err = p.readKey(name + "." + version)
		if err != nil || activeVersion != version {
			return nil, ErrKeyUnavailable
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	wrapped = wrapped[16:]
	if len(wrapped) < aead.NonceSize()+aead.Overhead() {
		return nil, ErrKeyUnavailable
	}
	dek, err := aead.Open(nil, wrapped[:aead.NonceSize()], wrapped[aead.NonceSize():], []byte(keyName))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
	}
	if len(dek) != 32 {
		return nil, ErrKeyUnavailable
	}
	return dek, nil
}
