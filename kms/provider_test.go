package kms

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writeKey(t *testing.T, dir, name string, b byte, perm os.FileMode) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, bytes.Repeat([]byte{b}, DEKSize), perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, perm); err != nil { // defeat umask
		t.Fatal(err)
	}
}

func TestLocalFile(t *testing.T) {
	ctx := t.Context()
	dek := bytes.Repeat([]byte{42}, DEKSize)

	t.Run("RoundTrip", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		p := LocalFile{Dir: dir}
		wrapped, version, err := p.Wrap(ctx, "local:k", dek)
		if err != nil || len(version) != versionLen || len(wrapped) != wrappedLen {
			t.Fatalf("wrap: version=%q len=%d err=%v", version, len(wrapped), err)
		}
		got, err := p.Unwrap(ctx, "local:k", wrapped)
		if err != nil || !bytes.Equal(got, dek) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("Tamper", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		p := LocalFile{Dir: dir}
		wrapped, _, err := p.Wrap(ctx, "local:k", dek)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range []int{versionLen, len(wrapped) - 1} {
			bad := bytes.Clone(wrapped)
			bad[i] ^= 1
			if _, err := p.Unwrap(ctx, "local:k", bad); !errors.Is(err, ErrKeyUnavailable) {
				t.Fatalf("byte %d tampered: %v", i, err)
			}
		}
		if _, err := p.Unwrap(ctx, "local:k", wrapped[:len(wrapped)-1]); !errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("truncated: %v", err)
		}
	})

	t.Run("Traversal", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		p := LocalFile{Dir: filepath.Join(dir, "sub")}
		if err := os.Mkdir(p.Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"local:../k", "local:..", "local:.", "local:", "local:a/b", `local:a\b`, "k"} {
			if _, _, err := p.Wrap(ctx, name, dek); !errors.Is(err, ErrKeyUnavailable) {
				t.Errorf("%q: %v", name, err)
			}
		}
	})

	t.Run("SymlinkRefused", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks need privileges on windows")
		}
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		if err := os.Symlink("k", filepath.Join(dir, "link")); err != nil {
			t.Fatal(err)
		}
		_, _, err := LocalFile{Dir: dir}.Wrap(ctx, "local:link", dek)
		if !errors.Is(err, ErrKeyUnavailable) || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink: %v", err)
		}
	})

	t.Run("RotationFallback", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		p := LocalFile{Dir: dir}
		old, oldVersion, err := p.Wrap(ctx, "local:k", dek)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(dir, "k"), filepath.Join(dir, "k."+oldVersion)); err != nil {
			t.Fatal(err)
		}
		writeKey(t, dir, "k", 8, 0o600)
		_, newVersion, err := p.Wrap(ctx, "local:k", dek)
		if err != nil || newVersion == oldVersion {
			t.Fatalf("wrap after rotation: version=%q err=%v", newVersion, err)
		}
		if got, err := p.Unwrap(ctx, "local:k", old); err != nil || !bytes.Equal(got, dek) {
			t.Fatalf("unwrap via k.%s: %v", oldVersion, err)
		}
		if err := os.Remove(filepath.Join(dir, "k."+oldVersion)); err != nil {
			t.Fatal(err)
		}
		_, err = p.Unwrap(ctx, "local:k", old)
		if !errors.Is(err, ErrKeyUnavailable) || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing rotated key must keep its reason: %v", err)
		}
	})

	t.Run("PermissiveModeRefused", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("no unix permissions on windows")
		}
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o644)
		if _, _, err := (LocalFile{Dir: dir}).Wrap(ctx, "local:k", dek); !errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("0644 key: %v", err)
		}
	})

	t.Run("WrongLength", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		if err := os.WriteFile(filepath.Join(dir, "short"), []byte("short"), 0o600); err != nil {
			t.Fatal(err)
		}
		p := LocalFile{Dir: dir}
		if _, _, err := p.Wrap(ctx, "local:short", dek); !errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("short key file: %v", err)
		}
		_, _, err := p.Wrap(ctx, "local:k", dek[:16])
		if err == nil || errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("short DEK must be a plain caller error: %v", err)
		}
	})

	t.Run("AADMismatch", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "a", 7, 0o600)
		writeKey(t, dir, "b", 7, 0o600) // same material, different name
		p := LocalFile{Dir: dir}
		wrapped, _, err := p.Wrap(ctx, "local:a", dek)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Unwrap(ctx, "local:b", wrapped); !errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("unwrap under another key name: %v", err)
		}
	})
}

// fake records the key name it was called with.
type fake struct {
	name    string
	cadence bool
	got     string
}

func (f *fake) Wrap(_ context.Context, keyName string, _ []byte) ([]byte, string, error) {
	f.got = keyName
	return []byte(f.name), f.name, nil
}

func (f *fake) Unwrap(_ context.Context, keyName string, _ []byte) ([]byte, error) {
	f.got = keyName
	return []byte(f.name), nil
}

type cadenced struct{ *fake }

func (c cadenced) LeaseCadenced(string) bool { return c.cadence }

func TestRouter(t *testing.T) {
	ctx := t.Context()
	dek := make([]byte, DEKSize)

	t.Run("LongestPrefix", func(t *testing.T) {
		short, long := &fake{name: "short"}, &fake{name: "long"}
		r := Router{Routes: map[string]KeyProvider{"aws:": short, "aws:arn:": long}}
		if _, v, err := r.Wrap(ctx, "aws:arn:x", dek); err != nil || v != "long" || long.got != "aws:arn:x" {
			t.Fatalf("aws:arn:x: v=%q got=%q err=%v", v, long.got, err)
		}
		if _, v, err := r.Wrap(ctx, "aws:other", dek); err != nil || v != "short" {
			t.Fatalf("aws:other: v=%q err=%v", v, err)
		}
		if got, err := r.Unwrap(ctx, "aws:arn:y", nil); err != nil || string(got) != "long" || long.got != "aws:arn:y" {
			t.Fatalf("unwrap: %q %v", got, err)
		}
	})

	t.Run("Default", func(t *testing.T) {
		def := &fake{name: "def"}
		r := Router{Routes: map[string]KeyProvider{"gcp:": &fake{}}, Default: def, DefaultScheme: "local:"}
		if _, v, err := r.Wrap(ctx, "k", dek); err != nil || v != "def" || def.got != "local:k" {
			t.Fatalf("default: v=%q got=%q err=%v", v, def.got, err)
		}
	})

	t.Run("DefaultLocalFile", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "k", 7, 0o600)
		r := Router{Default: LocalFile{Dir: dir}, DefaultScheme: "local:"}
		wrapped, _, err := r.Wrap(ctx, "k", dek)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := r.Unwrap(ctx, "k", wrapped); err != nil || !bytes.Equal(got, dek) {
			t.Fatalf("unwrap: %v", err)
		}
	})

	t.Run("NoProvider", func(t *testing.T) {
		for name, r := range map[string]Router{
			"zero":      {},
			"nil route": {Routes: map[string]KeyProvider{"local:": nil}, Default: &fake{}},
			"empty key": {Default: &fake{}},
		} {
			key := "local:k"
			if name == "empty key" {
				key = ""
			}
			if _, _, err := r.Wrap(ctx, key, dek); !errors.Is(err, ErrKeyUnavailable) {
				t.Errorf("%s wrap: %v", name, err)
			}
			if _, err := r.Unwrap(ctx, key, nil); !errors.Is(err, ErrKeyUnavailable) {
				t.Errorf("%s unwrap: %v", name, err)
			}
			if r.LeaseCadenced(key) {
				t.Errorf("%s: lease cadenced without a provider", name)
			}
		}
	})

	t.Run("LeaseCadenced", func(t *testing.T) {
		r := Router{
			Routes: map[string]KeyProvider{
				"aws:":   cadenced{&fake{cadence: true}},
				"gcp:":   cadenced{&fake{cadence: false}},
				"local:": &fake{},
			},
			Default:       cadenced{&fake{cadence: true}},
			DefaultScheme: "aws:",
		}
		for key, want := range map[string]bool{"aws:arn:x": true, "gcp:projects/p": false, "local:k": false, "arn:x": true} {
			if got := r.LeaseCadenced(key); got != want {
				t.Errorf("LeaseCadenced(%q) = %v, want %v", key, got, want)
			}
		}
	})
}
