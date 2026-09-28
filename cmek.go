package objstore

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/axiomhq/objstore/kms"
)

// EncryptionCustomerManaged is the Envelope mode InstallNamespaceKey
// accepts: the data key is wrapped under a key the customer controls.
const EncryptionCustomerManaged = "customer-managed"

// Envelope is a namespace's key record: the wrapped data encryption key and
// the customer key that wraps it. It never holds plaintext key bytes.
type Envelope struct {
	Mode       string `json:"mode"`
	KeyName    string `json:"key_name,omitempty"`
	KeyVersion string `json:"key_version,omitempty"`
	DEKWrapped []byte `json:"dek_wrapped,omitempty"`
}

const encryptedBlockSize = 64 << 10
const encryptedHeaderSize = 25 // magic(4), version(1), plaintext length(8), nonce(12)
const encryptedTagSize = 16

// keyCacheTTL is how long an unwrapped data key, or the absence of a key
// record, is trusted before the record is read again. Absence is trusted
// only by whole-object reads (see objectKey).
const keyCacheTTL = time.Second
const keyRecordPrefix = "cmek/"

// keyLookupTimeout bounds one shared key lookup (record read and unwrap).
// The lookup runs detached from its callers, so without a bound a hung KMS
// or store would wedge the namespace for every later caller.
const keyLookupTimeout = 30 * time.Second

// ErrInvalidEnvelope is returned by InstallNamespaceKey for an envelope that
// is not customer-managed or lacks a key name or wrapped key.
var ErrInvalidEnvelope = errors.New("objstore: invalid namespace key envelope")

// ErrKeyRecordExists is returned by InstallNamespaceKey when the namespace
// already has a different key record.
var ErrKeyRecordExists = errors.New("objstore: namespace key record already exists")

// Backoff after a failed key lookup: doubles from min to max per name.
const (
	minKeyBackoff = 100 * time.Millisecond
	maxKeyBackoff = 5 * time.Second
)

var encryptedMagic = [4]byte{'D', 'W', 'E', 'K'}

// cachedKey is a namespace's data key, or with key nil the namespace's
// lack of a key record (a plaintext namespace), trusted until until.
type cachedKey struct {
	key     []byte
	until   time.Time
	version string
}

type keyFailure struct {
	next  time.Time
	delay time.Duration
	cause error
}

type cmekState struct {
	provider kms.KeyProvider
	mu       sync.Mutex
	keys     map[string]cachedKey
	failures map[string]keyFailure
	aws      map[string]*awsRefresh
	// gen counts, per name, the Install, Retire and forget calls that
	// invalidated its cache entry. load caches its result only if gen did
	// not move while it read, so a lookup that saw "no record" just before
	// an Install cannot cache that stale answer after it. Entries are never
	// removed: a removed counter restarting at 0 could match a stale read.
	gen      map[string]uint64
	interval atomic.Int64
	// flight collapses concurrent record reads and unwraps per namespace;
	// checks does the same for CheckNamespaceKey's revalidations.
	flight, checks singleflight.Group
	// now and ttl are time.Now and keyCacheTTL outside tests.
	now func() time.Time
	ttl time.Duration
}

// awsRefresh is the lease-cadence state of one namespace. mu is a
// one-slot channel, not a sync.Mutex, so a waiter can give up on its
// context: it is held across a provider call.
type awsRefresh struct {
	mu           chan struct{}
	checkedUntil time.Time
	rotatedUntil time.Time
}

func (a *awsRefresh) lock(ctx context.Context) error {
	select {
	case a.mu <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *awsRefresh) unlock() { <-a.mu }

// idle reports, without waiting, whether a is unheld and both its windows
// have passed at now: nothing is lost by dropping it.
func (a *awsRefresh) idle(now time.Time) bool {
	select {
	case a.mu <- struct{}{}:
		defer a.unlock()
		return !now.Before(a.checkedUntil) && !now.Before(a.rotatedUntil)
	default:
		return false
	}
}

func newCMEKState(provider kms.KeyProvider) *cmekState {
	return &cmekState{
		provider: provider,
		keys:     make(map[string]cachedKey),
		failures: make(map[string]keyFailure),
		aws:      make(map[string]*awsRefresh),
		gen:      make(map[string]uint64),
		now:      time.Now,
		ttl:      keyCacheTTL,
	}
}

// crypt returns the encryption state, nil when none is configured.
func (s *Store) crypt() *cmekState {
	if s.cmek == nil {
		return nil
	}
	return s.cmek.Load()
}

// ConfigureCMEK enables transparent encryption of ns/<name>/ objects, as
// Config.KeyProvider does. Call it before first use of s: requests already
// in flight may run with the previous setting. A small envelope record
// outside that prefix bootstraps the encrypted manifest and metadata. The
// record contains a wrapped DEK, never plaintext key bytes. A nil
// provider is ignored. It keeps the refresh interval already set
// (Config.KeyRefreshInterval, SetCMEKRefreshInterval).
func (s *Store) ConfigureCMEK(provider kms.KeyProvider) {
	if provider == nil {
		return
	}
	if s.cmek == nil {
		s.cmek = new(atomic.Pointer[cmekState])
	}
	c := newCMEKState(provider)
	if old := s.cmek.Load(); old != nil {
		c.interval.Store(old.interval.Load())
	}
	s.cmek.Store(c)
}

// SetCMEKRefreshInterval ties access and rotation probes for keys whose
// provider is lease-cadenced (AWS KMS) to the lease heartbeat cadence, as
// Config.KeyRefreshInterval does. Set it once, before use.
func (s *Store) SetCMEKRefreshInterval(interval time.Duration) {
	if c := s.crypt(); c != nil {
		c.interval.Store(int64(interval))
	}
}

func (c *cmekState) refreshInterval() time.Duration { return time.Duration(c.interval.Load()) }

// leaseCadenced reports whether keyName's rotation is invisible in its
// key version (kms.LeaseCadencer).
func (c *cmekState) leaseCadenced(keyName string) bool {
	p, ok := c.provider.(kms.LeaseCadencer)
	return ok && p.LeaseCadenced(keyName)
}

func (c *cmekState) awsState(name, keyName string) *awsRefresh {
	if !c.leaseCadenced(keyName) || c.refreshInterval() <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.aws[name]
	if state == nil {
		state = &awsRefresh{mu: make(chan struct{}, 1)}
		c.aws[name] = state
	}
	return state
}

func (c *cmekState) cacheTTL(name, keyName string) time.Duration {
	if c.awsState(name, keyName) != nil {
		return max(c.ttl, c.refreshInterval())
	}
	return c.ttl
}

// retryReady returns nil, or while name's backoff is open an error naming
// the namespace and wrapping kms.ErrKeyUnavailable and the last failure.
func (c *cmekState) retryReady(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.failures[name]
	if !c.now().Before(f.next) {
		return nil
	}
	return fmt.Errorf("%w: namespace %q backing off after: %w", kms.ErrKeyUnavailable, name, f.cause)
}

// fail opens name's backoff and returns err wrapped with
// kms.ErrKeyUnavailable and the namespace. Every error is the key's: the
// lookup runs detached from its callers, so even a context error is the
// store's or the SDK's own timeout, not a caller giving up.
func (c *cmekState) fail(name string, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.failures[name]
	f.delay = min(maxKeyBackoff, max(minKeyBackoff, f.delay*2))
	f.next = c.now().Add(f.delay)
	f.cause = err
	c.failures[name] = f
	delete(c.keys, name)
	return fmt.Errorf("%w: namespace %q: %w", kms.ErrKeyUnavailable, name, err)
}

// cached returns name's cached lookup, if still fresh. An expired entry is
// dropped, with the name's lease-cadence state once that is idle, so the
// maps hold only names in recent use.
func (c *cmekState) cached(name string) (cachedKey, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k, ok := c.keys[name]
	if !ok {
		return k, false
	}
	now := c.now()
	if now.Before(k.until) {
		return k, true
	}
	delete(c.keys, name)
	if a := c.aws[name]; a != nil && a.idle(now) {
		delete(c.aws, name)
	}
	return k, false
}

// generation returns name's invalidation count; see cmekState.gen.
func (c *cmekState) generation(name string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen[name]
}

// rememberAt is remember for a lookup that began at generation gen: it
// caches nothing if name was invalidated since. absent also drops the
// name's lease-cadence state, as for a retired namespace.
func (c *cmekState) rememberAt(gen uint64, name string, dek []byte, version string, ttl time.Duration, absent bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen[name] != gen {
		return
	}
	if absent {
		delete(c.aws, name)
	}
	c.keys[name] = cachedKey{key: dek, until: c.now().Add(ttl), version: version}
	delete(c.failures, name)
}

// invalidate drops name's cached key and lease-cadence state (and with
// failures its backoff), bumps its generation, and detaches in-flight
// lookups so later callers start a fresh one.
func (c *cmekState) invalidate(name string, failures bool) {
	c.mu.Lock()
	c.gen[name]++
	delete(c.keys, name)
	delete(c.aws, name)
	if failures {
		delete(c.failures, name)
	}
	c.mu.Unlock()
	c.flight.Forget(name)
	c.checks.Forget(name)
}

func (c *cmekState) remember(name string, dek []byte, version string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[name] = cachedKey{key: dek, until: c.now().Add(ttl), version: version}
	delete(c.failures, name)
}

func namespaceObject(key string) string {
	if !strings.HasPrefix(key, "ns/") {
		return ""
	}
	rest := strings.TrimPrefix(key, "ns/")
	name, _, ok := strings.Cut(rest, "/")
	if !ok || name == "" {
		return ""
	}
	return name
}

// InstallNamespaceKey publishes the recovery envelope before the first
// encrypted object. The caller holds the namespace lifecycle lease.
func (s *Store) InstallNamespaceKey(ctx context.Context, name string, envelope Envelope) error {
	c := s.crypt()
	if c == nil {
		return kms.ErrKeyUnavailable
	}
	if envelope.Mode != EncryptionCustomerManaged || envelope.KeyName == "" || len(envelope.DEKWrapped) == 0 {
		return ErrInvalidEnvelope
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	key := keyRecordPrefix + name
	ok, err := s.b.PutIfAbsent(ctx, key, data)
	if err != nil {
		return err
	}
	if !ok {
		old, getErr := s.b.Get(ctx, key)
		if getErr != nil {
			return getErr
		}
		if string(old) != string(data) {
			return fmt.Errorf("%w: %s", ErrKeyRecordExists, name)
		}
	}
	c.invalidate(name, false)
	return nil
}

// RetireNamespaceKey leaves the deleted manifest head as a plaintext name-reuse
// fence, then removes its wrapped DEK. The lifecycle caller holds the lease.
// Repeating this after a crash between the CAS and delete is safe.
func (s *Store) RetireNamespaceKey(ctx context.Context, name, incarnation string) error {
	if s.crypt() == nil {
		return nil
	}
	record := keyRecordPrefix + name
	if _, err := s.b.Get(ctx, record); errors.Is(err, ErrNotFound) {
		s.forgetNamespaceKey(name)
		return nil
	} else if err != nil {
		return err
	}
	object := "ns/" + name + "/manifest"
	raw, tag, err := s.b.GetWithETag(ctx, object)
	if err != nil {
		return err
	}
	plain := raw
	if !deletedHead(raw, incarnation) {
		dek, err := s.objectKey(ctx, object, false)
		if err != nil {
			return err
		}
		plain, err = decryptObject(object, raw, dek)
		if err != nil || !deletedHead(plain, incarnation) {
			return fmt.Errorf("cannot retire namespace key before deleted manifest: %w", kms.ErrKeyUnavailable)
		}
		ok, err := s.b.PutIfMatch(ctx, object, plain, tag)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("deleted manifest changed while retiring namespace key")
		}
	}
	for _, fence := range []string{"lease", "compactor"} {
		if err := s.retireFence(ctx, name, fence); err != nil {
			return err
		}
	}
	if err := s.b.Delete(ctx, record); err != nil {
		return err
	}
	s.forgetNamespaceKey(name)
	return nil
}

func (s *Store) retireFence(ctx context.Context, name, fence string) error {
	object := "ns/" + name + "/" + fence
	for attempt := 0; attempt < 4; attempt++ {
		raw, tag, err := s.b.GetWithETag(ctx, object)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if plaintextFence(object, raw) {
			return nil
		}
		dek, err := s.objectKey(ctx, object, false)
		if err != nil {
			return err
		}
		plain, err := decryptObject(object, raw, dek)
		if err != nil {
			return err
		}
		ok, err := s.b.PutIfMatch(ctx, object, plain, tag)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return errors.New("namespace fence changed while retiring key")
}

func plaintextFence(key string, data []byte) bool {
	if !strings.HasPrefix(key, "ns/") || (!strings.HasSuffix(key, "/lease") && !strings.HasSuffix(key, "/compactor")) {
		return false
	}
	return json.Valid(data)
}

func (s *Store) forgetNamespaceKey(name string) {
	if c := s.crypt(); c != nil {
		c.forget(name)
	}
}

func (c *cmekState) forget(name string) { c.invalidate(name, true) }

func deletedHead(data []byte, incarnation string) bool {
	var head struct {
		State       string `json:"state"`
		Incarnation string `json:"incarnation"`
	}
	return json.Unmarshal(data, &head) == nil && head.State == "deleted" && incarnation != "" && head.Incarnation == incarnation
}

func plaintextDeletedManifest(key string, data []byte) bool {
	if !strings.HasPrefix(key, "ns/") || !strings.HasSuffix(key, "/manifest") {
		return false
	}
	var head struct {
		State       string `json:"state"`
		Incarnation string `json:"incarnation"`
	}
	return json.Unmarshal(data, &head) == nil && head.State == "deleted" && head.Incarnation != ""
}

// objectKey returns the data key for object: nil for an object outside
// ns/<name>/ or in a namespace without a key record. Lookups are cached for
// the key cache TTL, and concurrent lookups of one namespace share one
// record read and one unwrap.
//
// A cached "no record" is trusted only when verifiable: the caller reads
// whole objects and passes them through open, which re-checks bytes that
// carry the encrypted header. A range read cannot tell, and a write would
// store plaintext in a namespace another process has just keyed, so for
// them a cached "no record" re-reads the record (one GET, shared).
func (s *Store) objectKey(ctx context.Context, object string, verifiable bool) ([]byte, error) {
	c := s.crypt()
	if c == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name := namespaceObject(object)
	if name == "" {
		return nil, nil
	}
	if k, ok := c.cached(name); ok && (k.key != nil || verifiable) {
		return k.key, nil
	}
	return s.lookup(ctx, c, name, false)
}

// lookup runs load for name once for all concurrent callers. The shared
// load does not inherit a caller's cancellation, so one caller giving up
// neither fails the others nor opens the name's backoff; each caller still
// returns on its own ctx. The load is bounded by keyLookupTimeout and runs
// Urgent, so a lease heartbeat joining it never waits behind the pacer.
func (s *Store) lookup(ctx context.Context, c *cmekState, name string, check bool) ([]byte, error) {
	if err := c.retryReady(name); err != nil {
		return nil, err
	}
	group := &c.flight
	if check {
		group = &c.checks
	}
	detached := Urgent(context.WithoutCancel(ctx))
	ch := group.DoChan(name, func() (any, error) {
		lctx, cancel := context.WithTimeout(detached, keyLookupTimeout)
		defer cancel()
		return c.load(lctx, s.b, name, check)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		dek, _ := r.Val.([]byte)
		return dek, r.Err
	}
}

// load reads name's key record and unwraps its data key, caching the
// result: nil, nil (also cached) for a namespace without a record. check
// is the request-boundary revalidation: a lease-cadenced key skips the
// unwrap while its last check is fresh, where a plain load skips it while
// the cached key matches the record's version. Nothing is cached if name
// was invalidated while load ran.
func (c *cmekState) load(ctx context.Context, b Backend, name string, check bool) ([]byte, error) {
	gen := c.generation(name)
	data, err := b.Get(ctx, keyRecordPrefix+name)
	if errors.Is(err, ErrNotFound) {
		c.rememberAt(gen, name, nil, "", c.ttl, true)
		return nil, nil
	}
	if err != nil {
		return nil, c.fail(name, err)
	}
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("invalid namespace key record: %w", err)
	}
	state := c.awsState(name, envelope.KeyName)
	if state != nil {
		if err := state.lock(ctx); err != nil {
			return nil, c.fail(name, err)
		}
		defer state.unlock()
		if check && c.now().Before(state.checkedUntil) {
			return nil, nil
		}
		// Another request may have renewed the cached DEK while this one
		// fetched the envelope.
		if k, ok := c.cached(name); !check && ok && k.key != nil && k.version == envelope.KeyVersion {
			return k.key, nil
		}
	}
	dek, err := c.provider.Unwrap(ctx, envelope.KeyName, envelope.DEKWrapped)
	if err != nil {
		return nil, c.fail(name, err)
	}
	if len(dek) != kms.DEKSize {
		return nil, c.fail(name, fmt.Errorf("unwrapped data key is %d bytes, want %d", len(dek), kms.DEKSize))
	}
	c.rememberAt(gen, name, dek, envelope.KeyVersion, c.cacheTTL(name, envelope.KeyName), false)
	if state != nil {
		state.checkedUntil = c.now().Add(c.refreshInterval())
	}
	return dek, nil
}

// CheckNamespaceKey revalidates KMS access at the API boundary, including
// cache-only queries: it reads the key record and unwraps the data key,
// shared with concurrent checks of the same name. Lease-cadenced (AWS)
// keys are checked at most once per refresh interval.
func (s *Store) CheckNamespaceKey(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := s.crypt()
	if c == nil {
		return nil
	}
	_, err := s.lookup(ctx, c, name, true)
	return err
}

// CheckNamespaceKeyCached enforces the key-cache lease for internal
// calls. Use CheckNamespaceKey for access checks at a request boundary.
func (s *Store) CheckNamespaceKeyCached(ctx context.Context, name string) error {
	_, err := s.objectKey(ctx, "ns/"+name+"/manifest", true)
	return err
}

// RotateNamespaceKey asks the provider to wrap the existing DEK with its
// active key version. The encrypted objects and plaintext DEK stay unchanged.
// It returns the current envelope so an interrupted metadata publication can
// be repaired on the next write.
func (s *Store) RotateNamespaceKey(ctx context.Context, name string) (*Envelope, error) {
	rotated := false
	c := s.crypt()
	if c != nil {
		data, err := s.b.Get(ctx, keyRecordPrefix+name)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil {
			var envelope Envelope
			if err := json.Unmarshal(data, &envelope); err != nil {
				return nil, err
			}
			if state := c.awsState(name, envelope.KeyName); state != nil {
				if err := state.lock(ctx); err != nil {
					return nil, err
				}
				defer state.unlock()
				if c.now().Before(state.rotatedUntil) {
					return &envelope, nil
				}
				defer func() {
					if rotated {
						state.rotatedUntil = c.now().Add(c.refreshInterval())
					}
				}()
			}
		}
	}
	for attempt := 0; attempt < 3; attempt++ {
		next, retry, err := s.rotateNamespaceKeyOnce(ctx, name)
		if !retry {
			rotated = err == nil
			return next, err
		}
	}
	return nil, fmt.Errorf("%w: namespace key changed concurrently", kms.ErrKeyUnavailable)
}

func (s *Store) rotateNamespaceKeyOnce(ctx context.Context, name string) (*Envelope, bool, error) {
	c := s.crypt()
	if c == nil {
		return nil, false, nil
	}
	key := keyRecordPrefix + name
	data, tag, err := s.b.GetWithETag(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var old Envelope
	if err := json.Unmarshal(data, &old); err != nil {
		return nil, false, err
	}
	var dek []byte
	if c.awsState(name, old.KeyName) != nil {
		if k, ok := c.cached(name); ok && k.version == old.KeyVersion {
			dek = k.key
		}
	}
	if dek == nil {
		dek, err = c.provider.Unwrap(ctx, old.KeyName, old.DEKWrapped)
		if err != nil {
			return nil, false, fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
		}
	}
	wrapped, version, err := c.provider.Wrap(ctx, old.KeyName, dek)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", kms.ErrKeyUnavailable, err)
	}
	if version == old.KeyVersion && !c.leaseCadenced(old.KeyName) {
		return &old, false, nil
	}
	old.KeyVersion, old.DEKWrapped = version, wrapped
	next, err := json.Marshal(old)
	if err != nil {
		return nil, false, err
	}
	ok, err := s.b.PutIfMatch(ctx, key, next, tag)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, true, nil
	}
	c.remember(name, dek, version, c.cacheTTL(name, old.KeyName))
	return &old, false, nil
}

func aeadForKey(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func blockNonce(base []byte, block uint64) [12]byte {
	var nonce [12]byte
	copy(nonce[:], base)
	// XOR preserves a distinct nonce for every block of one object. Base is
	// random per object, and the 64-bit block number cannot wrap for int64 sizes.
	binary.BigEndian.PutUint64(nonce[4:], binary.BigEndian.Uint64(nonce[4:])^block)
	return nonce
}

func blockAAD(object string, header []byte, index uint64) []byte {
	aad := make([]byte, 0, len(object)+len(header)+8)
	aad = append(aad, object...)
	aad = append(aad, header...)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], index)
	return append(aad, number[:]...)
}

func encryptObject(object string, plain, key []byte) ([]byte, error) {
	aead, err := aeadForKey(key)
	if err != nil {
		return nil, err
	}
	blocks := 0
	if len(plain) > 0 {
		blocks = (len(plain)-1)/encryptedBlockSize + 1
	}
	if blocks > (math.MaxInt-encryptedHeaderSize-len(plain))/encryptedTagSize {
		return nil, ErrRange
	}
	out := make([]byte, encryptedHeaderSize, encryptedHeaderSize+len(plain)+blocks*encryptedTagSize)
	copy(out[:4], encryptedMagic[:])
	out[4] = 1
	binary.BigEndian.PutUint64(out[5:13], uint64(len(plain)))
	if _, err := io.ReadFull(rand.Reader, out[13:25]); err != nil {
		return nil, err
	}
	for i := range blocks {
		start := i * encryptedBlockSize
		end := min(start+encryptedBlockSize, len(plain))
		nonce := blockNonce(out[13:25], uint64(i))
		out = aead.Seal(out, nonce[:], plain[start:end], blockAAD(object, out[:encryptedHeaderSize], uint64(i)))
	}
	return out, nil
}

func parseEncryptedHeader(header []byte) (int64, error) {
	if len(header) != encryptedHeaderSize || string(header[:4]) != string(encryptedMagic[:]) || header[4] != 1 {
		return 0, errors.New("invalid encrypted object header")
	}
	size := binary.BigEndian.Uint64(header[5:13])
	if size > math.MaxInt64 {
		return 0, ErrRange
	}
	return int64(size), nil
}

func decryptBlocks(object string, header, ciphertext, key []byte, first, last int64) ([]byte, error) {
	aead, err := aeadForKey(key)
	if err != nil {
		return nil, err
	}
	size, err := parseEncryptedHeader(header)
	if err != nil {
		return nil, err
	}
	// The ciphertext has already been read and bounds the allocation even
	// when the untrusted header advertises an absurd plaintext length.
	out := make([]byte, 0, len(ciphertext))
	for i := first; i < last; i++ {
		plainLen := min(int64(encryptedBlockSize), size-i*encryptedBlockSize)
		if plainLen < 0 || int64(len(ciphertext)) < plainLen+encryptedTagSize {
			return nil, io.ErrUnexpectedEOF
		}
		nonce := blockNonce(header[13:25], uint64(i))
		out, err = aead.Open(out, nonce[:], ciphertext[:plainLen+encryptedTagSize], blockAAD(object, header, uint64(i)))
		if err != nil {
			return nil, err
		}
		ciphertext = ciphertext[plainLen+encryptedTagSize:]
	}
	if len(ciphertext) != 0 {
		return nil, errors.New("trailing encrypted object bytes")
	}
	return out, nil
}

func decryptObject(object string, data, key []byte) ([]byte, error) {
	if len(data) < encryptedHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	size, err := parseEncryptedHeader(data[:encryptedHeaderSize])
	if err != nil {
		return nil, err
	}
	blocks := int64(0)
	if size > 0 {
		blocks = (size-1)/encryptedBlockSize + 1
	}
	if blocks > (math.MaxInt64-encryptedHeaderSize-size)/encryptedTagSize || int64(len(data)) != encryptedHeaderSize+size+blocks*encryptedTagSize {
		return nil, io.ErrUnexpectedEOF
	}
	return decryptBlocks(object, data[:encryptedHeaderSize], data[encryptedHeaderSize:], key, 0, blocks)
}

func (s *Store) encryptedRange(ctx context.Context, object string, offset, length int64, key []byte) ([]byte, error) {
	header, err := s.b.GetRange(ctx, object, 0, encryptedHeaderSize)
	if err != nil {
		return nil, err
	}
	size, err := parseEncryptedHeader(header)
	if err != nil {
		return nil, OpErr("get-range", object, err)
	}
	if offset > size || length > size-offset {
		return nil, OpErr("get-range", object, ErrRange)
	}
	first := offset / encryptedBlockSize
	last := (offset+length-1)/encryptedBlockSize + 1
	if last > (math.MaxInt64-encryptedHeaderSize)/(encryptedBlockSize+encryptedTagSize) {
		return nil, OpErr("get-range", object, ErrRange)
	}
	physicalStart := encryptedHeaderSize + first*(encryptedBlockSize+encryptedTagSize)
	lastPlain := min(size, last*encryptedBlockSize)
	physicalLength := lastPlain - first*encryptedBlockSize + (last-first)*encryptedTagSize
	data, err := s.b.GetRange(ctx, object, physicalStart, physicalLength)
	if err != nil {
		return nil, err
	}
	plain, err := decryptBlocks(object, header, data, key, first, last)
	if err != nil {
		return nil, OpErr("get-range", object, err)
	}
	start := offset - first*encryptedBlockSize
	out := plain[start : start+length]
	if length*4 < int64(cap(plain)) {
		// A small range out of up to two 64 KiB blocks: copy it rather
		// than pin the whole decrypted span behind a few cached bytes.
		out = bytes.Clone(out)
	}
	return out, nil
}

// looksEncrypted reports whether data starts with the encrypted object
// header.
func looksEncrypted(data []byte) bool {
	return len(data) >= encryptedHeaderSize && string(data[:4]) == string(encryptedMagic[:]) && data[4] == 1
}

// open returns stored bytes as the caller wrote them. A nil dek means the
// namespace had no key record when the lookup ran; bytes carrying the
// encrypted header then force one fresh lookup, because a record another
// process installed within the negative-cache TTL would otherwise hand back
// ciphertext as plaintext.
func (s *Store) open(ctx context.Context, op, key string, data, dek []byte) ([]byte, error) {
	if dek == nil {
		c := s.crypt()
		name := namespaceObject(key)
		if c == nil || name == "" || !looksEncrypted(data) {
			return data, nil
		}
		var err error
		if dek, err = s.lookup(ctx, c, name, false); err != nil {
			return nil, OpErr(op, key, err)
		}
		if dek == nil {
			return data, nil
		}
	}
	// Ciphertext always decrypts; Config.AcceptPlaintext decides only
	// whether bytes without the header may pass as a plaintext object.
	if !looksEncrypted(data) && s.accept(key, data) {
		return data, nil
	}
	plain, err := decryptObject(key, data, dek)
	return plain, OpErr(op, key, err)
}
