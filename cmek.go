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

// Backoff after a failed key lookup. A failed unwrap doubles the delay per
// name from minKeyBackoff to maxKeyBackoff. A failed record read backs off
// minKeyBackoff flat: in a namespace without a key record every write reads
// the record, and one transient GET failure must not stall them for long.
const (
	minKeyBackoff = 100 * time.Millisecond
	maxKeyBackoff = 5 * time.Second
)

// keySweepAt is the key cache size past which caching a new name first
// drops every expired entry.
const keySweepAt = 256

var encryptedMagic = [4]byte{'D', 'W', 'E', 'K'}

// Encrypted object format versions. v2 (written now) seals at least one
// block, so an empty object carries a tag bound to the key and header. v1
// sealed no block for an empty object; v1 objects that are not empty read
// as before.
const (
	encryptedV1 = 1
	encryptedV2 = 2
)

// cachedKey is a namespace's data key and its AES-GCM cipher, or with key
// nil the namespace's lack of a key record (a plaintext namespace),
// trusted until until.
type cachedKey struct {
	key     []byte
	aead    cipher.AEAD
	until   time.Time
	version string
}

// newCachedKey returns dek at version with its cipher built.
func newCachedKey(dek []byte, version string) (cachedKey, error) {
	aead, err := aeadForKey(dek)
	if err != nil {
		return cachedKey{}, err
	}
	return cachedKey{key: dek, aead: aead, version: version}, nil
}

// keyFailure is an open backoff. storage marks a failed record read (the
// store's fault, not the key's): it is not reported as
// kms.ErrKeyUnavailable.
type keyFailure struct {
	next    time.Time
	delay   time.Duration
	cause   error
	storage bool
}

type cmekState struct {
	provider kms.KeyProvider
	mu       sync.Mutex
	keys     map[string]cachedKey
	failures map[string]keyFailure
	aws      map[string]*awsRefresh
	// epoch counts the Install, Retire and forget calls of every name. A
	// lookup caches its result, or opens a backoff, only if epoch did not
	// move while it ran, so a lookup that saw "no record" just before an
	// Install cannot cache that stale answer after it. Being store-wide, it
	// can cost an unrelated in-flight lookup its cache entry, never more;
	// it only grows, so a stale read can never match it again. open does
	// not move it: a stale "no record" it lets through is re-checked by
	// every read and trusted by no write.
	epoch uint64
	// sweepAt is the key cache size that triggers the next sweep.
	sweepAt int
	// interval is the Store's KeyRefreshInterval, shared with the Store so
	// a value set before ConfigureCMEK, or without a provider, is kept.
	interval *atomic.Int64
	// flight collapses concurrent record reads and unwraps per namespace;
	// checks does the same for CheckNamespaceKey's revalidations. Urgent
	// callers use flights of their own (see lookup).
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

func newCMEKState(provider kms.KeyProvider, interval *atomic.Int64) *cmekState {
	return &cmekState{
		provider: provider,
		keys:     make(map[string]cachedKey),
		failures: make(map[string]keyFailure),
		aws:      make(map[string]*awsRefresh),
		sweepAt:  keySweepAt,
		interval: interval,
		now:      time.Now,
		ttl:      keyCacheTTL,
	}
}

// crypt returns the encryption state, nil when none is configured.
func (s *Store) crypt() *cmekState { return s.cmek.Load() }

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
	s.cmek.Store(newCMEKState(provider, s.refresh))
}

// SetCMEKRefreshInterval ties access and rotation probes for keys whose
// provider is lease-cadenced (AWS KMS) to the lease heartbeat cadence, as
// Config.KeyRefreshInterval does. Set it once, before use; before or after
// ConfigureCMEK.
func (s *Store) SetCMEKRefreshInterval(interval time.Duration) {
	s.refresh.Store(int64(interval))
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

// lockAWS returns name's lease-cadence state locked, or nil when keyName
// has none. A state dropped (invalidate, or an idle sweep) while this
// waited for it is released and the current one locked instead. A held
// state can still be replaced by invalidate, so a holder of the old state
// and one of the new can overlap; the cost is a duplicate unwrap or a
// rotation's CAS retry.
func (c *cmekState) lockAWS(ctx context.Context, name, keyName string) (*awsRefresh, error) {
	for {
		state := c.awsState(name, keyName)
		if state == nil {
			return nil, nil
		}
		if err := state.lock(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		current := c.aws[name] == state
		c.mu.Unlock()
		if current {
			return state, nil
		}
		state.unlock()
	}
}

func (c *cmekState) cacheTTL(name, keyName string) time.Duration {
	if c.awsState(name, keyName) != nil {
		return max(c.ttl, c.refreshInterval())
	}
	return c.ttl
}

// keyUnavailable prefixes err with what and wraps it with
// kms.ErrKeyUnavailable, once: an err that already carries it is not
// wrapped again.
func keyUnavailable(what string, err error) error {
	if errors.Is(err, kms.ErrKeyUnavailable) {
		return fmt.Errorf("%s: %w", what, err)
	}
	return fmt.Errorf("%w: %s: %w", kms.ErrKeyUnavailable, what, err)
}

// retryReady returns nil, or while name's backoff is open an error naming
// the namespace and wrapping the last failure (and kms.ErrKeyUnavailable,
// unless that was a record read).
func (c *cmekState) retryReady(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.failures[name]
	if !c.now().Before(f.next) {
		return nil
	}
	what := fmt.Sprintf("namespace %q backing off after", name)
	if f.storage {
		return fmt.Errorf("%s: %w", what, f.cause)
	}
	return keyUnavailable(what, f.cause)
}

// fail returns err wrapped with the namespace, and for a key failure
// (!storage) with kms.ErrKeyUnavailable. Unless epoch moved since the
// lookup began (an Install, Retire or forget ran), it also opens name's
// backoff: minKeyBackoff flat for a failed record read (storage), which
// leaves a cached key alone; doubling per failure for the key's, which
// drops it. The lookup runs detached from its callers, so no error here
// is a caller's (see load for its own timeout).
func (c *cmekState) fail(epoch uint64, name string, err error, storage bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epoch == epoch {
		f := c.failures[name]
		delay := minKeyBackoff
		if !storage {
			f.delay = min(maxKeyBackoff, max(minKeyBackoff, f.delay*2))
			delay = f.delay
			delete(c.keys, name)
		}
		f.next = c.now().Add(delay)
		f.cause = err
		f.storage = storage
		c.failures[name] = f
	}
	what := fmt.Sprintf("namespace %q", name)
	if storage {
		return fmt.Errorf("%s: %w", what, err)
	}
	return keyUnavailable(what, err)
}

// cached returns name's cached lookup, if still fresh. An expired entry is
// dropped when read, with the name's lease-cadence state once that is
// idle; entries never read again are dropped by the sweep in put.
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

// currentEpoch returns the invalidation count; see cmekState.epoch.
func (c *cmekState) currentEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// put caches name's lookup and closes its backoff. Once the cache holds
// sweepAt names, caching a new one first drops every expired entry (and
// idle lease-cadence state), and the next sweep waits for twice what is
// left, so sweeping stays linear overall. Callers hold c.mu.
func (c *cmekState) put(name string, k cachedKey, ttl time.Duration) {
	now := c.now()
	if _, ok := c.keys[name]; !ok && len(c.keys) >= c.sweepAt {
		for n, k := range c.keys {
			if now.Before(k.until) {
				continue
			}
			delete(c.keys, n)
			if a := c.aws[n]; a != nil && a.idle(now) {
				delete(c.aws, n)
			}
		}
		c.sweepAt = max(keySweepAt, 2*len(c.keys))
	}
	k.until = now.Add(ttl)
	c.keys[name] = k
	delete(c.failures, name)
}

// rememberAt is remember for a lookup that began at epoch: it caches
// nothing if any name was invalidated since. absent also drops the name's
// lease-cadence state, as for a retired namespace.
func (c *cmekState) rememberAt(epoch uint64, name string, k cachedKey, ttl time.Duration, absent bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epoch != epoch {
		return
	}
	if absent {
		delete(c.aws, name)
	}
	c.put(name, k, ttl)
}

// invalidate drops name's cached key and lease-cadence state (and with
// failures its backoff), moves the epoch, and detaches in-flight lookups
// so later callers start a fresh one.
func (c *cmekState) invalidate(name string, failures bool) {
	c.mu.Lock()
	c.epoch++
	delete(c.keys, name)
	delete(c.aws, name)
	if failures {
		delete(c.failures, name)
	}
	c.mu.Unlock()
	forgetFlights(&c.flight, name)
	forgetFlights(&c.checks, name)
}

// drop forgets name's cached lookup and detaches its in-flight lookups so
// the next caller starts a fresh one. Unlike invalidate it does not move
// the epoch, so lookups of other names still cache their results.
func (c *cmekState) drop(name string) {
	c.mu.Lock()
	delete(c.keys, name)
	c.mu.Unlock()
	forgetFlights(&c.flight, name)
}

func (c *cmekState) remember(name string, k cachedKey, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.put(name, k, ttl)
}

// urgentFlight suffixes the flight key of an Urgent caller's lookup.
const urgentFlight = "\x00urgent"

// forgetFlights detaches name's in-flight lookups in g, urgent or not.
func forgetFlights(g *singleflight.Group, name string) {
	g.Forget(name)
	g.Forget(name + urgentFlight)
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
	// A backoff opened before the record existed (or while it was being
	// installed) must not outlive it.
	c.invalidate(name, true)
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
		aead, err := s.objectKey(ctx, object, false)
		if err != nil {
			return err
		}
		plain, err = decryptObject(object, raw, aead)
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
		aead, err := s.objectKey(ctx, object, false)
		if err != nil {
			return err
		}
		plain, err := decryptObject(object, raw, aead)
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

// objectKey returns the data key's cipher for object: nil for an object outside
// ns/<name>/ or in a namespace without a key record. Lookups are cached for
// the key cache TTL, and concurrent lookups of one namespace share one
// record read and one unwrap.
//
// A cached "no record" is trusted only when trustAbsent: the caller reads
// whole objects and passes them through open, which re-checks bytes that
// carry the encrypted header. A range read cannot tell, and a write would
// store plaintext in a namespace another process has just keyed, so for
// them a cached "no record" re-reads the record (one GET, shared).
func (s *Store) objectKey(ctx context.Context, object string, trustAbsent bool) (cipher.AEAD, error) {
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
	if k, ok := c.cached(name); ok && (k.key != nil || trustAbsent) {
		return k.aead, nil
	}
	return s.lookup(ctx, c, name, false)
}

// lookup runs load for name once for all concurrent callers. The shared
// load does not inherit a caller's cancellation, so one caller giving up
// neither fails the others nor opens the name's backoff; each caller still
// returns on its own ctx. The load is bounded by keyLookupTimeout. It
// keeps the values of the ctx that started it, Urgent among them, so
// Urgent callers (a lease heartbeat, a log commit) share a flight of their
// own: joining a bulk caller's would wait behind the pacer.
func (s *Store) lookup(ctx context.Context, c *cmekState, name string, check bool) (cipher.AEAD, error) {
	if err := c.retryReady(name); err != nil {
		return nil, err
	}
	group := &c.flight
	if check {
		group = &c.checks
	}
	key := name
	if IsUrgent(ctx) {
		key += urgentFlight
	}
	detached := context.WithoutCancel(ctx)
	ch := group.DoChan(key, func() (any, error) {
		lctx, cancel := context.WithTimeout(detached, keyLookupTimeout)
		defer cancel()
		return c.load(lctx, s.b, name, check)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		aead, _ := r.Val.(cipher.AEAD)
		return aead, r.Err
	}
}

// load reads name's key record and unwraps its data key, caching it and
// returning its cipher: nil, nil (also cached) for a namespace without a
// record. A failed or unparseable record read backs off as the store's
// failure, anything later as the key's (see fail). check
// is the request-boundary revalidation: a lease-cadenced key skips the
// unwrap while its last check is fresh, where a plain load skips it while
// the cached key matches the record's version. Nothing is cached if name
// was invalidated while load ran.
//
// ctx is detached from every caller, so a context error here is the
// lookup's own timeout (or the store's or SDK's): it is flattened into the
// message, never wrapped, so a caller whose own ctx is fine does not
// mistake it for its own deadline.
func (c *cmekState) load(ctx context.Context, b Backend, name string, check bool) (cipher.AEAD, error) {
	epoch := c.currentEpoch()
	start := time.Now()
	failed := func(err error, storage bool) error {
		elapsed := time.Since(start).Round(time.Millisecond)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			err = fmt.Errorf("key lookup timed out after %v: %v", elapsed, err)
		case errors.Is(err, context.Canceled):
			err = fmt.Errorf("key lookup canceled after %v: %v", elapsed, err)
		}
		return c.fail(epoch, name, err, storage)
	}
	data, err := b.Get(ctx, keyRecordPrefix+name)
	if errors.Is(err, ErrNotFound) {
		c.rememberAt(epoch, name, cachedKey{}, c.ttl, true)
		return nil, nil
	}
	if err != nil {
		return nil, failed(err, true)
	}
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, failed(fmt.Errorf("invalid key record: %w", err), true)
	}
	state, err := c.lockAWS(ctx, name, envelope.KeyName)
	if err != nil {
		return nil, failed(err, false)
	}
	if state != nil {
		defer state.unlock()
		if check && c.now().Before(state.checkedUntil) {
			return nil, nil
		}
		// Another request may have renewed the cached DEK while this one
		// fetched the envelope.
		if k, ok := c.cached(name); !check && ok && k.key != nil && k.version == envelope.KeyVersion {
			return k.aead, nil
		}
	}
	dek, err := c.provider.Unwrap(ctx, envelope.KeyName, envelope.DEKWrapped)
	if err != nil {
		return nil, failed(err, false)
	}
	if len(dek) != kms.DEKSize {
		return nil, failed(fmt.Errorf("unwrapped data key is %d bytes, want %d", len(dek), kms.DEKSize), false)
	}
	k, err := newCachedKey(dek, envelope.KeyVersion)
	if err != nil {
		return nil, failed(err, false)
	}
	c.rememberAt(epoch, name, k, c.cacheTTL(name, envelope.KeyName), false)
	if state != nil {
		state.checkedUntil = c.now().Add(c.refreshInterval())
	}
	return k.aead, nil
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
			state, err := c.lockAWS(ctx, name, envelope.KeyName)
			if err != nil {
				return nil, err
			}
			if state != nil {
				defer state.unlock()
				// The lock is shared with key lookups: bound how long this
				// holds it whatever the caller's deadline.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, keyLookupTimeout)
				defer cancel()
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
			return nil, false, keyUnavailable(fmt.Sprintf("rotate namespace %q: unwrap", name), err)
		}
	}
	wrapped, version, err := c.provider.Wrap(ctx, old.KeyName, dek)
	if err != nil {
		return nil, false, keyUnavailable(fmt.Sprintf("rotate namespace %q: wrap", name), err)
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
	k, err := newCachedKey(dek, version)
	if err != nil {
		return nil, false, err
	}
	c.remember(name, k, c.cacheTTL(name, old.KeyName))
	return &old, false, nil
}

func aeadForKey(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// blockNonce writes block's nonce into nonce (12 bytes) and returns it,
// so one buffer serves every block of a call.
func blockNonce(nonce, base []byte, block uint64) []byte {
	copy(nonce, base)
	// XOR preserves a distinct nonce for every block of one object. Base is
	// random per object, and the 64-bit block number cannot wrap for int64 sizes.
	binary.BigEndian.PutUint64(nonce[4:], binary.BigEndian.Uint64(nonce[4:])^block)
	return nonce
}

// blockAAD returns a block's additional data, object then header then an
// 8-byte block number, with the number left for setBlock: one buffer
// serves every block of a call.
func blockAAD(object string, header []byte) []byte {
	aad := make([]byte, len(object)+len(header)+8)
	copy(aad[copy(aad, object):], header)
	return aad
}

// setBlock writes index into aad's trailing block number and returns aad.
func setBlock(aad []byte, index uint64) []byte {
	binary.BigEndian.PutUint64(aad[len(aad)-8:], index)
	return aad
}

func encryptObject(object string, plain []byte, aead cipher.AEAD) ([]byte, error) {
	// At least one block: an empty object still carries a tag, so a bare
	// header cannot pass as an authentic empty object.
	blocks := max(1, (len(plain)+encryptedBlockSize-1)/encryptedBlockSize)
	if blocks > (math.MaxInt-encryptedHeaderSize-len(plain))/encryptedTagSize {
		return nil, ErrRange
	}
	out := make([]byte, encryptedHeaderSize, encryptedHeaderSize+len(plain)+blocks*encryptedTagSize)
	copy(out[:4], encryptedMagic[:])
	out[4] = encryptedV2
	binary.BigEndian.PutUint64(out[5:13], uint64(len(plain)))
	if _, err := io.ReadFull(rand.Reader, out[13:25]); err != nil {
		return nil, err
	}
	aad := blockAAD(object, out[:encryptedHeaderSize])
	nonce := make([]byte, 12)
	for i := range blocks {
		start := i * encryptedBlockSize
		end := min(start+encryptedBlockSize, len(plain))
		out = aead.Seal(out, blockNonce(nonce, out[13:25], uint64(i)), plain[start:end], setBlock(aad, uint64(i)))
	}
	return out, nil
}

// ErrLegacyEmptyObject is wrapped by a read of an empty object written in
// encrypted format v1 (before v0.5.0), which sealed no block: nothing
// authenticates it and a forged header would read the same, so it is
// refused. The error names the key. Such an object is exactly 25 bytes in
// the bucket: "DWEK", then version byte 1, then eight zero bytes (the
// length) and a nonce. If empty is the expected content, Put an empty
// value again (format v2 seals a tag); otherwise delete it.
var ErrLegacyEmptyObject = errors.New("objstore: legacy (format v1) empty encrypted object has no authentication tag; rewrite it")

// parseEncryptedHeader returns the plaintext size a v1 or v2 header
// declares, and the number of sealed blocks that follow it.
func parseEncryptedHeader(header []byte) (size, blocks int64, err error) {
	if len(header) != encryptedHeaderSize || !looksEncrypted(header) {
		return 0, 0, errors.New("invalid encrypted object header")
	}
	u := binary.BigEndian.Uint64(header[5:13])
	if u > math.MaxInt64 {
		return 0, 0, ErrRange
	}
	size = int64(u)
	if size == 0 {
		if header[4] == encryptedV1 {
			return 0, 0, ErrLegacyEmptyObject
		}
		return 0, 1, nil
	}
	return size, (size-1)/encryptedBlockSize + 1, nil
}

func decryptBlocks(object string, header, ciphertext []byte, aead cipher.AEAD, first, last int64) ([]byte, error) {
	size, _, err := parseEncryptedHeader(header)
	if err != nil {
		return nil, err
	}
	// The ciphertext has already been read and bounds the allocation even
	// when the untrusted header advertises an absurd plaintext length.
	out := make([]byte, 0, len(ciphertext))
	aad := blockAAD(object, header)
	nonce := make([]byte, 12)
	for i := first; i < last; i++ {
		plainLen := min(int64(encryptedBlockSize), size-i*encryptedBlockSize)
		if plainLen < 0 || int64(len(ciphertext)) < plainLen+encryptedTagSize {
			return nil, io.ErrUnexpectedEOF
		}
		out, err = aead.Open(out, blockNonce(nonce, header[13:25], uint64(i)), ciphertext[:plainLen+encryptedTagSize], setBlock(aad, uint64(i)))
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

func decryptObject(object string, data []byte, aead cipher.AEAD) ([]byte, error) {
	if len(data) < encryptedHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	size, blocks, err := parseEncryptedHeader(data[:encryptedHeaderSize])
	if err != nil {
		return nil, err
	}
	if blocks > (math.MaxInt64-encryptedHeaderSize-size)/encryptedTagSize || int64(len(data)) != encryptedHeaderSize+size+blocks*encryptedTagSize {
		return nil, io.ErrUnexpectedEOF
	}
	return decryptBlocks(object, data[:encryptedHeaderSize], data[encryptedHeaderSize:], aead, 0, blocks)
}

func (s *Store) encryptedRange(ctx context.Context, object string, offset, length int64, aead cipher.AEAD) ([]byte, error) {
	header, err := s.b.GetRange(ctx, object, 0, encryptedHeaderSize)
	if err != nil {
		return nil, err
	}
	size, _, err := parseEncryptedHeader(header)
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
	plain, err := decryptBlocks(object, header, data, aead, first, last)
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

// looksEncrypted reports whether data starts with an encrypted object
// header of a known version.
func looksEncrypted(data []byte) bool {
	return len(data) >= encryptedHeaderSize && string(data[:4]) == string(encryptedMagic[:]) && (data[4] == encryptedV1 || data[4] == encryptedV2)
}

// open returns stored bytes as the caller wrote them. A nil aead means the
// namespace had no key record when the lookup ran; bytes carrying the
// encrypted header then force one fresh lookup, because a record another
// process installed within the negative-cache TTL would otherwise hand back
// ciphertext as plaintext.
func (s *Store) open(ctx context.Context, op, key string, data []byte, aead cipher.AEAD) ([]byte, error) {
	if aead == nil {
		c := s.crypt()
		name := namespaceObject(key)
		if c == nil || name == "" || !looksEncrypted(data) {
			return data, nil
		}
		// Start a fresh lookup: an in-flight one may have read the record
		// before another process installed it, and joining it would hand
		// back that stale "no record". The epoch stays put: that lookup may
		// still cache its "no record", which reads re-check and writes
		// never trust, and lookups of other names keep caching.
		c.drop(name)
		var err error
		if aead, err = s.lookup(ctx, c, name, false); err != nil {
			return nil, OpErr(op, key, err)
		}
		if aead == nil {
			return data, nil
		}
	}
	// Ciphertext always decrypts; Config.AcceptPlaintext decides only
	// whether bytes without the header may pass as a plaintext object.
	if !looksEncrypted(data) && s.accept(key, data) {
		return data, nil
	}
	plain, err := decryptObject(key, data, aead)
	return plain, OpErr(op, key, err)
}
