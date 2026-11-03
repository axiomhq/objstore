//go:build js && wasm

// Package r2 adapts a Cloudflare Worker R2 bucket binding to objstore.
// Native programs should use aws/s3 with an R2 endpoint instead.
//
// Bindings refer to provisioned buckets: EnsureBucket checks accessibility;
// DropBucket is unsupported and never deletes objects. Streaming, signing and
// per-object KMS overrides are not supported. No operation is retried.
// R2 has no AbortSignal option: cancellation prevents dispatch, but an already
// dispatched operation waits for its Promise to settle. A write error does not
// prove that the write failed to commit.
package r2

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"syscall/js"

	"github.com/axiomhq/objstore"
)

// Backend uses the caller's R2Bucket binding, without credentials or HTTP.
type Backend struct{ bucket js.Value }

var _ objstore.Backend = (*Backend)(nil)

// New wraps a Worker environment's R2Bucket binding. The caller owns its lifetime.
func New(bucket js.Value) *Backend { return &Backend{bucket: bucket} }

// Open adds objstore's pacing and write bound to the binding.
func Open(bucket js.Value, cfg objstore.Config) *objstore.Store {
	return objstore.Open(New(bucket), cfg)
}

func (b *Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := b.put(ctx, "put", key, data, nil)
	return err
}

func (b *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	return b.put(ctx, "put-if-absent", key, data, map[string]any{"etagDoesNotMatch": "*"})
}

func (b *Backend) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	return b.put(ctx, "put-if-match", key, data, map[string]any{"etagMatches": strings.Trim(etag, `"`)})
}

func (b *Backend) put(ctx context.Context, op, key string, data []byte, condition map[string]any) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, objstore.OpErr(op, key, err)
	}
	if objstore.KMSKey(ctx) != "" {
		return false, objstore.OpErr(op, key, errors.ErrUnsupported)
	}
	buffer := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(buffer, data)
	opts := map[string]any{}
	if condition != nil {
		opts["onlyIf"] = condition
	}
	object, err := call(ctx, b.bucket, "put", key, buffer, opts)
	if err != nil {
		return false, objstore.OpErr(op, key, err)
	}
	return !object.IsNull(), nil
}

func (b *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, _, err := b.get(ctx, "get", key, nil)
	return data, err
}

func (b *Backend) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, _, err := b.get(ctx, "get-with-etag", key, nil)
	return data, etag, err
}

func (b *Backend) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	opts := map[string]any{}
	if etag != "" {
		opts["onlyIf"] = map[string]any{"etagDoesNotMatch": strings.Trim(etag, `"`)}
	}
	return b.get(ctx, "get-if-changed", key, opts)
}

func (b *Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	// JavaScript numbers cannot exactly represent integers above 2^53-1.
	const maxExact = 1<<53 - 1
	if offset < 0 || length <= 0 || length > maxExact || offset > maxExact-length {
		return nil, objstore.OpErr("get-range", key, objstore.ErrRange)
	}
	headers := js.Global().Get("Headers").New(map[string]any{
		"Range": fmt.Sprintf("bytes=%d-%d", offset, offset+length-1),
	})
	object, err := call(ctx, b.bucket, "get", key, map[string]any{"range": headers})
	if err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	if object.IsNull() {
		return nil, objstore.OpErr("get-range", key, objstore.ErrNotFound)
	}
	// A service may ignore an unsatisfiable Range header or shorten a range
	// at EOF. Neither is an exact ranged read. Check before buffering a body.
	span := object.Get("range")
	if span.IsUndefined() || span.Get("offset").Float() != float64(offset) || span.Get("length").Float() != float64(length) {
		_, _ = call(context.WithoutCancel(ctx), object.Get("body"), "cancel")
		return nil, objstore.OpErr("get-range", key, objstore.ErrRange)
	}
	data, err := read(ctx, object)
	if err != nil {
		return nil, objstore.OpErr("get-range", key, err)
	}
	if int64(len(data)) != length {
		return nil, objstore.OpErr("get-range", key, objstore.ErrRange)
	}
	return data, nil
}

func read(ctx context.Context, object js.Value) ([]byte, error) {
	buffer, err := call(ctx, object, "arrayBuffer")
	if err != nil {
		// Cancellation before arrayBuffer dispatch leaves an unread body.
		_, _ = call(context.WithoutCancel(ctx), object.Get("body"), "cancel")
		return nil, err
	}
	array := js.Global().Get("Uint8Array").New(buffer)
	data := make([]byte, array.Get("byteLength").Int())
	js.CopyBytesToGo(data, array)
	return data, nil
}

func (b *Backend) get(ctx context.Context, op, key string, opts map[string]any) ([]byte, string, bool, error) {
	if opts == nil {
		opts = map[string]any{}
	}
	object, err := call(ctx, b.bucket, "get", key, opts)
	if err != nil {
		return nil, "", false, objstore.OpErr(op, key, err)
	}
	if object.IsNull() {
		return nil, "", false, objstore.OpErr(op, key, objstore.ErrNotFound)
	}
	etag := object.Get("etag").String()
	if object.Get("body").IsUndefined() {
		return nil, etag, true, nil
	}
	data, err := read(ctx, object)
	if err != nil {
		return nil, "", false, objstore.OpErr(op, key, err)
	}
	return data, etag, false, nil
}

func (b *Backend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	return b.list(ctx, "list-page", prefix, after, limit, false)
}

func (b *Backend) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	return b.list(ctx, "list-prefixes-page", prefix, after, limit, true)
}

func (b *Backend) list(ctx context.Context, op, prefix, after string, limit int, prefixes bool) ([]string, string, error) {
	if limit < 1 || limit > objstore.MaxListPage {
		return nil, "", objstore.OpErr(op, prefix, fmt.Errorf("limit must be between 1 and %d", objstore.MaxListPage))
	}
	opts := map[string]any{"prefix": prefix, "startAfter": after, "limit": limit}
	if prefixes {
		opts["delimiter"] = "/"
	}
	for {
		page, err := call(ctx, b.bucket, "list", opts)
		if err != nil {
			return nil, "", objstore.OpErr(op, prefix, err)
		}
		items := page.Get("objects")
		if prefixes {
			items = page.Get("delimitedPrefixes")
		}
		keys := make([]string, 0, items.Length())
		for i := 0; i < items.Length(); i++ {
			item := items.Index(i)
			if !prefixes {
				item = item.Get("key")
			}
			// Some R2 runtimes apply startAfter to keys before grouping them,
			// so a child prefix at/before after can reappear.
			if key := item.String(); key > after {
				keys = append(keys, key)
			}
		}
		if !page.Get("truncated").Bool() {
			return keys, "", nil
		}
		if len(keys) > 0 {
			return keys, keys[len(keys)-1], nil
		}
		// A page containing only loose objects or excluded prefixes is not
		// EOF. Follow the service cursor; callers still receive lexical keys.
		opts["cursor"] = page.Get("cursor").String()
	}
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := call(ctx, b.bucket, "delete", key)
	return objstore.OpErr("delete", key, err)
}

func (b *Backend) DeleteMany(ctx context.Context, keys ...string) error {
	for len(keys) > 0 {
		if err := ctx.Err(); err != nil {
			return objstore.DeleteFailures(keys, err, true)
		}
		n := min(len(keys), 1000) // R2's per-request delete limit.
		batch := make([]any, n)
		for i, key := range keys[:n] {
			batch[i] = key
		}
		if _, err := call(ctx, b.bucket, "delete", batch); err != nil {
			failure := &objstore.DeleteError{}
			for i, key := range keys {
				failure.Failures = append(failure.Failures, objstore.DeleteFailure{
					Key: key, Err: objstore.OpErr("delete-many", key, err), Unattempted: i >= n,
				})
			}
			return failure
		}
		keys = keys[n:]
	}
	return nil
}

// EnsureBucket checks the provisioned binding; it cannot create a bucket.
func (b *Backend) EnsureBucket(ctx context.Context) error {
	_, err := call(ctx, b.bucket, "list", map[string]any{"limit": 1})
	return objstore.OpErr("ensure-bucket", "", err)
}

// DropBucket never empties or deletes a binding's bucket.
func (b *Backend) DropBucket(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return objstore.OpErr("drop-bucket", "", err)
	}
	return objstore.OpErr("drop-bucket", "", errors.ErrUnsupported)
}

// call converts both synchronous JS exceptions and Promise rejections to errors.
func call(ctx context.Context, receiver js.Value, method string, args ...any) (value js.Value, err error) {
	if err = ctx.Err(); err != nil {
		return value, err
	}
	defer func() {
		if p := recover(); p != nil {
			if e, ok := p.(js.Error); ok {
				err = bindingError(e.Value)
			} else {
				panic(p)
			}
		}
	}()
	promise := receiver.Call(method, args...)
	type result struct {
		value js.Value
		err   error
	}
	done := make(chan result, 1)
	resolve := js.FuncOf(func(_ js.Value, args []js.Value) any {
		done <- result{value: args[0]}
		return nil
	})
	reject := js.FuncOf(func(_ js.Value, args []js.Value) any {
		done <- result{err: bindingError(args[0])}
		return nil
	})
	defer resolve.Release()
	defer reject.Release()
	promise.Call("then", resolve, reject)
	r := <-done
	return r.value, r.err
}

func bindingError(value js.Value) error {
	message := js.Global().Get("String").Invoke(value).String()
	code := 0
	if value.Type() == js.TypeObject && !value.IsNull() {
		if n := value.Get("code"); n.Type() == js.TypeNumber {
			code = n.Int()
		}
	}
	// Older workerd versions expose R2 codes only in the message suffix.
	if code == 0 && strings.HasSuffix(message, ")") {
		if i := strings.LastIndexByte(message, '('); i >= 0 {
			code, _ = strconv.Atoi(message[i+1 : len(message)-1])
		}
	}
	var err error
	switch code {
	case 10003:
		err = objstore.ErrAccessDenied
	case 10006, 10007:
		err = objstore.ErrNotFound
	case 10020:
		err = objstore.ErrInvalidKey
	case 10039:
		err = objstore.ErrRange
	default:
		return errors.New(message)
	}
	return fmt.Errorf("%w: %s", err, message)
}
