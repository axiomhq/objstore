package objstore

import (
	"context"

	"golang.org/x/time/rate"
)

// pacer is a token bucket with a one-request burst: a 32-wide prefetch
// never exceeds the configured rate, and slots go out in call order so a
// lease renewal never starves behind bulk reads. nil means unpaced.
type pacer struct{ l *rate.Limiter }

func newPacer(rps float64) *pacer {
	if rps <= 0 {
		return nil
	}
	return &pacer{l: rate.NewLimiter(rate.Limit(rps), 1)}
}

func (p *pacer) wait(ctx context.Context) error {
	if p == nil || IsUrgent(ctx) {
		return nil
	}
	return p.l.Wait(ctx)
}

// paced is a Backend whose object requests each take a pacer slot first.
// Bucket management is not paced.
type paced struct {
	Backend
	pace *pacer
}

func (p *paced) Put(ctx context.Context, key string, data []byte) error {
	if err := p.pace.wait(ctx); err != nil {
		return err
	}
	return p.Backend.Put(ctx, key, data)
}

func (p *paced) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if err := p.pace.wait(ctx); err != nil {
		return false, err
	}
	return p.Backend.PutIfAbsent(ctx, key, data)
}

func (p *paced) Get(ctx context.Context, key string) ([]byte, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, err
	}
	return p.Backend.Get(ctx, key)
}

func (p *paced) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, err
	}
	return p.Backend.GetRange(ctx, key, offset, length)
}

func (p *paced) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	return p.Backend.GetWithETag(ctx, key)
}

func (p *paced) GetIfChanged(ctx context.Context, key, etag string) ([]byte, string, bool, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, "", false, err
	}
	return p.Backend.GetIfChanged(ctx, key, etag)
}

func (p *paced) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (bool, error) {
	if err := p.pace.wait(ctx); err != nil {
		return false, err
	}
	return p.Backend.PutIfMatch(ctx, key, data, etag)
}

func (p *paced) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	return p.Backend.ListPage(ctx, prefix, after, limit)
}

func (p *paced) ListPrefixesPage(ctx context.Context, prefix, after string, limit int) ([]string, string, error) {
	if err := p.pace.wait(ctx); err != nil {
		return nil, "", err
	}
	return p.Backend.ListPrefixesPage(ctx, prefix, after, limit)
}

func (p *paced) Delete(ctx context.Context, key string) error {
	if err := p.pace.wait(ctx); err != nil {
		return err
	}
	return p.Backend.Delete(ctx, key)
}

func (p *paced) DeleteMany(ctx context.Context, keys ...string) error {
	if err := p.pace.wait(ctx); err != nil {
		return err
	}
	return p.Backend.DeleteMany(ctx, keys...)
}
