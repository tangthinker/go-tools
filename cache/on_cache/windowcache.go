package windowcachev2

import (
	"context"
	"fmt"
	"github.com/tangthinker/go-tools/cache/on_cache/cacheclient"
	"time"
)

type Cache interface {
	Next(ctx context.Context, reqSize int) ([]string, error)
}

type PullFunc func(reqSize int) ([]string, error)

type Config struct {
	PullFunc PullFunc

	PullSize int

	TTL time.Duration

	Keys *CacheKeys
}

type CacheKeys struct {
	dataKey     string
	curKey      string
	totalKey    string
	expireAtKey string
}

func NewCacheKeys(prefix string) *CacheKeys {
	return &CacheKeys{
		dataKey:     fmt.Sprintf("%s-data", prefix),
		curKey:      fmt.Sprintf("%s-cur", prefix),
		totalKey:    fmt.Sprintf("%s-total", prefix),
		expireAtKey: fmt.Sprintf("%s-expire-at", prefix),
	}
}

type WindowCache struct {
	config Config

	cacheClient cacheclient.CacheClient
}

func NewWindowCache(client cacheclient.CacheClient, config *Config) *WindowCache {
	wc := &WindowCache{
		config:      *config,
		cacheClient: client,
	}
	wc.initKeys()
	return wc
}

func (c *WindowCache) Next(ctx context.Context, reqSize int) ([]string, error) {

	exp, err := c.expireAt(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()

	if exp == -1 || now > exp {
		ok, err := c.refresh(ctx)
		if err != nil && !ok {
			return nil, err
		}
		exp, err = c.expireAt(ctx)
		if err != nil {
			return nil, err
		}
	}

	cur, err := c.cur(ctx)
	if err != nil {
		return nil, err
	}

	total, err := c.total(ctx)
	if err != nil {
		return nil, err
	}

	if cur == total {
		ok, err := c.refresh(ctx)
		if err != nil && !ok {
			return nil, err
		}
		cur, err = c.cur(ctx)
		if err != nil {
			return nil, err
		}
		total, err = c.total(ctx)
		if err != nil {
			return nil, err
		}
	}

	if cur+int64(reqSize) > total {

		err := c.incrCur(ctx, total-cur)

		if err != nil {
			return nil, err
		}

		ret, err := c.rangeData(ctx, cur, total)

		if err != nil {
			return nil, err
		}

		return ret, nil
	}

	err = c.incrCur(ctx, int64(reqSize))
	if err != nil {
		return nil, err
	}

	ret, err := c.rangeData(ctx, cur, cur+int64(reqSize))

	if err != nil {
		return nil, err
	}

	return ret, nil

}

// refresh
// return true if pull data is not empty
// return false if pull data is empty
func (c *WindowCache) refresh(ctx context.Context) (bool, error) {

	data, err := c.config.PullFunc(c.config.PullSize)

	if err != nil {
		// downgrade

		// no data return nil
		t, err := c.total(ctx)
		if err != nil || t <= 0 {
			return false, err
		}

		// have bac data, set cur 0
		err = c.setCur(ctx, 0)
		return true, err

	}

	if len(data) == 0 {
		return true, nil
	}

	err = c.setData(ctx, data)
	if err != nil {
		return false, err
	}

	err = c.setTotal(ctx, int64(len(data)))
	if err != nil {
		return false, err
	}

	err = c.setCur(ctx, 0)
	if err != nil {
		return false, err
	}

	err = c.setExpireAt(ctx, time.Now().Add(c.config.TTL).Unix())
	if err != nil {
		return false, err
	}

	return true, nil

}

func (c *WindowCache) total(ctx context.Context) (int64, error) {
	t, err := c.cacheClient.GetInt64(ctx, c.config.Keys.totalKey)

	if err != nil {
		return 0, fmt.Errorf("window cache failed to get total key: %w", err)
	}

	return t, nil
}

func (c *WindowCache) initKeys() {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_ = c.setTotal(ctx, -1)
	_ = c.setCur(ctx, -1)
	_ = c.setExpireAt(ctx, -1)

}

func (c *WindowCache) setTotal(ctx context.Context, t int64) error {
	err := c.cacheClient.SetInt64(ctx, c.config.Keys.totalKey, t)

	if err != nil {
		return fmt.Errorf("window cache failed to set total key: %w", err)
	}

	return nil
}

func (c *WindowCache) cur(ctx context.Context) (int64, error) {
	t, err := c.cacheClient.GetInt64(ctx, c.config.Keys.curKey)

	if err != nil {
		return 0, fmt.Errorf("window cache failed to get cur key: %w", err)
	}

	return t, nil
}

func (c *WindowCache) setCur(ctx context.Context, t int64) error {
	err := c.cacheClient.SetInt64(ctx, c.config.Keys.curKey, t)

	if err != nil {
		return fmt.Errorf("window cache failed to set cur key: %w", err)
	}

	return nil
}

func (c *WindowCache) incrCur(ctx context.Context, t int64) error {
	err := c.cacheClient.IncrInt64(ctx, c.config.Keys.curKey, t)

	if err != nil {
		return fmt.Errorf("window cache failed to incr cur key: %w", err)
	}

	return nil

}

func (c *WindowCache) expireAt(ctx context.Context) (int64, error) {
	t, err := c.cacheClient.GetInt64(ctx, c.config.Keys.expireAtKey)

	if err != nil {
		return 0, fmt.Errorf("window cache failed to get expire at key: %w", err)
	}

	return t, nil
}

func (c *WindowCache) setExpireAt(ctx context.Context, t int64) error {
	err := c.cacheClient.SetInt64(ctx, c.config.Keys.expireAtKey, t)

	if err != nil {
		return fmt.Errorf("window cache failed to set expire at key: %w", err)
	}

	return nil
}

func (c *WindowCache) setData(ctx context.Context, data []string) error {
	if len(data) == 0 {
		return nil
	}

	err := c.cacheClient.SetList(ctx, c.config.Keys.dataKey, data)

	if err != nil {
		return fmt.Errorf("window cache failed to set data key: %w", err)
	}

	return nil

}

// range data
func (c *WindowCache) rangeData(ctx context.Context, start, end int64) ([]string, error) {
	data, err := c.cacheClient.Range(ctx, c.config.Keys.dataKey, start, end)

	if err != nil {
		return nil, fmt.Errorf("window cache failed to range data key: %w", err)
	}

	return data, nil
}
