package cacheclient

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type RedisCacheClient struct {
	client *redis.Client
}

func NewRedisCacheClient(client *redis.Client) *RedisCacheClient {
	return &RedisCacheClient{
		client: client,
	}
}

func (r RedisCacheClient) GetInt64(ctx context.Context, key string) (int64, error) {
	return r.client.Get(ctx, key).Int64()
}

func (r RedisCacheClient) SetInt64(ctx context.Context, key string, value int64) error {
	return r.client.Set(ctx, key, value, 0).Err()
}

func (r RedisCacheClient) IncrInt64(ctx context.Context, key string, delta int64) error {
	return r.client.IncrBy(ctx, key, delta).Err()
}

func (r RedisCacheClient) SetList(ctx context.Context, key string, list []string) error {

	var cacheData []*redis.Z

	for i := range list {
		cacheData = append(cacheData, &redis.Z{
			Score:  float64(i),
			Member: list[i]},
		)
	}

	return r.client.ZAdd(ctx, key, cacheData...).Err()

}

func (r RedisCacheClient) Range(ctx context.Context, key string, start, end int64) ([]string, error) {

	result, err := r.client.ZRange(ctx, key, start, end).Result()

	if err != nil {
		return nil, err
	}

	return result, nil

}
