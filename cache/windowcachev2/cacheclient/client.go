package cacheclient

import "context"

type DirectCacheType string

type CacheClient interface {
	GetInt64(ctx context.Context, key string) (int64, error)

	SetInt64(ctx context.Context, key string, value int64) error

	//IncrInt64 must atomically increment the value
	IncrInt64(ctc context.Context, key string, delta int64) error

	// SetList the list is ordered
	SetList(ctx context.Context, key string, list []string) error

	// Range returns a range of elements from the list
	Range(ctx context.Context, key string, start, end int64) ([]string, error)
}
