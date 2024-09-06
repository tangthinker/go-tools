package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type WindowCachePullFunc[T any] func(reqSize int) ([]T, error)

type WindowCacheConfig[T any] struct {
	TTL time.Duration

	PullFunc WindowCachePullFunc[T]

	DefaultPullSize int
}

type curPointer struct {
	cur atomic.Int32
}

func (c *curPointer) add(delta int) {
	c.cur.Add(int32(delta))
}

func (c *curPointer) set(delta int) {
	c.cur.Store(int32(delta))
}

func (c *curPointer) get() int {
	return int(c.cur.Load())
}

/*
WindowCache
in-memory cache for window data
*/
type WindowCache[T any] struct {
	data []T

	expAt time.Time

	neededCapacity int

	curPointer curPointer

	config *WindowCacheConfig[T]

	m sync.Mutex
}

func NewWindowCache[T any](config *WindowCacheConfig[T]) *WindowCache[T] {

	return &WindowCache[T]{
		data: make([]T, 0),

		config: config,

		expAt: time.Now().Add(config.TTL * -1),

		neededCapacity: config.DefaultPullSize,

		m: sync.Mutex{},
	}

}

func (wc *WindowCache[T]) Next(reqSize int) ([]T, error) {

	restSize := len(wc.data) - wc.curPointer.get()

	// cache update
	if restSize < reqSize || time.Now().After(wc.expAt) {
		wc.neededCapacity = reqSize * 5
		err := wc.refresh()

		if err != nil {
			return nil, fmt.Errorf("refresh err: %w", err)
		}
	}

	cur := wc.curPointer.get()

	if len(wc.data)-cur < reqSize {

		wc.curPointer.set(len(wc.data))

		return wc.data[cur:], nil
	}

	wc.curPointer.add(reqSize)
	return wc.data[cur : cur+reqSize], nil

}

func (wc *WindowCache[T]) refresh() error {

	wc.m.Lock()
	defer wc.m.Unlock()

	pulledData, err := wc.config.PullFunc(wc.neededCapacity)

	if err != nil {

		// downgrade

		wc.curPointer.set(0)
		if len(wc.data) != 0 {
			return nil
		}

		return fmt.Errorf("failed to pull data from cache: %w", err)

	}

	if len(pulledData) == 0 {

		// pull blank data

		wc.data = make([]T, 0)
		return nil

	}

	wc.data = pulledData

	wc.curPointer.set(0)

	wc.expAt = time.Now().Add(wc.config.TTL)

	return nil

}
