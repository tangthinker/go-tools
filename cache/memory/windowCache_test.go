package memory

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestWindowCache(t *testing.T) {

	pullFunc := func(reqSize int) ([]int, error) {

		result := make([]int, reqSize)

		for i := 0; i < reqSize; i++ {
			randNum := rand.Intn(1000)
			result[i] = randNum
		}

		return result, nil
	}

	windowCache := NewWindowCache[int](&WindowCacheConfig[int]{
		TTL:             30 * time.Second,
		PullFunc:        pullFunc,
		DefaultPullSize: 10,
	})

	for i := 0; i < 1000; i++ {

		data, err := windowCache.Next(5)
		if err != nil {
			t.Error(err)
		}

		fmt.Println(data)
	}

}

// benchmark
// BenchmarkWindowCache-8   	   16701	     70829 ns/op
// cpu: Apple M3
func BenchmarkWindowCache(b *testing.B) {

	pullFunc := func(reqSize int) ([]int, error) {
		result := make([]int, reqSize)
		for i := 0; i < reqSize; i++ {
			randNum := rand.Intn(1000)
			result[i] = randNum
		}
		return result, nil
	}

	windowCache := NewWindowCache[int](&WindowCacheConfig[int]{
		TTL:             30 * time.Second,
		PullFunc:        pullFunc,
		DefaultPullSize: 10,
	})

	for i := 0; i < b.N; i++ {

		for i := 0; i < 1000; i++ {
			_, err := windowCache.Next(5)
			if err != nil {
				b.Error(err)
			}
			// fmt.Println(data)
		}

	}
}
