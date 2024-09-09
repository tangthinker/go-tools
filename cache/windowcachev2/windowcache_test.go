package windowcachev2

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/tangthinker/go-tools/cache/windowcachev2/cacheclient"
	"log"
	"testing"
	"time"
)

var redisClient *redis.Client

func initRedis() {

	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("redis client init success")
}

func TestWindowCacheV2(t *testing.T) {

	initRedis()

	pullFunc := func(reqSize int) ([]string, error) {
		return []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, nil
	}

	cache := NewWindowCache(cacheclient.NewRedisCacheClient(redisClient), &Config{
		PullFunc: pullFunc,
		PullSize: 20,
		TTL:      30 * time.Second,
		Keys:     NewCacheKeys("test_key"),
	})

	for i := 0; i < 10; i++ {
		data, err := cache.Next(context.Background(), 3)
		if err != nil {
			t.Error(err)
		}
		t.Log(data)
	}

}
