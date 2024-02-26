package main

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var (
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	ctx = context.Background()
)

// Client

func TestConn(t *testing.T) {

	assert.NotNil(t, client)

	err := client.Close()
	assert.Nil(t, err)
}

func TestPing(t *testing.T) {

	resp, err := client.Ping(ctx).Result()

	fmt.Println(resp)

	assert.Nil(t, err)
	assert.Equal(t, "PONG", resp)

}

// String

func TestString(t *testing.T) {
	t.Run("Set Expired Success", func(t *testing.T) {
		client.SetEx(ctx, "name", "jalal", 3*time.Second)

		result, err := client.Get(ctx, "name").Result()
		assert.Nil(t, err)
		assert.Equal(t, "jalal", result)
	})
	t.Run("Set Expired Failed", func(t *testing.T) {
		client.SetEx(ctx, "name", "jalal", 3*time.Second)

		time.Sleep(5 * time.Second)
		result, err := client.Get(ctx, "name").Result()
		assert.NotNil(t, err)
		assert.Equal(t, "", result)
	})
}

// List
func TestList(t *testing.T) {
	t.Run("Push From Left to Right and Get from Left to Right", func(t *testing.T) {
		client.RPush(ctx, "name", "jalaluddin")
		client.RPush(ctx, "name", "muh")
		client.RPush(ctx, "name", "akbar")

		assert.Equal(t, "jalaluddin", client.LPop(ctx, "name").Val())
		assert.Equal(t, "muh", client.LPop(ctx, "name").Val())
		assert.Equal(t, "akbar", client.LPop(ctx, "name").Val())
	})
	t.Run("Push From Left to Right and Get from Right to Left", func(t *testing.T) {
		client.RPush(ctx, "name", "jalaluddin")
		client.RPush(ctx, "name", "muh")
		client.RPush(ctx, "name", "akbar")

		assert.Equal(t, "akbar", client.RPop(ctx, "name").Val())
		assert.Equal(t, "muh", client.RPop(ctx, "name").Val())
		assert.Equal(t, "jalaluddin", client.RPop(ctx, "name").Val())
	})
	t.Run("Push From Right to Left and Get from Left to Right", func(t *testing.T) {
		client.LPush(ctx, "name", "jalaluddin")
		client.LPush(ctx, "name", "muh")
		client.LPush(ctx, "name", "akbar")

		assert.Equal(t, "akbar", client.LPop(ctx, "name").Val())
		assert.Equal(t, "muh", client.LPop(ctx, "name").Val())
		assert.Equal(t, "jalaluddin", client.LPop(ctx, "name").Val())
	})
	t.Run("Push From Right to Left and Get from Right to Left", func(t *testing.T) {
		client.LPush(ctx, "name", "jalaluddin")
		client.LPush(ctx, "name", "muh")
		client.LPush(ctx, "name", "akbar")

		assert.Equal(t, "jalaluddin", client.RPop(ctx, "name").Val())
		assert.Equal(t, "muh", client.RPop(ctx, "name").Val())
		assert.Equal(t, "akbar", client.RPop(ctx, "name").Val())
	})
	t.Run("Delete String", func(t *testing.T) {
		client.LPush(ctx, "name", "jalaluddin")
		client.LPush(ctx, "name", "muh")
		client.LPush(ctx, "name", "akbar")

		client.Del(ctx, "name")

		assert.Equal(t, "", client.RPop(ctx, "name").Val())
		assert.Equal(t, "", client.RPop(ctx, "name").Val())
		assert.Equal(t, "", client.RPop(ctx, "name").Val())
	})
}

// Sets

func TestSets(t *testing.T) {
	client.SAdd(ctx, "user", "jalal")
	client.SAdd(ctx, "user", "jalal")
	client.SAdd(ctx, "user", "muh")
	client.SAdd(ctx, "user", "muh")
	client.SAdd(ctx, "user", "akbar")
	client.SAdd(ctx, "user", "akbar")

	assert.Equal(t, int64(3), client.SCard(ctx, "user").Val())
	assert.Equal(t, []string{"akbar", "muh", "jalal"}, client.SMembers(ctx, "user").Val()) // A-Z
}

// Sorted Set
func TestSortedSet(t *testing.T) {

	client.ZAdd(context.Background(), "scores", redis.Z{Score: 100, Member: "Eko"})
	client.ZAdd(context.Background(), "scores", redis.Z{Score: 85, Member: "Budi"})
	client.ZAdd(context.Background(), "scores", redis.Z{Score: 95, Member: "Joko"})

	assert.Equal(t, []string{"Budi", "Joko", "Eko"}, client.ZRange(context.Background(), "scores", 0, -1).Val())

	assert.Equal(t, "Eko", client.ZPopMax(context.Background(), "scores").Val()[0].Member)
	assert.Equal(t, "Joko", client.ZPopMax(context.Background(), "scores").Val()[0].Member)
	assert.Equal(t, "Budi", client.ZPopMax(context.Background(), "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user1", "id", "1")
	client.HSet(ctx, "user1", "name", "jalal")
	client.HSet(ctx, "user1", "email", "jalal@gmail.com")

	user := client.HGetAll(ctx, "user1").Val()

	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "jalal", user["name"])
	assert.Equal(t, "jalal@gmail.com", user["email"])

	client.Del(ctx, "user1")
}

// Geo Point
func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 118.72176877586517,
		Latitude:  -8.469458429309466,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 118.72559574160127,
		Latitude:  -8.453015099470573,
	})
	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()

	assert.Equal(t, 1.8768, distance)

	search := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  118.72538814963505,
		Latitude:   -8.458725925297939,
		Radius:     2,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, search)
}

// Hyper Log Log
func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "jalal", "muh", "akbar")
	client.PFAdd(ctx, "visitors", "jalal", "eko", "budi")
	client.PFAdd(ctx, "visitors", "eko", "budi", "joko")

	total := client.PFCount(ctx, "visitors").Val()

	assert.Equal(t, int64(6), total)
}

// Pipeline
func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.SetEx(ctx, "name", "Jalal", 5*time.Second)
		p.SetEx(ctx, "country", "Indonesia", 5*time.Second)
		return nil
	})

	assert.Nil(t, err)
	assert.Equal(t, "Jalal", client.Get(ctx, "name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "country").Val())
}

// Transaction
func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetEx(ctx, "name", "Jalal", 5*time.Second)
		p.SetEx(ctx, "country", "USA", 5*time.Second)
		return nil
	})

	assert.Nil(t, err)
	assert.Equal(t, "Jalal", client.Get(ctx, "name").Val())
	assert.Equal(t, "USA", client.Get(ctx, "country").Val())
}

// Stream
func TestStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Jalal",
				"country": "USA",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}
func TestConsumeStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", "<"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println("id: \n", message.ID)
			fmt.Println("values: ", message.Values)
		}
	}
}

// Pub Sub

func TestSuscribePubSub(t *testing.T) {
	suscriber := client.Subscribe(ctx, "channel-1")
	for i := 0; i < 10; i++ {
		message, err := suscriber.ReceiveMessage(ctx)
		fmt.Println(message.Payload)
		assert.Nil(t, err)
	}
	suscriber.Close()
}
func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}
