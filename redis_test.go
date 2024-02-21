package main

import (
	"context"
	"fmt"
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

// func TestSets(t *testing.T){

// }
