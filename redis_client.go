package main

import (
	"math/big"
	"strconv"

	redis "github.com/go-redis/redis/v7"
)

type redisClient interface {
	getNextBlockNumber() (*big.Int, error)
	updateBlockNumber(blockNumber *big.Int) error
}

type realRedisClient struct {
	redis              *redis.Client
	latestBlockKey     string
	latestBlockDefault *big.Int
}

func createRealRedisClient(
	address string,
	password string,
	db int,
	latestBlockKey string,
	latestBlockDefault *big.Int,
) (redisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	_, err := client.Ping().Result()

	return &realRedisClient{
		redis:              client,
		latestBlockKey:     latestBlockKey,
		latestBlockDefault: latestBlockDefault,
	}, err
}

func (client *realRedisClient) getNextBlockNumber() (*big.Int, error) {
	cmd := client.redis.Get(client.latestBlockKey)
	result, err := cmd.Result()

	if err == redis.Nil {
		blockNumber, _ := strconv.Atoi(result)
		return big.NewInt(int64(blockNumber)), nil
	} else if err != nil {
		return big.NewInt(0), err
	}

	blockNumber, _ := strconv.Atoi(result)

	return big.NewInt(int64(blockNumber)), nil
}

func (client *realRedisClient) updateBlockNumber(blockNumber *big.Int) error {
	err := client.redis.Watch(func(tx *redis.Tx) error {
		status := tx.Set(client.latestBlockKey, blockNumber.String(), 0)
		return status.Err()
	}, client.latestBlockKey)
	return err
}
