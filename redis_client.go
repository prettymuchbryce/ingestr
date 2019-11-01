package main

import (
	"math/big"
	"strconv"

	redis "github.com/go-redis/redis/v7"
)

type redisClient interface {
	getNextBlockNumber(config *config) (*big.Int, error)
	updateBlockNumber(config *config, blockNumber *big.Int) error
}

type realRedisClient struct {
	redis *redis.Client
}

func createRealRedisClient(address string, password string, db int) (*realRedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	_, err := client.Ping().Result()

	return &realRedisClient{client}, err
}

func (client *realRedisClient) getNextBlockNumber(config *config) (*big.Int, error) {
	cmd := client.redis.Get(config.redisLatestBlockKey)
	result, err := cmd.Result()

	if err == redis.Nil {
		return config.latestBlockDefault, nil
	} else if err != nil {
		return big.NewInt(0), err
	}

	blockNumber, _ := strconv.Atoi(result)

	return big.NewInt(int64(blockNumber)), nil
}

func (client *realRedisClient) updateBlockNumber(config *config, blockNumber *big.Int) (*big.Int, error) {
	cmd := client.redis.Get(config.redisLatestBlockKey)
	result, err := cmd.Result()

	if err == redis.Nil {
		return config.latestBlockDefault, nil
	} else if err != nil {
		return big.NewInt(0), err
	}

	blockNumber, _ = strconv.Atoi(result)

	return big.NewInt(int64(blockNumber)), nil
}
