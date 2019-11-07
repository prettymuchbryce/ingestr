package main

import (
	"encoding/json"

	"strconv"

	"github.com/ethereum/go-ethereum/core/types"
	redis "github.com/go-redis/redis/v7"
)

func testGetBlock(block string) *types.Block {
	var rb *receiptsBlock
	json.Unmarshal([]byte(block), &rb)

	return types.NewBlock(rb.Header, nil, nil, nil)
}

func testGetRedisWorkingBlocks(client *redis.Client, key string) (intResult []int64, err error) {
	options := &redis.ZRangeBy{
		Min: "-inf",
		Max: "inf",
	}

	vals := client.ZRangeByScore(key, options)
	result, err := vals.Result()

	if err != nil {
		return nil, err
	}

	for i := 0; i < len(result); i++ {
		ii, _ := strconv.Atoi(result[i])
		intResult = append(intResult, int64(ii))
	}

	return intResult, nil
}

func testGetRedisLastFinishedBlock(client *redis.Client, key string) (int64, error) {
	getCmd := client.Get(key)
	err := getCmd.Err()
	if err != nil {
		return 0, err
	}

	ii, _ := getCmd.Int64()

	return ii, nil
}

func testClearRedis(client *redis.Client) error {
	result := client.FlushDB()
	return result.Err()
}
