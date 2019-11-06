package main

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/core/types"
	redis "github.com/go-redis/redis/v7"
)

func testGetBlock(block string) *types.Block {
	var rb *receiptsBlock
	json.Unmarshal([]byte(block), &rb)

	return types.NewBlock(rb.Header, nil, nil, nil)
}

func testGetRedisWorkingBlocks(redis *redis.Client) ([]int64, error) {

}

func testGetRedisLastFinishedBlock(redis *redis.Client) (int64, error) {

}
