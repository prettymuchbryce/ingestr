package main

import (
	"math/big"
	"strconv"
	"time"

	redis "github.com/go-redis/redis/v7"
)

type redisClient interface {
	removeFromWorkingSet(blockNumber *big.Int) error
	getStaleWorkingBlock() (*big.Int, error)
	getNextWorkingBlock(nextAllowedBlock *big.Int) (*big.Int, error)
}

type realRedisClient struct {
	redis                *redis.Client
	workingBlockStart    *big.Int
	workingTimeSetKey    string
	workingBlockSetKey   string
	lastFinishedBlockKey string
	ttlSeconds           int
}

func createRealRedisClient(
	address string,
	password string,
	db int,
	workingBlockStart *big.Int,
	workingTimeSetKey string,
	workingBlockSetKey string,
	lastFinishedBlockKey string,
	ttlSeconds int,
) (*realRedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	_, err := client.Ping().Result()

	return &realRedisClient{
		client,
		workingBlockStart,
		workingTimeSetKey,
		workingBlockSetKey,
		lastFinishedBlockKey,
		ttlSeconds,
	}, err
}

func (client *realRedisClient) getStaleWorkingBlock() (*big.Int, error) {
	var block *big.Int
	err := client.redis.Watch(func(tx *redis.Tx) error {
		options := &redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(time.Now().Unix()-int64(client.ttlSeconds), 10),
		}
		vals := client.redis.ZRangeByScore(client.workingTimeSetKey, options)
		blockStrings, err := vals.Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}

			return err
		}

		if len(blockStrings) > 0 {
			i, err := strconv.Atoi(blockStrings[0])
			if err != nil {
				return err
			}
			block = big.NewInt(int64(i))
		} else {
		}

		// Add as redis members
		var member *redis.Z
		if block != nil {
			member = &redis.Z{
				Score:  float64(time.Now().Unix()),
				Member: block.Int64(),
			}
		}

		// Reset stale member TTL
		if member != nil {
			cmd := tx.ZAdd(client.workingTimeSetKey, member)
			return cmd.Err()
		}

		return nil
	}, client.workingTimeSetKey)

	return block, err
}

func (client *realRedisClient) getNextWorkingBlock(nextAllowedBlock *big.Int) (*big.Int, error) {
	var block *big.Int = big.NewInt(0)
	watchErr := client.redis.Watch(func(tx *redis.Tx) error {
		options := &redis.ZRangeBy{
			Min:    "-inf",
			Max:    "inf",
			Count:  1,
			Offset: 0,
		}

		vals := client.redis.ZRevRangeByScore(client.workingBlockSetKey, options)
		results, err := vals.Result()
		if err != nil {
			// This is a first run
			if err == redis.Nil {
				block = client.workingBlockStart
				return nil
			} else {
				return err
			}
		} else {
			if len(results) > 0 {
				resultInt, err := strconv.Atoi(results[0])
				if err != nil {
					return err
				}

				block = big.NewInt(int64(resultInt + 1))
			}
		}

		// Check where we previously left off
		getCmd := client.redis.Get(client.lastFinishedBlockKey)
		err = getCmd.Err()
		if err != nil {
			return err
		} else {
			// Start from where we left off
			lastFinishedInt, err := getCmd.Int64()
			if err != nil {
				return err
			}

			lastFinishedBlock := big.NewInt(lastFinishedInt)

			if lastFinishedBlock.Cmp(block) >= 0 {
				block.Add(lastFinishedBlock, big.NewInt(1))
			}
		}

		if block.Cmp(nextAllowedBlock) > 0 {
			return nil
		}

		// Add as redis member
		member := &redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: block.Int64(),
		}

		cmd := tx.ZAdd(client.workingTimeSetKey, member)

		if cmd.Err() != nil {
			return cmd.Err()
		}

		// Add as redis member
		member = &redis.Z{
			Score:  float64(block.Int64()),
			Member: block.Int64(),
		}

		cmd = tx.ZAdd(client.workingBlockSetKey, member)

		return cmd.Err()
	}, client.workingTimeSetKey)

	return block, watchErr
}

func (client *realRedisClient) removeFromWorkingSet(blockNumber *big.Int) error {
	watchErr := client.redis.Watch(func(tx *redis.Tx) error {
		cmd := client.redis.Get(client.lastFinishedBlockKey)
		err := cmd.Err()

		if err == nil {
			var lastFinishedBlock *big.Int
			lastFinishedInt, err := cmd.Int64()
			if err != nil {
				return err
			}

			lastFinishedBlock = big.NewInt(lastFinishedInt)

			if blockNumber.Cmp(lastFinishedBlock) == 1 {
				cmd := tx.Set(client.lastFinishedBlockKey, blockNumber.Int64(), 0)
				if cmd.Err() != nil {
					return cmd.Err()
				}
			}
		} else if err == redis.Nil {
			cmd := tx.Set(client.lastFinishedBlockKey, blockNumber.Int64(), 0)
			if cmd.Err() != nil {
				return cmd.Err()
			}
		} else {
			return err
		}

		cmdRem := tx.ZRem(client.workingBlockSetKey, blockNumber.Int64())
		if cmdRem.Err() != nil {
			return cmdRem.Err()
		}

		cmdRem = tx.ZRem(client.workingTimeSetKey, blockNumber.Int64())
		if cmdRem.Err() != nil {
			return cmdRem.Err()
		}

		return nil
	}, client.lastFinishedBlockKey)

	return watchErr
}
