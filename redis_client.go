package main

import (
	"math/big"
	"strconv"
	"time"

	redis "github.com/go-redis/redis/v7"
)

type redisClient interface {
	removeFromWorkingSet(blockNumber *big.Int) error
	getStaleWorkingBlocks() ([]*big.Int, error)
	getNextWorkingBlocks() ([]*big.Int, error)
}

type realRedisClient struct {
	redis                *redis.Client
	workingBlockStart    *big.Int
	workingTimeSetKey    string
	workingBlockSetKey   string
	lastFinishedBlockKey string
	maxConcurrency       int
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
	maxConcurrency int,
	ttlSeconds int,
) (redisClient, error) {
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
		maxConcurrency,
		ttlSeconds,
	}, err
}

func (client *realRedisClient) getStaleWorkingBlocks() ([]*big.Int, error) {
	var blocks []*big.Int
	err := client.redis.Watch(func(tx *redis.Tx) error {
		options := &redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(time.Now().Unix()-int64(client.ttlSeconds), 10),
		}
		vals := client.redis.ZRangeByScore(client.workingTimeSetKey, options)
		blockStrings, err := vals.Result()
		if err != nil {
			// No key (first run)
			if err == redis.Nil {
				return nil
			}

			return err
		}

		// Check if any working blocks are stale
		for i := 0; i < len(blockStrings); i++ {
			i, err := strconv.Atoi(blockStrings[i])
			if err != nil {
				return err
			}
			blocks = append(blocks, big.NewInt(int64(i)))
		}

		// Add as redis members
		members := []*redis.Z{}
		for i := 0; i < len(blocks); i++ {
			member := &redis.Z{
				Score:  float64(time.Now().Unix()),
				Member: blocks[i].Int64(),
			}
			members = append(members, member)
		}

		// Reset stale members TTL
		if len(members) > 0 {
			cmd := tx.ZAdd(client.workingTimeSetKey, members...)
			return cmd.Err()
		}

		return nil
	}, client.workingTimeSetKey)

	return blocks, err
}

func (client *realRedisClient) getNextWorkingBlocks() ([]*big.Int, error) {
	var blocks []*big.Int
	watchErr := client.redis.Watch(func(tx *redis.Tx) error {

		// Get cardinality of working set
		cmd := client.redis.ZCard(client.workingTimeSetKey)
		card, err := cmd.Result()
		if err != nil {
			if err == redis.Nil {
				card = 0
			} else {
				return err
			}
		}

		var nextBlock *big.Int = big.NewInt(0)

		// If nothing in the working set
		if card == 0 {

			// Check where we previously left off
			cmd := client.redis.Get(client.lastFinishedBlockKey)
			err := cmd.Err()

			if err != nil {
				// This is a first run
				if err == redis.Nil {
					nextBlock = client.workingBlockStart
				} else {
					return cmd.Err()
				}
			} else {
				// Start from where we left off
				lastFinishedInt, err := cmd.Int64()
				if err != nil {
					return err
				}
				lastFinishedBlock := big.NewInt(lastFinishedInt)
				nextBlock.Add(lastFinishedBlock, big.NewInt(1))
			}

			for i := 0; i < client.maxConcurrency; i++ {
				iBig := big.NewInt(int64(i))
				var next *big.Int = big.NewInt(0)
				next.Add(nextBlock, iBig)
				blocks = append(blocks, next)
			}
		} else {
			// If blocks in the working set, check to see how many more blocks we can
			// start work on (if any)
			options := &redis.ZRangeBy{
				Min:    "-inf",
				Max:    "inf",
				Count:  1,
				Offset: 0,
			}
			vals := client.redis.ZRevRangeByScore(client.workingBlockSetKey, options)
			results, err := vals.Result()
			if err != nil {
				return err
			}

			resultInt, err := strconv.Atoi(results[0])
			if err != nil {
				return err
			}

			latestBlock := big.NewInt(int64(resultInt))
			nextBlock.Add(latestBlock, big.NewInt(1))
			for i := 0; i < client.maxConcurrency-int(card); i++ {
				iBig := big.NewInt(int64(i))
				var next *big.Int = big.NewInt(0)
				next.Add(nextBlock, iBig)
				blocks = append(blocks, next)
			}
		}

		if len(blocks) == 0 {
			return nil
		}

		// Add as redis members
		members := []*redis.Z{}
		for i := 0; i < len(blocks); i++ {
			member := &redis.Z{
				Score:  float64(time.Now().Unix()),
				Member: blocks[i].Int64(),
			}

			members = append(members, member)
		}

		cmd = tx.ZAdd(client.workingTimeSetKey, members...)

		if cmd.Err() != nil {
			return cmd.Err()
		}

		// Add as redis members
		members = []*redis.Z{}
		for i := 0; i < len(blocks); i++ {
			member := &redis.Z{
				Score:  float64(blocks[i].Int64()),
				Member: blocks[i].Int64(),
			}

			members = append(members, member)
		}

		cmd = tx.ZAdd(client.workingBlockSetKey, members...)

		return cmd.Err()
	}, client.workingTimeSetKey)

	return blocks, watchErr
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

			if blockNumber.Cmp(lastFinishedBlock) < 0 {
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
