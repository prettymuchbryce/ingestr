package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

var latestBlock *big.Int

var zero = big.NewInt(int64(0))

type clients struct {
	eth   ethClient
	redis redisClient
	s3    s3Client
	sns   snsClient
}

type config struct {
	ethNodeHost         string
	ethNodePort         string
	latestBlockDefault  *big.Int
	redisAddress        string
	redisDB             int
	redisLatestBlockKey string
	redisPassword       string
	s3BucketURI         string
	s3Enabled           bool
	s3TimeoutMS         int
	snsEnabled          bool
	snsTopic            string
	snsTimeoutMS        int
	minConfirmations    int
	newBlockTimeoutMS   int
	httpReqTimeoutMS    int
}

func loadEnvVariables() *config {
	latestBlockDefault, _ := strconv.Atoi(os.Getenv("LATEST_BLOCK_DEFAULT"))
	minConfirmations, _ := strconv.Atoi(os.Getenv("MIN_CONFIRMATIONS"))
	redisDB, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
	s3Enabled, _ := strconv.ParseBool(os.Getenv("S3_ENABLED"))
	s3TimeoutMS, _ := strconv.Atoi(os.Getenv("S3_TIMEOUT_MS"))
	snsEnabled, _ := strconv.ParseBool(os.Getenv("SNS_ENABLED"))
	snsTimeoutMS, _ := strconv.Atoi(os.Getenv("SNS_TIMEOUT_MS"))
	newBlockTimeoutMS, _ := strconv.Atoi(os.Getenv("NEW_BLOCK_TIMEOUT_MS"))
	httpReqTimeoutMS, _ := strconv.Atoi(os.Getenv("HTTP_TIMEOUT_MS"))

	return &config{
		ethNodeHost:         os.Getenv("ETH_NODE_HOST"),
		ethNodePort:         os.Getenv("ETH_NODE_PORT"),
		latestBlockDefault:  big.NewInt(int64(latestBlockDefault)),
		minConfirmations:    minConfirmations,
		redisAddress:        os.Getenv("REDIS_ADDRESS"),
		redisDB:             redisDB,
		redisLatestBlockKey: os.Getenv("REDIS_LATEST_BLOCK_KEY"),
		redisPassword:       os.Getenv("REDIS_PASSWORD"),
		s3BucketURI:         os.Getenv("S3_BUCKET_URI"),
		s3Enabled:           s3Enabled,
		s3TimeoutMS:         s3TimeoutMS,
		snsEnabled:          snsEnabled,
		snsTimeoutMS:        snsTimeoutMS,
		snsTopic:            os.Getenv("SNS_TOPIC"),
		newBlockTimeoutMS:   newBlockTimeoutMS,
		httpReqTimeoutMS:    httpReqTimeoutMS,
	}
}

func main() {
	env := os.Getenv("INGESTR_ENV")

	if env == "" {
		godotenv.Load(".env.development")
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	conf := loadEnvVariables()

	initLogger()

	log.Info("Starting up")
	log.Info("Creating Clients")

	ethClient, err := createRealEthClient(conf.ethNodeHost, conf.ethNodePort)
	if err != nil {
		log.Fatal("Failed to connect to ETH node")
	}

	redisClient, err := createRealRedisClient(
		conf.redisAddress,
		conf.redisPassword,
		conf.redisDB,
		conf.redisLatestBlockKey,
		conf.latestBlockDefault,
	)
	if err != nil {
		log.Fatal("Failed to connect to redis")
	}

	snsClient := createRealSnsClient(conf.snsTopic, msToDuration(conf.snsTimeoutMS))

	s3Client := createRealS3Client(conf.s3BucketURI, msToDuration(conf.s3TimeoutMS))

	clients := &clients{
		eth:   ethClient,
		redis: redisClient,
		sns:   snsClient,
		s3:    s3Client,
	}

	start(clients, conf)
}

func start(clients *clients, config *config) {
	go func() {
		for {
			if latestBlock != nil {
				nextBlock, err := clients.redis.getNextBlockNumber()
				if err != nil {
					log.Error("Failed to get next block in redis")
				} else {
					if nextBlock.Cmp(zero) == 0 {
						nextBlock = config.latestBlockDefault
					}

					nextAllowedBlock := nextBlock.Add(
						nextBlock,
						big.NewInt(int64(config.minConfirmations)),
					)

					// if latestBlock >= nextBlock + minConfirmations
					if latestBlock.Cmp(nextAllowedBlock) >= 0 {
						processBlock(nextBlock, config, clients)
					}
				}
			}
		}
	}()

	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, msToDuration(config.newBlockTimeoutMS))
	defer cancelFn()

	log.Info("Subscribing to new blocks")
	ethChan := make(chan *types.Header)
	_, err := clients.eth.SubscribeNewHead(ctx, ethChan)
	if err != nil {
		log.Fatal("Failed to subscribe to latest block")
	}

	for {
		header := <-ethChan
		latestBlock = header.Number

		log.Infof("Found new block: %s", latestBlock)
	}
}

func processBlock(
	blockNumber *big.Int,
	config *config,
	clients *clients,
) error {
	log.Infof("Processing block: %s", blockNumber.String())

	block, err := clients.s3.getBlock(blockNumber)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				ctx := context.Background()
				ctx, cancelFn := context.WithTimeout(ctx, msToDuration(config.httpReqTimeoutMS))
				defer cancelFn()
				var err2 error
				block, err2 = clients.eth.BlockByNumber(ctx, blockNumber)
				if err2 != nil {
					log.Errorf("Failed to get block from ETH node: %s", block.Number().String())
				}
			}
		} else {
			log.Errorf("Failed to reach S3 to get block: %s", blockNumber.String())
		}
	}

	fmt.Println("wat is block", block)

	err = clients.sns.broadcast(block)
	if err != nil {
		log.Errorf("Failed to publish block to SNS: %s", block.Number().String())
	}

	err = clients.s3.storeBlock(block)
	if err != nil {
		log.Errorf("Failed to store block in S3: %s", block.Number().String())
	}

	if err == nil {
		err = clients.redis.updateBlockNumber(block.Number())
		if err != nil {
			log.Errorf("Failed to update latest block in redis: %s", block.Number().String())
		}
	}

	log.Infof("Successfully processed block: %s", block.Number().String())

	return nil
}
