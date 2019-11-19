package main

import (
	"context"
	"math/big"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ethereum/go-ethereum/core/types"
	redis "github.com/go-redis/redis/v7"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

var latestBlock *big.Int
var workCompleteChan chan bool = make(chan bool)

type clients struct {
	eth   ethClient
	redis redisClient
	s3    s3Client
	sns   snsClient
}

type config struct {
	ethNodeHost               string
	ethNodePort               string
	httpReqTimeoutMS          int
	maxConcurrency            int
	minConfirmations          int
	newBlockTimeoutMS         int
	redisAddress              string
	redisDB                   int
	redisLastFinishedBlockKey string
	redisPassword             string
	redisWorkingBlockSetKey   string
	redisWorkingTimeSetKey    string
	s3BucketURI               string
	s3TimeoutMS               int
	snsEnabled                bool
	snsTimeoutMS              int
	snsTopic                  string
	workingBlockStart         *big.Int
	workingBlockTTLSeconds    int
}

func loadEnvVariables() *config {
	httpReqTimeoutMS, _ := strconv.Atoi(os.Getenv("HTTP_TIMEOUT_MS"))
	maxConcurrency, _ := strconv.Atoi(os.Getenv("MAX_CONCURRENCY"))
	minConfirmations, _ := strconv.Atoi(os.Getenv("MIN_CONFIRMATIONS"))
	newBlockTimeoutMS, _ := strconv.Atoi(os.Getenv("NEW_BLOCK_TIMEOUT_MS"))
	redisDB, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
	s3TimeoutMS, _ := strconv.Atoi(os.Getenv("S3_TIMEOUT_MS"))
	snsEnabled, _ := strconv.ParseBool(os.Getenv("SNS_ENABLED"))
	snsTimeoutMS, _ := strconv.Atoi(os.Getenv("SNS_TIMEOUT_MS"))
	workingBlockStart, _ := strconv.Atoi(os.Getenv("WORKING_BLOCK_START"))
	workingBlockTTLSeconds, _ := strconv.Atoi(os.Getenv("WORKING_BLOCK_TTL_SECONDS"))

	return &config{
		ethNodeHost:               os.Getenv("ETH_NODE_HOST"),
		ethNodePort:               os.Getenv("ETH_NODE_PORT"),
		httpReqTimeoutMS:          httpReqTimeoutMS,
		maxConcurrency:            maxConcurrency,
		minConfirmations:          minConfirmations,
		newBlockTimeoutMS:         newBlockTimeoutMS,
		redisAddress:              os.Getenv("REDIS_ADDRESS"),
		redisDB:                   redisDB,
		redisLastFinishedBlockKey: os.Getenv("REDIS_LAST_FINISHED_BLOCK_KEY"),
		redisPassword:             os.Getenv("REDIS_PASSWORD"),
		redisWorkingBlockSetKey:   os.Getenv("REDIS_WORKING_BLOCK_SET_KEY"),
		redisWorkingTimeSetKey:    os.Getenv("REDIS_WORKING_TIME_SET_KEY"),
		s3BucketURI:               os.Getenv("S3_BUCKET_URI"),
		s3TimeoutMS:               s3TimeoutMS,
		snsEnabled:                snsEnabled,
		snsTimeoutMS:              snsTimeoutMS,
		snsTopic:                  os.Getenv("SNS_TOPIC"),
		workingBlockStart:         big.NewInt(int64(workingBlockStart)),
		workingBlockTTLSeconds:    workingBlockTTLSeconds,
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
		return
	}

	conf := loadEnvVariables()

	initLogger()

	log.Info("Starting up")
	log.Info("Establishing connection to Ethereum node")

	ethClient, err := createRealEthClient(conf.ethNodeHost, conf.ethNodePort)
	if err != nil {
		log.Error("Failed to connect to ETH node")
		log.Fatal(err)
		return
	}

	log.Info("Connecting to redis")

	redisClient, err := createRealRedisClient(
		conf.redisAddress,
		conf.redisPassword,
		conf.redisDB,
		conf.workingBlockStart,
		conf.redisWorkingTimeSetKey,
		conf.redisWorkingBlockSetKey,
		conf.redisLastFinishedBlockKey,
		conf.workingBlockTTLSeconds,
	)
	if err != nil {
		log.Error("Failed to connect to redis")
		log.Fatal(err)
		return
	}

	log.Info("Creating SNS client")
	snsClient := createRealSnsClient(conf.snsTopic, msToDuration(conf.snsTimeoutMS))

	log.Info("Creating S3 client")
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
				for i := 0; i < config.maxConcurrency; i++ {
					findNextWork(clients, config)
				}

				for {
					<-workCompleteChan
					findNextWork(clients, config)
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

func findNextWork(clients *clients, config *config) int {
	var newWorkItems int = 0
	nextBlock, err := clients.redis.getStaleWorkingBlock()
	if err != nil {
		if err != redis.TxFailedErr {
			log.Error(err)
			return 0
		}
	}

	if nextBlock == nil {
		nextBlock, err = clients.redis.getNextWorkingBlock()
		if err != nil {
			if err != redis.TxFailedErr {
				log.Error(err)
				return 0
			}
		}
	}

	nextAllowedBlock := latestBlock.Sub(
		latestBlock,
		big.NewInt(int64(config.minConfirmations)),
	)

	if nextBlock.Cmp(nextAllowedBlock) <= 0 {
		go processBlock(nextBlock, config, clients)
		newWorkItems++
	}

	return newWorkItems
}

func processBlock(
	blockNumber *big.Int,
	config *config,
	clients *clients,
) error {
	log.Infof("Processing block: %s", blockNumber.String())

	defer func() { workCompleteChan <- true }()

	var hitFromCache = false
	var receiptBlockString string
	receiptBlockString, err := clients.s3.GetBlock(blockNumber)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				ctx := context.Background()
				ctx, cancelFn := context.WithTimeout(ctx, msToDuration(config.httpReqTimeoutMS))
				block, err := clients.eth.BlockByNumber(ctx, blockNumber)
				if err != nil {
					cancelFn()
					log.Errorf("Failed to get block from ETH node: %s", blockNumber.String())
					return err
				}
				cancelFn()
				transactions := block.Transactions()
				var receipts []*types.Receipt = make([]*types.Receipt, 0)
				for i := 0; i < len(transactions); i++ {
					t := transactions[i]
					ctx := context.Background()
					ctx, cancelFn := context.WithTimeout(ctx, msToDuration(config.httpReqTimeoutMS))
					receipt, err := clients.eth.TransactionReceipt(ctx, t.Hash())
					if err != nil {
						cancelFn()
						log.Error(err)
						return err
					}
					receipts = append(receipts, receipt)
					cancelFn()
				}

				receiptsBlock := &receiptsBlock{
					Header:       block.Header(),
					Receipts:     receipts,
					Hash:         block.Hash(),
					Transactions: block.Transactions(),
				}

				receiptBlockString, err = marshalReceiptBlock(receiptsBlock)
				if err != nil {
					log.Error(err)
					return err
				}
			} else {
				log.Error(err)
				return err
			}
		} else {
			log.Error(err)
			return err
		}
	}

	if err == nil {
		log.Infof("s3 Cache hit for block: %s", blockNumber.String())
		hitFromCache = true
	}

	err = clients.sns.Publish(blockNumber.String())
	if err != nil {
		log.Errorf("Failed to publish new block number to SNS: %s", blockNumber.String())
		log.Error(err)
		return err
	}

	if !hitFromCache {
		err = clients.s3.StoreBlock(blockNumber, receiptBlockString)
		if err != nil {
			log.Errorf("Failed to store block in S3: %s", blockNumber.String())
			log.Error(err)
			return err
		}
	}

	for retries := 10; retries > 0; retries-- {
		err := clients.redis.removeFromWorkingSet(blockNumber)
		if err == nil {
			break
		}
		if err != redis.TxFailedErr {
			log.Error(err)
			return err
		}
		if retries == 0 {
			log.Error("Failed to process block due to redis lock. Will retry after TTL")
			log.Error(err)
			return err
		}
	}

	log.Infof("Successfully processed block: %s", blockNumber.String())

	return nil
}
