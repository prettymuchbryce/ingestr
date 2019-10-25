package main

import (
	"context"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	sns "github.com/aws/aws-sdk-go/service/sns"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	redis "github.com/go-redis/redis/v7"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

var latestBlock *big.Int

var zero = big.NewInt(int64(0))

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
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	conf := loadEnvVariables()

	initLogger()

	ethClient := connect(conf.ethNodeHost, conf.ethNodePort)

	go func() {
		redisClient, err := createRedisClient(conf.redisAddress, conf.redisPassword, conf.redisDB)
		if err != nil {
			log.Fatal("Failed to connect to redis")
		}

		s3, cancelS3Context := createS3(msToDuration(conf.s3TimeoutMS))
		defer cancelS3Context()

		sns, cancelSNSContext := createSNS(msToDuration(conf.snsTimeoutMS))
		defer cancelSNSContext()

		for {
			if latestBlock != nil {
				nextBlock, err := getNextBlockNumber(redisClient, conf)
				if err != nil {
					log.Error("Failed to get next block in redis")
				} else {
					if nextBlock == zero {
						nextBlock = conf.latestBlockDefault
					}

					if latestBlock.Cmp(nextBlock.Add(nextBlock, big.NewInt(int64(conf.minConfirmations)))) == -1 {
						ctx := context.Background()
						ctx, cancelFn := context.WithTimeout(ctx, msToDuration(conf.httpReqTimeoutMS))
						block, err := ethClient.BlockByNumber(ctx, nextBlock)
						if err != nil {
							log.Error("Failed to get next block from Ethereum client")
						} else {
							processBlock(block, conf, redisClient, s3, sns)
						}
						if cancelFn != nil {
							cancelFn()
						}
					}
				}
			}
		}
	}()

	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, msToDuration(conf.newBlockTimeoutMS))
	defer cancelFn()

	ethChan := make(chan *types.Header)
	_, err = ethClient.SubscribeNewHead(ctx, ethChan)
	if err != nil {
		log.Fatal("Failed to subscribe to latest block")
	}

	header := <-ethChan
	latestBlock = header.Number
}

func getNextBlockNumber(client *redis.Client, config *config) (*big.Int, error) {
	cmd := client.Get(config.redisLatestBlockKey)
	result, err := cmd.Result()
	if err != nil {
		return big.NewInt(0), err
	}

	if cmd.String() == "" {
		return config.latestBlockDefault, nil
	}

	blockNumber, _ := strconv.Atoi(result)

	return big.NewInt(int64(blockNumber)), nil
}

func processBlock(
	block *types.Block,
	config *config,
	client *redis.Client,
	s3 *s3.S3,
	sns *sns.SNS,
) {
	// Pull the block
	// Store in S3
	// Publish to SNS
}

func connect(host string, port string) *ethclient.Client {
	client, err := ethclient.Dial(host + ":" + port)
	if err != nil {
		log.Error(err)
	}

	return client
}

func createS3(timeout time.Duration) (*s3.S3, func()) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	sess := session.Must(session.NewSession())

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	return svc, cancelFn
}

func createSNS(timeout time.Duration) (*sns.SNS, func()) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	sess := session.Must(session.NewSession())

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc := sns.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	return svc, cancelFn
}

func createRedisClient(address string, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	_, err := client.Ping().Result()

	return client, err
}
