package main

import (
	"math/big"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/prettymuchbryce/ingestr/mocks"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var testConf *config
var testClients *clients

func TestMain(m *testing.M) {
	godotenv.Load(".env.test")

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}

	testConf = loadEnvVariables()

	redisClient, err := createRealRedisClient(
		testConf.redisAddress,
		testConf.redisPassword,
		testConf.redisDB,
		testConf.workingBlockStart,
		testConf.redisWorkingTimeSetKey,
		testConf.redisWorkingBlockSetKey,
		testConf.redisLastFinishedBlockKey,
		testConf.maxConcurrency,
		testConf.workingBlockTTLSeconds,
	)

	testClients = &clients{
		eth:   &mocks.EthClient{},
		sns:   &mocks.SNSClient{},
		s3:    &mocks.S3Client{},
		redis: redisClient,
	}

	code := m.Run()
	os.Exit(code)
}

func TestFindWorkNoLatest(t *testing.T) {
	newWorkItems := findNextWork(testClients, testConf)
	assert.Equal(t, 0, newWorkItems)
}

func TestFindNextWorkLatest(t *testing.T) {
	latestBlock = big.NewInt(int64(8886217))

	newWorkItems := findNextWork(testClients, testConf)
	assert.Equal(t, 3, newWorkItems)
}
