package main

import (
	"math/big"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
	"github.com/prettymuchbryce/ingestr/mocks"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var testConf *config
var testClients *clients

var ethMock *mocks.EthClient
var s3Mock *mocks.S3Client
var snsMock *mocks.SNSClient

var testBlock string = `{"header":{"parentHash":"0x5f3e1a662605fe4c1a7b26f7a84e66c6ffe7d56503e675e81e86a5aa33726b37","sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347","miner":"0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c","stateRoot":"0xe5613ce0eab670e3c92c81e84382c30174a9dd02dccd0ad55da0cee3f9516dcb","transactionsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","receiptsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","difficulty":"0x8b3dc6a8633f3","number":"0x868761","gasLimit":"0x97f1a3","gasUsed":"0x0","timestamp":"0x5db4786e","extraData":"0x5050594520737061726b706f6f6c2d6574682d636e2d687a32","mixHash":"0xfa1ef05a78048a92ca9b8eb03f0aeb719a6083fd54bbaaa2ca6ea18f0882c18a","nonce":"0x56f2b9180109f03a","hash":"0x5715f190c16d0b954ee2741b255cdb5930b738ef7edba01852df8a721c81409b"},"hash":"0x5715f190c16d0b954ee2741b255cdb5930b738ef7edba01852df8a721c81409b","transactions":[]}`

var testBlockReceipts string = `{"header":{"parentHash":"0x5f3e1a662605fe4c1a7b26f7a84e66c6ffe7d56503e675e81e86a5aa33726b37","sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347","miner":"0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c","stateRoot":"0xe5613ce0eab670e3c92c81e84382c30174a9dd02dccd0ad55da0cee3f9516dcb","transactionsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","receiptsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","difficulty":"0x8b3dc6a8633f3","number":"0x868761","gasLimit":"0x97f1a3","gasUsed":"0x0","timestamp":"0x5db4786e","extraData":"0x5050594520737061726b706f6f6c2d6574682d636e2d687a32","mixHash":"0xfa1ef05a78048a92ca9b8eb03f0aeb719a6083fd54bbaaa2ca6ea18f0882c18a","nonce":"0x56f2b9180109f03a","hash":"0x5715f190c16d0b954ee2741b255cdb5930b738ef7edba01852df8a721c81409b"},"receipts":[],"hash":"0x5715f190c16d0b954ee2741b255cdb5930b738ef7edba01852df8a721c81409b","transactions":[]}`

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

	ethMock = &mocks.EthClient{}
	snsMock = &mocks.SNSClient{}
	s3Mock = &mocks.S3Client{}

	testClients = &clients{
		eth:   ethMock,
		sns:   snsMock,
		s3:    s3Mock,
		redis: redisClient,
	}

	code := m.Run()
	os.Exit(code)
}

func TestFindWorkNoLatest(t *testing.T) {
	newWorkItems := findNextWork(testClients, testConf)
	assert.Equal(t, 0, newWorkItems)
}

/*
func TestFindNextWorkLatest(t *testing.T) {
	latestBlock = big.NewInt(int64(8886217))

	newWorkItems := findNextWork(testClients, testConf)
	assert.Equal(t, 3, newWorkItems)

	latestBlock = nil
}
*/

func TestProcessBlock(t *testing.T) {
	blockNumber := big.NewInt(int64(8886217))
	s3Mock.On("GetBlock", blockNumber).Return("", awserr.New(s3.ErrCodeNoSuchKey, "", nil))
	ethMock.On("BlockByNumber", mock.Anything, blockNumber).Return(testGetBlock(testBlock), nil)
	snsMock.On("Publish", mock.Anything).Return(nil)
	s3Mock.On("StoreBlock", blockNumber, mock.Anything).Return(nil)
	err := processBlock(blockNumber, testConf, testClients)
	assert.NoError(t, err)
}
