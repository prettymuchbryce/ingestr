package main

import (
	"context"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ethereum/go-ethereum/core/types"
)

type s3Client interface {
	getBlock(blockNumber *big.Int) (*types.Block, error)
	storeBlock(block *types.Block) error
}

type realS3Client struct {
	bucket  string
	s3      *s3.S3
	timeout *time.Duration
}

func (client *realS3Client) getBlock(blockNumber *big.Int) (*types.Block, error) {
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, client.timeout)
	defer cancelFn()
	input := &s3.GetObjectInput{
		Bucket: client.bucket,
		Key:    blockNumber.String(),
	}
	output, err := client.s3.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	return output.String(), nil
}

func (client *realS3Client) storeBlock(block *types.Block) error {
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, client.timeout)
	defer cancelFn()

	input := &s3.PutObjectInput{
		Bucket: clinet.bucket,
		Key:    block.Number().String(),
	}

	_, err := client.s3.PutObjectWithContext(ctx, input)
	return err
}
