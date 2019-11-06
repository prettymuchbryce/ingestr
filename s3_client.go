package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io/ioutil"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Client interface {
	GetBlock(blockNumber *big.Int) (string, error)
	StoreBlock(blockNumber *big.Int, data string) error
}

type realS3Client struct {
	bucket  string
	s3      *s3.S3
	timeout time.Duration
}

func createRealS3Client(bucket string, timeout time.Duration) *realS3Client {
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
	if timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, timeout)
	}

	return &realS3Client{
		bucket:  bucket,
		s3:      svc,
		timeout: timeout,
	}
}

func (client *realS3Client) GetBlock(blockNumber *big.Int) (string, error) {
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, client.timeout)
	defer cancelFn()
	blockNumberString := blockNumber.String()

	input := &s3.GetObjectInput{
		Bucket: &client.bucket,
		Key:    &blockNumberString,
	}

	result, err := client.s3.GetObjectWithContext(ctx, input)
	if err != nil {
		return "", err
	}

	gzipReader, err := gzip.NewReader(result.Body)
	if err != nil {
		return "", err
	}

	defer gzipReader.Close()

	// var buffer bytes.Buffer
	data, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (client *realS3Client) StoreBlock(blockNumber *big.Int, data string) error {
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, client.timeout)
	defer cancelFn()

	blockNumberString := blockNumber.String()

	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	gzipWriter.Write([]byte(data))
	gzipWriter.Close()

	input := &s3.PutObjectInput{
		Bucket: &client.bucket,
		Key:    &blockNumberString,
		Body:   bytes.NewReader(buffer.Bytes()),
	}

	_, err := client.s3.PutObjectWithContext(ctx, input)
	return err
}
