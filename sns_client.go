package main

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	sns "github.com/aws/aws-sdk-go/service/sns"
)

type snsClient interface {
	publish(data string) error
}

type realSnsClient struct {
	topic   string
	sns     *sns.SNS
	timeout time.Duration
}

func createRealSnsClient(topic string, timeout time.Duration) snsClient {
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
	if timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, timeout)
	}

	return &realSnsClient{
		sns:     svc,
		timeout: timeout,
		topic:   topic,
	}
}

func (client *realSnsClient) publish(data string) error {
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, client.timeout)
	defer cancelFn()

	input := &sns.PublishInput{
		Message:  &data,
		TopicArn: &client.topic,
	}

	_, err := client.sns.PublishWithContext(ctx, input)
	if err != nil {
		return err
	}

	return nil
}
