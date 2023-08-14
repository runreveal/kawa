package sqs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	batch "github.com/runreveal/kawa/x/batcher"
	"github.com/segmentio/ksuid"
)

type Option func(*sqs)

func WithQueueURL(queueURL string) Option {
	return func(s *sqs) {
		s.queueURL = queueURL
	}
}

func WithRegion(region string) Option {
	return func(s *sqs) {
		s.region = region
	}
}

func WithAccessKeyID(accessKeyID string) Option {
	return func(s *sqs) {
		s.accessKeyID = accessKeyID
	}
}

func WithAccessSecretKey(accessSecretKey string) Option {
	return func(s *sqs) {
		s.accessSecretKey = accessSecretKey
	}
}

func WithBatchSize(batchSize int) Option {
	return func(s *sqs) {
		s.batchSize = batchSize
	}
}

type sqs struct {
	batcher *batch.Destination[types.Event]

	queueURL string
	region   string

	accessKeyID     string
	accessSecretKey string

	batchSize int
}

func New(opts ...Option) *sqs {
	ret := &sqs{}
	for _, o := range opts {
		o(ret)
	}
	if ret.batchSize == 0 {
		ret.batchSize = 100
	}
	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(ret.batchSize),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (s *sqs) Run(ctx context.Context) error {
	if s.queueURL == "" {
		return errors.New("missing queue url")
	}

	return s.batcher.Run(ctx)
}

func (s *sqs) Send(ctx context.Context, ack func(), msgs ...kawa.Message[types.Event]) error {
	return s.batcher.Send(ctx, ack, msgs...)
}

// Flush sends the given messages of type kawa.Message[type.Event] to an sqs queue
func (s *sqs) Flush(ctx context.Context, msgs []kawa.Message[types.Event]) error {

	var config = &aws.Config{}
	if s.accessKeyID != "" && s.accessSecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.accessKeyID, s.accessSecretKey, "")
	}
	if s.region != "" {
		config.Region = aws.String(s.region)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return err
	}
	sqsClient := awssqs.New(sess)

	var entries = []*awssqs.SendMessageBatchRequestEntry{}
	for _, msg := range msgs {
		messageRequest := &awssqs.SendMessageBatchRequestEntry{
			Id:          aws.String(ksuid.New().String()),
			MessageBody: aws.String(string(msg.Value.RawLog)),
		}
		if strings.HasSuffix(s.queueURL, ".fifo") {
			messageRequest.MessageGroupId = messageRequest.Id
			messageRequest.MessageDeduplicationId = messageRequest.Id
		}
		entries = append(entries, messageRequest)
	}

	sendMessageInput := &awssqs.SendMessageBatchInput{
		QueueUrl: aws.String(s.queueURL),
		Entries:  entries,
	}

	// Upload the file to S3
	_, err = sqsClient.SendMessageBatch(sendMessageInput)
	if err != nil {
		return err
	}
	return nil
}
