package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/runreveal/kawa"
	batch "github.com/runreveal/kawa/x/batcher"
	"github.com/segmentio/ksuid"
)

type Option func(*S3)

func WithBucketName(bucketName string) Option {
	return func(s *S3) {
		s.bucketName = bucketName
	}
}

func WithPathPrefix(pathPrefix string) Option {
	return func(s *S3) {
		s.pathPrefix = pathPrefix
	}
}

func WithBucketRegion(bucketRegion string) Option {
	return func(s *S3) {
		s.bucketRegion = bucketRegion
	}
}

func WithCustomEndpoint(customEndpoint string) Option {
	return func(s *S3) {
		s.customEndpoint = customEndpoint
	}
}

func WithAccessKeyID(accessKeyID string) Option {
	return func(s *S3) {
		s.accessKeyID = accessKeyID
	}
}

func WithSecretAccessKey(secretAccessKey string) Option {
	return func(s *S3) {
		s.secretAccessKey = secretAccessKey
	}
}

func WithBatchSize(batchSize int) Option {
	return func(s *S3) {
		s.batchSize = batchSize
	}
}

type S3 struct {
	batcher *batch.Destination[[]byte]

	bucketName   string
	bucketRegion string
	pathPrefix   string

	customEndpoint  string
	accessKeyID     string
	secretAccessKey string

	batchSize int
}

func New(opts ...Option) *S3 {
	ret := &S3{}
	for _, o := range opts {
		o(ret)
	}
	if ret.batchSize == 0 {
		ret.batchSize = 100
	}
	ret.batcher = batch.NewDestination[[]byte](ret,
		batch.Raise[[]byte](),
		batch.FlushLength(ret.batchSize),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (s *S3) Run(ctx context.Context) error {
	if s.bucketName == "" {
		return errors.New("missing bucket name")
	}

	return s.batcher.Run(ctx)
}

func (s *S3) Send(ctx context.Context, ack func(), msgs ...kawa.Message[[]byte]) error {
	return s.batcher.Send(ctx, ack, msgs...)
}

// Flush sends the given messages of type kawa.Message[type.Event] to an s3 bucket
func (s *S3) Flush(ctx context.Context, msgs []kawa.Message[[]byte]) error {

	// We need a handle a variety of arguments specifically the way
	// they are presented within the config file. If we don't some
	// s3 compatible services will not work correctly, like R2.
	var config = &aws.Config{}
	if s.customEndpoint != "" {
		config.Endpoint = aws.String(s.customEndpoint)
	}
	if s.accessKeyID != "" && s.secretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.accessKeyID, s.secretAccessKey, "")
	}
	if s.bucketRegion != "" {
		config.Region = aws.String(s.bucketRegion)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(sess)

	var buf bytes.Buffer
	gzipBuffer := gzip.NewWriter(&buf)
	for _, msg := range msgs {
		_, err := gzipBuffer.Write(msg.Value)
		if err != nil {
			return err
		}
		_, err = gzipBuffer.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	if err := gzipBuffer.Close(); err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%s/%s_%d.gz",
		s.pathPrefix,
		time.Now().UTC().Format("2006/01/02/15"),
		ksuid.New().String(),
		time.Now().Unix(),
	)

	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}

	// Upload the file to S3
	_, err = uploader.Upload(uploadInput)
	if err != nil {
		return err
	}
	return nil
}
