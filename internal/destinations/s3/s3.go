package runreveal

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	batch "github.com/runreveal/kawa/x/batcher"
)

type Option func(*s3)

func WithBucketName(bucketName string) Option {
	return func(s *s3) {
		s.bucketName = bucketName
	}
}

func WithPathPrefix(pathPrefix string) Option {
	return func(s *s3) {
		s.pathPrefix = pathPrefix
	}
}

func WithBucketRegion(bucketRegion string) Option {
	return func(s *s3) {
		s.bucketRegion = bucketRegion
	}
}

func WithCustomEndpoint(customEndpoint string) Option {
	return func(s *s3) {
		s.customEndpoint = customEndpoint
	}
}

func WithAccessKeyID(accessKeyID string) Option {
	return func(s *s3) {
		s.accessKeyID = accessKeyID
	}
}

func WithAccessSecretKey(accessSecretKey string) Option {
	return func(s *s3) {
		s.accessSecretKey = accessSecretKey
	}
}

type s3 struct {
	batcher *batch.Destination[types.Event]

	bucketName   string
	bucketRegion string
	pathPrefix   string

	customEndpoint  string
	accessKeyID     string
	accessSecretKey string
}

func New(opts ...Option) *s3 {
	ret := &s3{}
	for _, o := range opts {
		o(ret)
	}
	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(25),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (s *s3) Run(ctx context.Context) error {
	if s.bucketName == "" {
		return errors.New("missing bucket name")
	}

	return s.batcher.Run(ctx)
}

func (s *s3) Send(ctx context.Context, ack func(), msgs ...kawa.Message[types.Event]) error {
	return s.batcher.Send(ctx, ack, msgs...)
}

// Flush sends the given messages of type kawa.Message[type.Event] to an s3 bucket
func (s *s3) Flush(ctx context.Context, msgs []kawa.Message[types.Event]) error {

	// We need a handle a variety of arguments specifically the way
	// they are presented within the config file. If we don't some
	// s3 compatible services will not work correctly, like R2.
	var config = &aws.Config{}
	if s.customEndpoint != "" {
		config.Endpoint = aws.String(s.customEndpoint)
	}
	if s.accessKeyID != "" && s.accessSecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.accessKeyID, s.accessSecretKey, "")
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
	err = json.NewEncoder(gzipBuffer).Encode(msgs)
	if err != nil {
		return err
	}
	if err := gzipBuffer.Close(); err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%s/%d.json.gz",
		s.pathPrefix,
		time.Now().UTC().Format("2006/01/02/15"),
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
