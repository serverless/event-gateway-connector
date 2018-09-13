package awskinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/serverless/event-gateway-connector/connection"

	validator "gopkg.in/go-playground/validator.v9"
)

// AWSKinesis is a configuration used to configure AWS Kinesis stream as a source.
type AWSKinesis struct {
	Service            kinesisiface.KinesisAPI `json:"-" validate:"-"`
	StreamName         string                  `json:"streamName" validate:"required"`
	Region             string                  `json:"region" validate:"required"`
	NumShards          uint                    `json:"numShards"`
	AWSAccessKeyID     string                  `json:"awsAccessKeyId,omitempty"`
	AWSSecretAccessKey string                  `json:"awsSecretAccessKey,omitempty"`
	AWSSessionToken    string                  `json:"awsSessionToken,omitempty"`
}

func init() {
	connection.RegisterSource(connection.SourceType("awskinesis"), SourceLoader{})
}

// SourceLoader satisfies the connection SourceLoader interface
type SourceLoader struct{}

// Load will decode the provided JSON data into valid AWSKinesis format, or error out
func (s SourceLoader) Load(data []byte) (connection.Source, error) {
	var src AWSKinesis
	err := json.Unmarshal(data, &src)
	if err != nil {
		return nil, fmt.Errorf("unable to load awskinesis source config: %s", err.Error())
	}

	if err := src.validate(); err != nil {
		return nil, fmt.Errorf("missing required fields for awskinesis source: %s", err.Error())
	}

	conf := aws.NewConfig().WithRegion(src.Region)
	if src.AWSAccessKeyID != "" && src.AWSSecretAccessKey != "" {
		conf = conf.WithCredentials(
			credentials.NewStaticCredentials(
				src.AWSAccessKeyID,
				src.AWSSecretAccessKey,
				src.AWSSessionToken,
			),
		)
	}

	// set the default value of shards in case this is not provided
	if src.NumShards == 0 {
		src.NumShards = 1
	}

	awsSession, err := session.NewSession(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create awskinesis service session: %s", err.Error())
	}

	src.Service = kinesis.New(awsSession)
	return src, nil
}

func (a AWSKinesis) validate() error {
	return validator.New().Struct(a)
}

// Fetch retrieves the next document from the awskinesis source
func (a AWSKinesis) Fetch() ([]byte, error) {
	// getrecord here
	return nil, nil
}

// NumWorkers returns the number of workers required for this connection
func (a AWSKinesis) NumWorkers() uint {
	return a.NumShards
}
