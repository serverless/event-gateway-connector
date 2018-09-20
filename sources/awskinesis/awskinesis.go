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
	Shards             []*kinesis.Shard        `json:"-"`
	AWSAccessKeyID     string                  `json:"awsAccessKeyId,omitempty"`
	AWSSecretAccessKey string                  `json:"awsSecretAccessKey,omitempty"`
	AWSSessionToken    string                  `json:"awsSessionToken,omitempty"`
}

func init() {
	connection.RegisterSource(connection.SourceType("awskinesis"), SourceLoader{})
}

// SourceLoader satisfies the connection SourceLoader interface
type SourceLoader struct{}

// Load will decode the provided JSON data into valid AWSKinesis format and establish
// a connection to the endpoint. Provided the connection is successful, we return an instance
// of the connection.Source, othewise an error.
func (s SourceLoader) Load(id connection.ID, data []byte) (connection.Source, error) {
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

	awsSession, err := session.NewSession(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create awskinesis service session: %s", err.Error())
	}

	src.Service = kinesis.New(awsSession)

	stream, err := src.Service.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(src.StreamName),
		},
	)
	if err != nil {
		return src, err
	}
	src.Shards = stream.StreamDescription.Shards

	return src, nil
}

func (a AWSKinesis) validate() error {
	return validator.New().Struct(a)
}

// Fetch retrieves the next document from the awskinesis source
func (a AWSKinesis) Fetch(shardID uint) ([][]byte, error) {
	ret := [][]byte{}

	// set up the shard iterator for our particular shardID
	// NOTE: may want to make the ShardIteratorType into a config value
	//       https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#GetShardIteratorInput
	iter, err := a.Service.GetShardIterator(
		&kinesis.GetShardIteratorInput{
			ShardId:           a.Shards[shardID].ShardId,
			ShardIteratorType: aws.String("LATEST"),
			StreamName:        aws.String(a.StreamName),
		},
	)
	if err != nil {
		return ret, err
	}

	records, err := a.Service.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iter.ShardIterator,
	})
	if err != nil {
		return ret, err
	}

	for _, rec := range records.Records {
		ret = append(ret, rec.Data)
	}

	return ret, nil
}

// NumberOfWorkers returns number of shards to handle by the pool
func (a AWSKinesis) NumberOfWorkers() uint {
	return uint(len(a.Shards))
}
