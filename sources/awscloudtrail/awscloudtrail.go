package awscloudtrail

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudtrail"
	"github.com/aws/aws-sdk-go/service/cloudtrail/cloudtrailiface"
	"github.com/serverless/event-gateway-connector/connection"

	validator "gopkg.in/go-playground/validator.v9"
)

// AWSCloudTrail is a configuration used to configure AWS CloudTrail trail as a source.
type AWSCloudTrail struct {
	Service            cloudtrailiface.CloudTrailAPI `json:"-" validate:"-"`
	Region             string                        `json:"region" validate:"required"`
	AWSAccessKeyID     string                        `json:"awsAccessKeyId,omitempty"`
	AWSSecretAccessKey string                        `json:"awsSecretAccessKey,omitempty"`
	AWSSessionToken    string                        `json:"awsSessionToken,omitempty"`
}

func init() {
	connection.RegisterSource(connection.SourceType("awscloudtrail"), SourceLoader{})
}

// SourceLoader satisfies the connection SourceLoader interface
type SourceLoader struct{}

// Load will decode the provided JSON data into valid AWSCloudTrail format and establish
// a connection to the endpoint. Provided the connection is successful, we return an instance
// of the connection.Source, othewise an error.
func (s SourceLoader) Load(data []byte) (connection.Source, error) {
	var src AWSCloudTrail
	err := json.Unmarshal(data, &src)
	if err != nil {
		return nil, fmt.Errorf("unable to load awscloudtrail source config: %s", err.Error())
	}

	if err := src.validate(); err != nil {
		return nil, fmt.Errorf("missing required fields for awscloudtrail source: %s", err.Error())
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
		return nil, fmt.Errorf("unable to create awscloudtrail service session: %s", err.Error())
	}

	src.Service = cloudtrail.New(awsSession)

	return src, nil
}

func (a AWSCloudTrail) validate() error {
	return validator.New().Struct(a)
}

// Fetch retrieves the next event from the awscloudtrail source
func (a AWSCloudTrail) Fetch(shardID uint, lastSeq string) (*connection.Records, error) {
    var start time.Time
	ret := &connection.Records{}
	maxResults := int64(50)
	params := &cloudtrail.LookupEventsInput{
		MaxResults: &maxResults,
	}

	if len(lastSeq) != 0 {
	    start, err := time.Parse(time.RFC3339, lastSeq)
	    if err != nil {
	        return nil, err
	    }
	    params.StartTime = &start
	} else {
	    // If we don't have a start time, we'll go with a day ago.
	    start := time.Now().AddDate(0, 0, -1)
	    params.StartTime = &start
	}

	resp, err := a.Service.LookupEvents(params)
	if err != nil {
		return nil, err
	}


	for _, event := range resp.Events {
		ret.Data = append(ret.Data, []byte(*event.CloudTrailEvent))
		if event.EventTime.After(start) {
		    start = event.EventTime.Add(1 * time.Second)
		    ret.LastSequence = start.Format(time.RFC3339)
		}
	}

	return ret, nil
}

// NumberOfWorkers always returns 1 as CloudTrail is heavily rate-limited.
func (a AWSCloudTrail) NumberOfWorkers() uint {
	return 1
}
