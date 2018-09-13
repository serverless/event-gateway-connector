package pool_test

import (
	"encoding/json"
	"testing"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/sources/awskinesis"
	"github.com/serverless/event-gateway-connector/sources/kafka"
	"github.com/stretchr/testify/assert"
)

func TestMarshalJSON(t *testing.T) {
	a := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source: &awskinesis.AWSKinesis{
			StreamName:         "stream_one",
			Region:             "us-east-1",
			AWSAccessKeyID:     "key_one",
			AWSSecretAccessKey: "secret_key_one",
			AWSSessionToken:    "session_token_one",
		},
	}

	data, err := json.Marshal(a)
	assert.NotNil(t, data)
	assert.Nil(t, err)
}

func TestUnmarshalJSON(t *testing.T) {
	a := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source: &awskinesis.AWSKinesis{
			StreamName:         "stream_one",
			Region:             "us-east-1",
			AWSAccessKeyID:     "key_one",
			AWSSecretAccessKey: "secret_key_one",
			AWSSessionToken:    "session_token_one",
		},
	}

	b := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source:     &kafka.Kafka{Topic: "funfunfun"},
	}

	dataA, err := json.Marshal(a)
	assert.NotNil(t, dataA)
	assert.Nil(t, err)

	var srcA connection.Connection
	err = json.Unmarshal(dataA, &srcA)
	assert.Nil(t, err)

	dataB, err := json.Marshal(b)
	assert.NotNil(t, dataB)
	assert.Nil(t, err)

	var srcB connection.Connection
	err = json.Unmarshal(dataB, &srcB)

	assert.NotNil(t, err)
}
