# Event Gateway Connector

Event Gateway Connector enables importing messages (events) from different, popular message queues into Event Gateway. Currently supported technologies: AWS Kinesis.

## Concepts

**Source** is a implementation of message queue client that connects to the queue (based on Connection) and fetches messages. Currently implemented sources can be found in [`sources`](./sources) folder.

**Connection** is a configuration describing a source message queue (address, credentials) and target Event Gateway (e.g. URL, path, event type). Connections are managed by [Config API](#config-api).


## Config API

Config API allows creating connections that Connector will use to fetch data from.

### API

#### Create Connection

`POST /v1/spaces/<space name>/connections`

Payload:
```
{
	"target": "http://localhost:4001",
	"eventType": "user.created",
	"type": "awskinesis",
	"source": {
		"streamName": "test",
		"region": "us-east-1"
	}
}
```

#### Update Connection

`PUT /v1/spaces/<space name>/connections/<connection ID>`

Payload:
```
{
	"target": "http://localhost:4001",
	"eventType": "user.created",
	"type": "awskinesis",
	"source": {
		"streamName": "test",
		"region": "us-east-1"
	}
}
```

#### Delete Connection

`DELETE /v1/spaces/<space name>/connections/<connection ID>`