# Event Gateway Connector

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