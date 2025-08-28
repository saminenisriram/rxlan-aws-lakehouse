# rxlan-ddb-stream-forwarder

- Trigger: DynamoDB Streams on WeatherIngest table
- Destination: Kinesis Data Stream (`rxlan-ddb-events-dev`)
- Partition key: `city` (env var configurable)

### Env vars
- KINESIS_STREAM = rxlan-ddb-events-dev
- PARTITION_KEY  = city
- APP_NAME       = rxlan
- STAGE          = dev
