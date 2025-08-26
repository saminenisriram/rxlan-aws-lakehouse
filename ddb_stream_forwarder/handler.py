"""
rxlan-ddb-stream-forwarder (Step 3)

Flow:
DynamoDB Table (with Streams ON) →
    DynamoDB Streams →
        Lambda (this code) →
            Kinesis Data Stream (rxlan-ddb-events-dev) →
                Firehose →
                    S3 (Bronze)

What this Lambda does:
- Triggered automatically by DynamoDB Streams
- For each INSERT, take the NewImage
- Convert DynamoDB's type-wrapped JSON into plain JSON
- Batch send records into Kinesis Data Stream
- Partition by 'city' (or fallback key)
"""

import os
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer


KINESIS_STREAM = os.getenv("KINESIS_STREAM", "rxlan-ddb-events-dev")
PARTITION_KEY_FIELD = os.getenv("PARTITION_KEY", "city")

kinesis = boto3.client("kinesis")
deserializer = TypeDeserializer()


def lambda_handler(event, context):
    records_to_send = []
    for record in event.get("Records", []):
        if record.get("eventName") != "INSERT":
            continue

        new_image = record.get("dynamodb").get("NewImage")
        if not new_image:
            continue

        # Deserialize DynamoDB JSON to plain JSON
        plain_record = {k: deserializer.deserialize(v) for k, v in new_image.items()}

        # Determine partition key
        partition_key = str(plain_record.get(PARTITION_KEY_FIELD, "unknown"))

        # Prepare record for Kinesis
        records_to_send.append({
            "Data": (json.dumps(plain_record) + "\n").encode("utf-8"),
            "PartitionKey": partition_key
        })

    if records_to_send:
        resp = kinesis.put_records(
            Records=records_to_send,
            StreamName=KINESIS_STREAM
        )
        print(json.dumps({
            "level": "info",
            "sent": len(records_to_send),
            "failed": resp.get("FailedRecordCount", 0)
        }))

    return {"records_processed": len(records_to_send), "kinesis_response": resp}