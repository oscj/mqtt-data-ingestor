# mqtt-data-ingestor

A simple ingestion program that subscribes to a topic on an mqtt broker and inserts the incoming messages to a mongo database.

## Required
- This program requires a config.yaml file in the root of the directory w/ the following fields:

```yaml
broker_address: <broker_address>
broker_port: <broker_port>
client_id: <client_id>
client_password: <client_password>
client_username: <client_username>
target_topic: <target_topic>
db_uri: <db_uri>
target_db: <target_db>
target_collection: <target_collection>
```