# Apache Flink Kafka to S3 Parquet Application

This application consumes data from Kafka, applies an Avro schema from an API response, and writes to S3 as Parquet files every 5 minutes.

## Features

- Consumes data from Kafka topic
- Fetches Avro schema from a schema registry API
- Processes incoming Kafka messages using the schema
- Writes records to S3 in Parquet format every 5 minutes
- Supports authentication for Kafka (SASL/SSL)
- Configurable schema version selection

## Requirements

- Java 11+
- Maven 3.6+
- Kafka cluster
- S3-compatible storage
- Schema Registry API for Avro schemas

## Building the Application

To build the application:

```bash
mvn clean package
```

This will create a JAR file in the `target` directory and copy all dependencies to `target/dependency` for easier execution.

## Configuration

The application supports the following command-line parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--bootstrap-servers` | Kafka bootstrap servers | `localhost:9092` |
| `--topic` | Kafka topic to consume from | `input-topic` |
| `--username` | Kafka SASL username | _(empty)_ |
| `--password` | Kafka SASL password | _(empty)_ |
| `--cert-file` | Kafka SSL certificate file | _(empty)_ |
| `--s3-path` | S3 path for output files | `s3://my-bucket/output` |
| `--schema-api-url` | URL of the schema registry API | `https://frame-be.sandbox.analytics.yo-digital.com/api/v1/schemaregistry/schemas/TV_EVENTS_RAW.CENTRAL/aggregated?isTabular=false` |
| `--schema-version` | Specific schema version to use, -1 for latest | `-1` (latest) |

## Running the Application

To run the application with default parameters:

```bash
java -classpath target/classes:target/dependency/* com.example.KafkaToS3Job
```

To specify parameters:

```bash
java -classpath target/classes:target/dependency/* com.example.KafkaToS3Job \
  --bootstrap-servers kafka1:9092,kafka2:9092 \
  --topic my-kafka-topic \
  --username myuser \
  --password mypassword \
  --s3-path s3://my-bucket/my-data \
  --schema-version 2
```

## Testing Locally

For local testing, we provide a Docker Compose setup with Kafka, Zookeeper, and MinIO (S3-compatible storage).

1. Start the local environment:

```bash
docker-compose up -d
```

2. Generate test data and send to Kafka:

```bash
./generate-test-data.sh
```

3. Run the test application (uses a simplified version with a fixed schema):

```bash
./run-local-test.sh
```

4. Check the output:
   - For local file output: `/tmp/output`
   - For MinIO output: Open http://localhost:9001 in your browser (credentials: minio/minio123)

## Architecture

The application consists of:

1. `KafkaToS3Job` - Main Flink job that coordinates the data flow
2. `SchemaFetcher` - Utility to fetch and parse Avro schema from API
3. `AvroParquetWriter` - Utility to handle Avro-Parquet conversion and writing
4. `SchemaResponse` - Model class for the schema registry API response

## File Rolling Strategy

The application writes Parquet files every 5 minutes using Flink's tumbling windows. When using S3, files are written in the following pattern:

```
s3://my-bucket/output/yyyy-MM-dd--HH-mm/part-0-0
```

Each window's data is written as a separate file, making it easy to track and process data chronologically.

## Production Deployment

For production deployment:

1. Configure proper Kafka authentication (username/password)
2. Set up proper S3 credentials
3. Configure resource requirements for your Flink cluster
4. Consider setting up checkpointing for fault tolerance

## Troubleshooting

- **Kafka Connection Issues**: Verify bootstrap servers, topic existence, and authentication credentials
- **Schema Registry Issues**: Check API URL and connectivity
- **S3 Write Issues**: Verify S3 credentials and bucket permissions