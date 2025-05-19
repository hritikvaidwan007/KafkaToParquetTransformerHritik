package com.example;

import com.example.model.SchemaResponse;
import com.example.util.AvroParquetWriter;
import com.example.util.SchemaFetcher;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Apache Flink application that:
 * 1. Consumes data from Kafka
 * 2. Fetches Avro schema from an API
 * 3. Processes the data using the schema
 * 4. Writes to S3 as Parquet files every 5 minutes
 *
 * <p>This job utilizes Flink's DataStream API which is the foundation of Flink applications.
 * The DataStream API provides primitives for streaming data processing, with streams being 
 * potentially unbounded sequences of data records.
 * 
 * <p>References:
 * - Flink DataStream API: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/
 * - Flink Kafka Connector: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/
 * - Flink File Sink: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/
 * - Flink Window Operations: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/
 */
public class KafkaToS3Job {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToS3Job.class);
    
    public static void main(String[] args) throws Exception {
        // Get command line parameters using ParameterTool, which is a convenient utility 
        // to parse program arguments and retrieve configuration values
        // Demonstrating use of Java's var keyword for type inference (Java 10+)
        var params = ParameterTool.fromArgs(args);
        
        // Configuration properties with default values. These can be overridden via command line args.
        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String topic = params.get("topic", "input-topic");
        String username = params.get("username", "");
        String password = params.get("password", "");
        String certFile = params.get("cert-file", "");
        String s3Path = params.get("s3-path", "s3://my-bucket/output");
        String schemaApiUrl = params.get("schema-api-url", 
                "https://frame-be.sandbox.analytics.yo-digital.com/api/v1/schemaregistry/schemas/TV_EVENTS_RAW.CENTRAL/aggregated?isTabular=false");
        int schemaVersion = params.getInt("schema-version", -1); // -1 means use latest version
        
        // Create execution environment, which is the entry point for Flink applications
        // getExecutionEnvironment() automatically detects whether to use a local environment
        // or to use a remote cluster environment based on the context
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/execution_mode/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Fetch schema from API - we only need to do this once at the start
        // This is an example of accessing external systems during job initialization
        SchemaFetcher schemaFetcher = new SchemaFetcher();
        SchemaResponse schemaResponse = schemaFetcher.fetchSchema(schemaApiUrl);
        Schema avroSchema = schemaFetcher.getAvroSchema(schemaResponse, schemaVersion);
        LOG.info("Using schema version: {}", schemaVersion == -1 ? "latest" : schemaVersion);
        
        // Configure Kafka source authentication properties
        // Flink Kafka connector supports different authentication mechanisms
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/#security
        Properties kafkaProps = new Properties();
        if (!username.isEmpty() && !password.isEmpty()) {
            // SASL authentication if credentials are provided
            // SASL_SSL is a security protocol that first makes an SSL connection, then uses SASL for authentication
            kafkaProps.put("security.protocol", "SASL_SSL");
            kafkaProps.put("sasl.mechanism", "PLAIN");
            kafkaProps.put("sasl.jaas.config", 
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + 
                    username + "\" password=\"" + password + "\";");
        }
        
        if (!certFile.isEmpty()) {
            // SSL settings if cert file is provided for secure connections
            kafkaProps.put("ssl.truststore.location", certFile);
        }
        
        // Create Kafka source using the Flink Kafka connector
        // KafkaSource uses the builder pattern to construct a source that consumes Kafka topics
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/#using-the-kafka-connector
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)      // Set Kafka broker addresses
                .setTopics(topic)                           // Set which topics to consume
                .setProperties(kafkaProps)                  // Set additional Kafka properties
                .setStartingOffsets(OffsetsInitializer.latest())  // Start from latest offsets
                .setValueOnlyDeserializer(new SimpleStringSchema())  // Deserialize Kafka messages as strings
                .build();
        
        // Create a DataStream from the Kafka source
        // This represents the main input stream of data for our job
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/#datastream-api
        DataStream<String> stream = env.fromSource(
                source,                         // The source to read from
                WatermarkStrategy.noWatermarks(),  // No event-time processing, using processing time
                "Kafka Source");                // Source operator name for monitoring
        
        // Process stream by converting JSON strings to Avro GenericRecords
        // The map transformation applies a function to each element and creates a new stream
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/#map
        DataStream<GenericRecord> avroRecords = stream
                .map(message -> AvroParquetWriter.convertJsonToAvro(message, avroSchema))
                .name("Convert to Avro");  // Name the operator for better monitoring
        
        // Create a file sink that writes Parquet files to S3 (or any supported filesystem)
        // StreamingFileSink supports exactly-once semantics for file systems that provide
        // flush/sync/hsync, and at-least-once semantics for other file systems
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/
        StreamingFileSink<GenericRecord> parquetSink = AvroParquetWriter.createParquetSink(
                new Path(s3Path),     // Output path (S3, HDFS, or local FS)
                avroSchema,           // Avro schema for Parquet structure
                Time.minutes(5));     // Rolling interval
        
        // Window the data stream to collect records for a period before writing
        // Tumbling windows group elements into fixed-size, non-overlapping windows
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/
        avroRecords
                // Create 5-minute tumbling windows based on processing time
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                // Apply window function to process & collect records before writing
                .apply(AvroParquetWriter::windowedWriter)
                // Add the file sink at the end of the processing pipeline
                .addSink(parquetSink)
                .name("Parquet S3 Sink");  // Name the sink for monitoring
        
        // Execute the Flink job
        // This starts actual execution of the data pipeline
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/execution/
        env.execute("Kafka to S3 Parquet Job");
    }
}