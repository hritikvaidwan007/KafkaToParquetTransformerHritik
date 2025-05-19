package com.example;

import com.example.model.SchemaResponse;
import com.example.util.AvroParquetWriter;
import com.example.util.SchemaFetcher;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Apache Flink application that:
 * 1. Consumes data from Kafka
 * 2. Fetches Avro schema from an API
 * 3. Processes the data using the schema
 * 4. Writes to S3 as Parquet files every 5 minutes
 */
public class KafkaToS3Job {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToS3Job.class);
    
    public static void main(String[] args) throws Exception {
        // Get command line parameters
        // Demonstrating use of Java's var keyword for type inference (Java 10+)
        var params = ParameterTool.fromArgs(args);
        
        // Configuration properties
        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String topic = params.get("topic", "input-topic");
        String username = params.get("username", "");
        String password = params.get("password", "");
        String certFile = params.get("cert-file", "");
        String s3Path = params.get("s3-path", "s3://my-bucket/output");
        String schemaApiUrl = params.get("schema-api-url", 
                "https://frame-be.sandbox.analytics.yo-digital.com/api/v1/schemaregistry/schemas/TV_EVENTS_RAW.CENTRAL/aggregated?isTabular=false");
        int schemaVersion = params.getInt("schema-version", -1); // -1 means use latest version
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Fetch schema from API - we only need to do this once at the start
        SchemaFetcher schemaFetcher = new SchemaFetcher();
        SchemaResponse schemaResponse = schemaFetcher.fetchSchema(schemaApiUrl);
        Schema avroSchema = schemaFetcher.getAvroSchema(schemaResponse, schemaVersion);
        LOG.info("Using schema version: {}", schemaVersion == -1 ? "latest" : schemaVersion);
        
        // Configure Kafka source
        Properties kafkaProps = new Properties();
        if (!username.isEmpty() && !password.isEmpty()) {
            // SASL authentication if credentials are provided
            // Demonstrating string concatenation
            kafkaProps.put("security.protocol", "SASL_SSL");
            kafkaProps.put("sasl.mechanism", "PLAIN");
            kafkaProps.put("sasl.jaas.config", 
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + 
                    username + "\" password=\"" + password + "\";");
        }
        
        if (!certFile.isEmpty()) {
            // SSL settings if cert file is provided
            kafkaProps.put("ssl.truststore.location", certFile);
        }
        
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setProperties(kafkaProps)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create data stream from Kafka source
        // Demonstrating method chaining - a common pattern in Flink
        DataStream<String> stream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Kafka Source");
        
        // Process stream and convert to Avro GenericRecord
        // Demonstrating lambda expressions (Java 8+)
        DataStream<GenericRecord> avroRecords = stream
                .map(message -> AvroParquetWriter.convertJsonToAvro(message, avroSchema))
                .name("Convert to Avro");
        
        // Create Parquet sink
        // Using StreamingFileSink with path and custom rolling policy
        // Demonstrating use of Builder Pattern
        StreamingFileSink<GenericRecord> parquetSink = AvroParquetWriter.createParquetSink(
                new Path(s3Path), 
                avroSchema, 
                Time.minutes(5));
        
        // Window the data stream and write to S3 every 5 minutes
        // Demonstrating use of method references (Java 8+)
        avroRecords
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .trigger(new AvroParquetWriter.CountAndTimeTrigger<>(1000, Time.minutes(5)))
                .apply(AvroParquetWriter::windowedWriter)
                .addSink(parquetSink)
                .name("Parquet S3 Sink");
        
        // Execute the Flink job
        env.execute("Kafka to S3 Parquet Job");
    }
}
