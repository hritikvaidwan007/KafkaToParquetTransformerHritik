package com.example;

import com.example.util.AvroParquetWriter;
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

/**
 * A simplified version of the KafkaToS3Job for local testing
 * Uses a fixed Avro schema instead of fetching from an API
 */
public class LocalTestJob {
    private static final Logger LOG = LoggerFactory.getLogger(LocalTestJob.class);
    
    // Sample Avro schema for testing - matches the schema in the sample API response
    private static final String TEST_SCHEMA = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"MyClass\",\n" +
            "  \"namespace\": \"com.acme.avro\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"_timestamp\",\n" +
            "      \"type\": [\"null\", \"string\"],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"title\",\n" +
            "      \"type\": [\"null\", \"string\"],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"schemaId\",\n" +
            "      \"type\": [\"null\", \"int\"],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"schemaVersion\",\n" +
            "      \"type\": [\"null\", \"int\"],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    
    public static void main(String[] args) throws Exception {
        // Get command line parameters
        var params = ParameterTool.fromArgs(args);
        
        // Configuration properties
        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String topic = params.get("topic", "input-topic");
        String outputPath = params.get("output-path", "file:///tmp/output");
        int windowMinutes = params.getInt("window-minutes", 5);
        
        LOG.info("Starting LocalTestJob with Kafka broker: {}, topic: {}, output path: {}", 
                bootstrapServers, topic, outputPath);
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Parse the schema
        Schema avroSchema = new Schema.Parser().parse(TEST_SCHEMA);
        LOG.info("Using test schema: {}", avroSchema.getName());
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-test-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create data stream from Kafka source
        DataStream<String> stream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Kafka Source");
        
        // Process stream and convert to Avro GenericRecord
        DataStream<GenericRecord> avroRecords = stream
                .map(message -> {
                    LOG.info("Processing message: {}", message);
                    return AvroParquetWriter.convertJsonToAvro(message, avroSchema);
                })
                .name("Convert to Avro");
        
        // Create Parquet sink
        StreamingFileSink<GenericRecord> parquetSink = AvroParquetWriter.createParquetSink(
                new Path(outputPath), 
                avroSchema, 
                Time.minutes(windowMinutes));
        
        // Window the data stream and write to parquet files
        avroRecords
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(windowMinutes)))
                .apply(AvroParquetWriter::windowedWriter)
                .addSink(parquetSink)
                .name("Parquet Sink");
        
        // Execute the Flink job
        env.execute("Local Test Job");
    }
}