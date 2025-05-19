package com.example.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class to handle Avro-Parquet conversion and writing operations for Flink.
 * This class provides methods for:
 * 1. Converting JSON messages to Avro GenericRecords
 * 2. Creating Parquet file sinks for S3 or other filesystems
 * 3. Processing windowed data for streaming writes
 *
 * <p>The class leverages Flink's built-in support for the Parquet format and Avro serialization.
 * It acts as a bridge between raw JSON data from Kafka and the structured file storage in S3.
 *
 * <p>References:
 * - Flink Parquet Format: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/formats/parquet/
 * - Flink File Sink: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/
 * - Flink Windows: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/
 */
public class AvroParquetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetWriter.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    
    /**
     * Converts a JSON string to an Avro GenericRecord based on the provided schema.
     * This method serves as a transformation function for Flink's map operator.
     * 
     * <p>The method maps JSON fields to Avro schema fields, handling missing fields
     * by setting them to null or default values defined in the schema.
     * 
     * <p>Reference: 
     * - Avro GenericRecord: https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecord.html
     * - Flink Map Transformation: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/#map
     * 
     * @param jsonString The JSON string to convert, typically from a Kafka message
     * @param schema The Avro schema to structure the record with
     * @return An Avro GenericRecord representing the JSON data
     * @throws IOException If conversion fails due to parsing errors
     */
    public static GenericRecord convertJsonToAvro(String jsonString, Schema schema) throws IOException {
        try {
            // Parse the JSON string to a Map using Jackson
            // This is a common approach for dynamic JSON parsing in Java
            @SuppressWarnings("unchecked")
            Map<String, Object> jsonMap = JSON_MAPPER.readValue(jsonString, Map.class);
            
            // Create a new GenericRecord based on the schema
            // GenericRecord is Avro's representation of a record that conforms to a specific schema
            GenericRecord record = new GenericData.Record(schema);
            
            // Iterate through schema fields and set values from JSON
            // This ensures all schema fields are properly populated
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                
                // If the field exists in the JSON, set it in the record
                if (jsonMap.containsKey(fieldName)) {
                    record.put(fieldName, jsonMap.get(fieldName));
                } else {
                    // If the field is not in the JSON, set null
                    // The Avro schema's default values will be used if available
                    record.put(fieldName, null);
                }
            }
            
            return record;
        } catch (Exception e) {
            // Log error details for debugging and wrap the exception for upstream handling
            LOG.error("Error converting JSON to Avro: {}", jsonString, e);
            throw new IOException("Failed to convert JSON to Avro", e);
        }
    }
    
    /**
     * Creates a StreamingFileSink that writes Avro GenericRecords as Parquet files.
     * 
     * <p>The resulting sink supports writing to any Flink-compatible filesystem like S3, HDFS, or local files.
     * It uses Flink's checkpoint mechanism to ensure exactly-once semantics when possible.
     * 
     * <p>References:
     * - Flink StreamingFileSink: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#using-the-streamingfilesink
     * - Flink Parquet Format: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/formats/parquet/
     * - Flink Checkpointing: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/
     * 
     * @param outputPath The path where Parquet files will be written (S3, HDFS, or local filesystem)
     * @param schema The Avro schema that defines the structure of the Parquet files
     * @param rollInterval The time interval for rolling files (not directly used in this implementation)
     * @return A configured StreamingFileSink for writing Parquet files
     */
    public static StreamingFileSink<GenericRecord> createParquetSink(
            Path outputPath, 
            Schema schema,
            Time rollInterval) {
        
        // Create a StreamingFileSink for bulk format (Parquet)
        // The BulkWriter handles batching records efficiently for the Parquet columnar format
        // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#bulk-encoded-formats
        return StreamingFileSink
                // Use Avro-to-Parquet writer provided by Flink
                .forBulkFormat(outputPath, AvroParquetWriters.forGenericRecord(schema))
                // Configure rolling policy based on checkpoints
                // This ensures files are properly finalized during checkpoints for fault tolerance
                // Reference: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#rolling-policy
                .withRollingPolicy(
                    OnCheckpointRollingPolicy.build())
                .build();
    }
    
    /**
     * Process and collect records from a time window for writing to the Parquet sink.
     * This method is used as a window function in the Flink pipeline.
     * 
     * <p>This implementation simply forwards all records in the window to the sink,
     * but it could be extended to perform aggregations or transformations on the window data.
     * 
     * <p>References:
     * - Flink Window Apply Function: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/#window-function
     * - Flink Collector: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/#collector
     * 
     * @param window The time window containing a batch of records
     * @param records The collection of records within the window
     * @param out The collector to output processed records
     */
    public static void windowedWriter(
            TimeWindow window,
            Iterable<GenericRecord> records,
            Collector<GenericRecord> out) {
        
        // Forward all records in the window to the sink
        // The Collector interface is Flink's way of emitting results from operators
        for (GenericRecord record : records) {
            out.collect(record);
        }
        
        // Count records for logging (second pass through the iterator)
        // This provides visibility into the window processing
        long count = 0;
        for (Iterator<GenericRecord> it = records.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        
        // Log window completion information for monitoring
        LOG.info("Processed window [{}, {}] with {} records", 
                window.getStart(), window.getEnd(), count);
    }
}