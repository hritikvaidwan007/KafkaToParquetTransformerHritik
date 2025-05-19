package com.example.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

/**
 * Utility class to handle Avro-Parquet conversion and writing
 * This class demonstrates:
 * 1. Static utility methods
 * 2. JSON to Avro conversion
 * 3. Parquet file creation
 * 4. Custom Flink trigger implementation
 */
public class AvroParquetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetWriter.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    
    /**
     * Convert a JSON string to an Avro GenericRecord
     * 
     * @param jsonString JSON string to convert
     * @param schema Avro schema to use for conversion
     * @return Avro GenericRecord
     */
    public static GenericRecord convertJsonToAvro(String jsonString, Schema schema) throws IOException {
        try {
            // Parse the JSON string to a Map
            // Demonstrating type casting and generic types
            @SuppressWarnings("unchecked")
            Map<String, Object> jsonMap = JSON_MAPPER.readValue(jsonString, Map.class);
            
            // Create a new GenericRecord based on the schema
            GenericRecord record = new GenericData.Record(schema);
            
            // Iterate through schema fields and set values from JSON
            // Demonstrating for-each loop with schema fields
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                
                // If the field exists in the JSON, set it in the record
                if (jsonMap.containsKey(fieldName)) {
                    record.put(fieldName, jsonMap.get(fieldName));
                } else {
                    // If the field is not in the JSON, set null or default value
                    record.put(fieldName, null);
                }
            }
            
            return record;
        } catch (Exception e) {
            LOG.error("Error converting JSON to Avro: {}", jsonString, e);
            throw new IOException("Failed to convert JSON to Avro", e);
        }
    }
    
    /**
     * Create a Parquet sink that writes to S3
     * 
     * @param outputPath S3 output path
     * @param schema Avro schema to use
     * @param rollInterval Time interval for rolling Parquet files
     * @return StreamingFileSink configured for Parquet output
     */
    public static StreamingFileSink<GenericRecord> createParquetSink(
            Path outputPath, 
            Schema schema,
            Time rollInterval) {
        
        // Create a builder for the streaming file sink
        // Demonstrating builder pattern and method references
        return StreamingFileSink
                .forBulkFormat(outputPath, AvroParquetWriters.forGenericRecord(schema))
                .withRollingPolicy(
                    OnCheckpointRollingPolicy.build())
                .build();
    }
    
    /**
     * Process windowed data for writing to S3
     * 
     * @param window Time window
     * @param records Input records in the window
     * @param out Output collector
     */
    public static void windowedWriter(
            TimeWindow window,
            Iterable<GenericRecord> records,
            Collector<GenericRecord> out) {
        
        // Simply forward all records in the window to the sink
        // Demonstrating iterator usage
        for (GenericRecord record : records) {
            out.collect(record);
        }
        
        // Log window statistics
        long count = 0;
        for (Iterator<GenericRecord> it = records.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        
        LOG.info("Processed window [{}, {}] with {} records", 
                window.getStart(), window.getEnd(), count);
    }
    
    /**
     * Custom trigger to fire based on count or time
     * Demonstrates:
     * 1. Generic type parameters
     * 2. Class inheritance
     * 3. Method overriding
     */
    public static class CountAndTimeTrigger<T> extends Trigger<T, TimeWindow> {
        private final long maxCount;
        private final long timeInterval;
        
        private transient ListState<Long> countState;
        
        public CountAndTimeTrigger(long maxCount, Time timeInterval) {
            this.maxCount = maxCount;
            this.timeInterval = timeInterval.toMilliseconds();
        }
        
        @Override
        public void onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // Initialize state if this is the first element
            if (countState == null) {
                countState = ctx.getPartitionedState(
                        new ListStateDescriptor<>("count", Long.class));
            }
            
            // Update count
            long count = getCount() + 1;
            countState.clear();
            countState.add(count);
            
            // Register timer for window end
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            
            // Fire if count exceeds maxCount
            if (count >= maxCount) {
                return TriggerResult.FIRE;
            }
            
            return TriggerResult.CONTINUE;
        }
        
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // Fire on processing time
            return TriggerResult.FIRE;
        }
        
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // We're not using event time
            return TriggerResult.CONTINUE;
        }
        
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // Clear state when window is purged
            if (countState != null) {
                countState.clear();
            }
        }
        
        private long getCount() throws Exception {
            if (countState == null) {
                return 0;
            }
            
            Iterator<Long> iterator = countState.get().iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
            
            return 0;
        }
    }
}
