package com.example.util;

import com.example.model.SchemaResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;

/**
 * Utility class to fetch and parse Avro schemas from an external schema registry API.
 * 
 * <p>This class handles the interaction with the schema registry during the Flink job initialization.
 * It retrieves schema metadata and converts it to Apache Avro Schema objects that can be used
 * for serialization/deserialization in the Flink pipeline.
 * 
 * <p>In Flink applications, fetching configurations or schemas is typically done during
 * the initialization phase before the streaming pipeline begins execution. This approach 
 * ensures the schema is available to all parallel instances of the job.
 * 
 * <p>References:
 * - Flink Configuration: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/config/
 * - Avro Schemas: https://avro.apache.org/docs/1.11.1/specification/
 * - Flink with Avro: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/formats/avro/
 */
public class SchemaFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaFetcher.class);
    private final ObjectMapper objectMapper;
    
    /**
     * Initializes a new SchemaFetcher with a Jackson ObjectMapper for JSON processing.
     * 
     * <p>The ObjectMapper is configured with default settings for parsing JSON responses
     * from the schema registry API.
     */
    public SchemaFetcher() {
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Fetches the schema definition from the specified schema registry API endpoint.
     * 
     * <p>This method makes an HTTP GET request to the schema registry and parses the
     * JSON response into a structured SchemaResponse object. It is designed to be called
     * during the initialization phase of the Flink job.
     * 
     * <p>Network connections in Flink should typically be established outside the main
     * processing pipeline to avoid creating connections for each record, which can lead
     * to performance issues. However, fetching schema information once during initialization
     * is a common and efficient pattern.
     * 
     * <p>References:
     * - HTTP Best Practices: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/process_function/#integration-with-external-systems
     * 
     * @param apiUrl The URL of the schema registry API endpoint
     * @return A SchemaResponse object containing the schema metadata and versions
     * @throws IOException If there's an error connecting to the API or parsing the response
     */
    public SchemaResponse fetchSchema(String apiUrl) throws IOException {
        // Using try-with-resources to ensure proper resource cleanup
        // This pattern is essential for managing external connections in Flink applications
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            LOG.info("Fetching schema from API: {}", apiUrl);
            
            HttpGet request = new HttpGet(apiUrl);
            
            // Execute the request and process the response
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                // Validate the response status code
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(String.format("Failed to fetch schema: HTTP error code: %d", statusCode));
                }
                
                // Extract and parse the response body
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonStr = EntityUtils.toString(entity);
                    LOG.debug("Received schema response: {}", jsonStr);
                    
                    // Deserialize JSON to SchemaResponse POJO
                    return objectMapper.readValue(jsonStr, SchemaResponse.class);
                } else {
                    throw new IOException("Empty response from schema registry API");
                }
            }
        } catch (Exception e) {
            // Proper exception handling with contextual information
            LOG.error("Error fetching schema", e);
            throw new IOException("Failed to fetch schema from API", e);
        }
    }
    
    /**
     * Extracts and parses an Avro Schema from the schema registry response.
     * 
     * <p>This method supports selecting either the latest schema version or a specific
     * version by number. The schema text from the response is parsed into an Avro Schema
     * object that can be used for data serialization and deserialization in the Flink pipeline.
     * 
     * <p>In Flink, schema management is critical for data processing pipelines, especially
     * when dealing with evolving data structures. The Avro Schema object created here will
     * be used by the Flink-Avro integration to properly serialize/deserialize records.
     * 
     * <p>References:
     * - Avro Schema Resolution: https://avro.apache.org/docs/1.11.1/spec.html#Schema+Resolution
     * - Flink Serialization: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/serialization/types_serialization/
     * 
     * @param schemaResponse The schema registry response containing available schema versions
     * @param requestedVersion The specific version to use, or -1 to select the latest version
     * @return An Avro Schema object parsed from the selected schema definition
     * @throws IOException If there's an error finding or parsing the schema
     */
    public Schema getAvroSchema(SchemaResponse schemaResponse, int requestedVersion) throws IOException {
        try {
            // Get the list of schema versions from the response
            var versions = schemaResponse.getSchemas().getVersions();
            
            if (versions == null || versions.isEmpty()) {
                throw new IOException("No schema versions found in the response");
            }
            
            // Determine which schema version to use
            SchemaResponse.SchemaVersion schemaVersion;
            
            if (requestedVersion == -1) {
                // Use the latest version when requestedVersion is -1
                // Using Java 8 Stream API for efficient collection processing
                schemaVersion = versions.stream()
                        .max(Comparator.comparingInt(SchemaResponse.SchemaVersion::getVersion))
                        .orElseThrow(() -> new IOException("Failed to find latest schema version"));
            } else {
                // Find the specific requested version
                schemaVersion = versions.stream()
                        .filter(v -> v.getVersion() == requestedVersion)
                        .findFirst()
                        .orElseThrow(() -> new IOException("Schema version " + requestedVersion + " not found"));
            }
            
            LOG.info("Using schema version: {}", schemaVersion.getVersion());
            
            // Parse the schema text into an Avro Schema object
            // The Schema object will be used for data serialization/deserialization in the Flink pipeline
            return new Schema.Parser().parse(schemaVersion.getSchemaText());
            
        } catch (Exception e) {
            LOG.error("Error parsing Avro schema", e);
            throw new IOException("Failed to parse Avro schema", e);
        }
    }
}