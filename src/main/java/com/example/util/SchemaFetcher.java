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
import java.util.Optional;

/**
 * Utility class to fetch Avro schema from the schema registry API
 * This class demonstrates:
 * 1. HTTP client usage
 * 2. Exception handling
 * 3. JSON parsing
 * 4. Java Optional usage
 */
public class SchemaFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaFetcher.class);
    private final ObjectMapper objectMapper;
    
    /**
     * Constructor - demonstrates initializing instance variables
     */
    public SchemaFetcher() {
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Fetch the schema from the schema registry API
     * 
     * @param apiUrl the URL of the schema registry API
     * @return the schema response object
     * @throws IOException if there's an error fetching or parsing the schema
     */
    public SchemaResponse fetchSchema(String apiUrl) throws IOException {
        // Using try-with-resources to ensure resources are closed (Java 7+)
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            LOG.info("Fetching schema from API: {}", apiUrl);
            
            HttpGet request = new HttpGet(apiUrl);
            
            // Execute the request and get the response
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                // Check response status code
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    // Demonstrating string formatting with String.format()
                    throw new IOException(String.format("Failed to fetch schema: HTTP error code: %d", statusCode));
                }
                
                // Parse the response entity
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonStr = EntityUtils.toString(entity);
                    LOG.debug("Received schema response: {}", jsonStr);
                    
                    // Parse JSON to SchemaResponse object
                    return objectMapper.readValue(jsonStr, SchemaResponse.class);
                } else {
                    throw new IOException("Empty response from schema registry API");
                }
            }
        } catch (Exception e) {
            // Demonstrating exception chaining
            LOG.error("Error fetching schema", e);
            throw new IOException("Failed to fetch schema from API", e);
        }
    }
    
    /**
     * Get the Avro schema from the schema response
     * 
     * @param schemaResponse the schema response object
     * @param requestedVersion the requested schema version, -1 for latest
     * @return the Avro schema
     * @throws IOException if there's an error parsing the schema
     */
    public Schema getAvroSchema(SchemaResponse schemaResponse, int requestedVersion) throws IOException {
        try {
            // Get the list of schema versions
            var versions = schemaResponse.getSchemas().getVersions();
            
            if (versions == null || versions.isEmpty()) {
                throw new IOException("No schema versions found in the response");
            }
            
            // Find the requested schema version or get the latest
            SchemaResponse.SchemaVersion schemaVersion;
            
            if (requestedVersion == -1) {
                // Get the latest version using stream API and max() (Java 8+)
                // Demonstrating Stream API and Lambda expressions
                schemaVersion = versions.stream()
                        .max(Comparator.comparingInt(SchemaResponse.SchemaVersion::getVersion))
                        .orElseThrow(() -> new IOException("Failed to find latest schema version"));
            } else {
                // Get the specific version using stream API and filter() (Java 8+)
                // Demonstrating Stream API, Lambda expressions, and Optional handling
                schemaVersion = versions.stream()
                        .filter(v -> v.getVersion() == requestedVersion)
                        .findFirst()
                        .orElseThrow(() -> new IOException("Schema version " + requestedVersion + " not found"));
            }
            
            LOG.info("Using schema version: {}", schemaVersion.getVersion());
            
            // Parse the schema text into an Avro Schema object
            return new Schema.Parser().parse(schemaVersion.getSchemaText());
            
        } catch (Exception e) {
            LOG.error("Error parsing Avro schema", e);
            throw new IOException("Failed to parse Avro schema", e);
        }
    }
}
