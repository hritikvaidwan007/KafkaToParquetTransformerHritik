package com.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Model class to represent the schema registry API response
 * This class demonstrates the use of:
 * 1. Jackson annotations for JSON mapping
 * 2. Immutable objects with getters but no setters
 * 3. Nested static classes for hierarchical data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaResponse {

    @JsonProperty("schemaMetadata")
    private SchemaMetadata schemaMetadata;
    
    @JsonProperty("id")
    private int id;
    
    @JsonProperty("schemas")
    private Schemas schemas;
    
    // Default constructor required for Jackson deserialization
    public SchemaResponse() {
    }
    
    // Getters - demonstrating JavaBean pattern
    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }
    
    public int getId() {
        return id;
    }
    
    public Schemas getSchemas() {
        return schemas;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SchemaMetadata {
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("description")
        private String description;
        
        public String getName() {
            return name;
        }
        
        public String getType() {
            return type;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Schemas {
        @JsonProperty("versions")
        private List<SchemaVersion> versions = new ArrayList<>();
        
        @JsonProperty("draft")
        private List<SchemaVersion> draft = new ArrayList<>();
        
        public List<SchemaVersion> getVersions() {
            return versions;
        }
        
        public List<SchemaVersion> getDraft() {
            return draft;
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SchemaVersion {
        @JsonProperty("id")
        private int id;
        
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("version")
        private int version;
        
        @JsonProperty("schemaText")
        private String schemaText;
        
        @JsonProperty("timestamp")
        private long timestamp;
        
        @JsonProperty("stateId")
        private int stateId;
        
        public int getId() {
            return id;
        }
        
        public String getName() {
            return name;
        }
        
        public String getDescription() {
            return description;
        }
        
        public int getVersion() {
            return version;
        }
        
        public String getSchemaText() {
            return schemaText;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public int getStateId() {
            return stateId;
        }
    }
}
