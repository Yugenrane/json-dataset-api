package com.example.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

@Entity
@Table(name = "dataset_records",
        indexes = {
                @Index(name = "idx_dataset_name", columnList = "dataset_name"),
                @Index(name = "idx_created_at", columnList = "created_at")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Slf4j
public class DatasetRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dataset_name", nullable = false, length = 100)
    @NotBlank(message = "Dataset name cannot be blank")
    @Size(min = 1, max = 100, message = "Dataset name must be between 1 and 100 characters")
    private String datasetName;

    @Column(name = "record_data", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    @NotNull(message = "Record data cannot be null")
    private String recordData;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private LocalDateTime updatedAt = LocalDateTime.now();

    public DatasetRecord(String datasetName, Map<String, Object> recordDataMap) {
        this.datasetName = datasetName;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        this.setRecordDataFromMap(recordDataMap);
    }

    @JsonIgnore
    public Map<String, Object> getRecordDataAsMap() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> result = mapper.readValue(this.recordData, Map.class);
            log.debug("Successfully parsed JSON data for record ID: {}", this.id);
            return result;
        } catch (JsonProcessingException e) {
            String errorMessage = String.format(
                    "Failed to parse JSON data for record ID: %d, Dataset: %s, Error: %s",
                    this.id, this.datasetName, e.getMessage()
            );
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public void setRecordDataFromMap(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Record data cannot be null or empty");
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            this.recordData = mapper.writeValueAsString(data);
            log.debug("Successfully converted Map to JSON for dataset: {}", this.datasetName);
        } catch (JsonProcessingException e) {
            String errorMessage = String.format(
                    "Failed to convert Map to JSON for dataset: %s, Map size: %d, Error: %s",
                    this.datasetName, data.size(), e.getMessage()
            );
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @JsonIgnore
    public Object getFieldValue(String fieldName) {
        Map<String, Object> dataMap = getRecordDataAsMap();
        return dataMap.get(fieldName);
    }

    @JsonIgnore
    public boolean hasField(String fieldName) {
        Map<String, Object> dataMap = getRecordDataAsMap();
        return dataMap.containsKey(fieldName) && dataMap.get(fieldName) != null;
    }

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        if (this.createdAt == null) {
            this.createdAt = now;
        }
        this.updatedAt = now;
        log.debug("Creating new record in dataset: {}", this.datasetName);
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
        log.debug("Updating record ID: {} in dataset: {}", this.id, this.datasetName);
    }
}
