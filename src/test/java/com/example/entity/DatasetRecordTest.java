package com.example.entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class DatasetRecordTest {

    private DatasetRecord datasetRecord;
    private Map<String, Object> testData;

    @BeforeEach
    void setUp() {
        testData = Map.of(
                "name", "John Doe",
                "age", 30,
                "department", "Engineering"
        );

        datasetRecord = DatasetRecord.builder()
                .datasetName("test_dataset")
                .build();
    }

    @Test
    void setRecordDataFromMap_WithValidData_ShouldSetJsonData() {
        // When
        datasetRecord.setRecordDataFromMap(testData);

        // Then
        assertThat(datasetRecord.getRecordData()).isNotNull();
        assertThat(datasetRecord.getRecordData()).contains("John Doe");
    }

    @Test
    void getRecordDataAsMap_WithValidJson_ShouldReturnMap() {
        // Given
        datasetRecord.setRecordDataFromMap(testData);

        // When
        Map<String, Object> result = datasetRecord.getRecordDataAsMap();

        // Then
        assertThat(result).isNotNull();
        assertThat(result.get("name")).isEqualTo("John Doe");
        assertThat(result.get("age")).isEqualTo(30);
    }

    @Test
    void setRecordDataFromMap_WithNullData_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetRecord.setRecordDataFromMap(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Record data cannot be null or empty");
    }

    @Test
    void hasField_WithExistingField_ShouldReturnTrue() {
        // Given
        datasetRecord.setRecordDataFromMap(testData);

        // When & Then
        assertThat(datasetRecord.hasField("name")).isTrue();
        assertThat(datasetRecord.hasField("nonexistent")).isFalse();
    }

    @Test
    void getFieldValue_WithExistingField_ShouldReturnValue() {
        // Given
        datasetRecord.setRecordDataFromMap(testData);

        // When & Then
        assertThat(datasetRecord.getFieldValue("name")).isEqualTo("John Doe");
        assertThat(datasetRecord.getFieldValue("age")).isEqualTo(30);
    }
}
