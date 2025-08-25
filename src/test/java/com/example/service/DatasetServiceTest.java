package com.example.service;

import com.example.entity.DatasetRecord;
import com.example.repository.DatasetRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatasetServiceTest {

    @Mock
    private DatasetRecordRepository repository;

    @InjectMocks
    private DatasetServiceImpl datasetService;

    private Map<String, Object> sampleData;
    private DatasetRecord sampleRecord;

    @BeforeEach
    void setUp() {
        sampleData = Map.of(
                "id", 1,
                "name", "John Doe",
                "age", 30,
                "department", "Engineering"
        );

        sampleRecord = DatasetRecord.builder()
                .id(1L)
                .datasetName("test_dataset")
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    // INSERT RECORD TESTS
    @Test
    void insertRecord_WithValidData_ShouldReturnSavedRecord() {
        // Given
        when(repository.save(any(DatasetRecord.class))).thenReturn(sampleRecord);

        // When
        DatasetRecord result = datasetService.insertRecord("test_dataset", sampleData);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(1L);
        verify(repository).save(any(DatasetRecord.class));
    }

    @Test
        // Suppress null parameter warning
    void insertRecord_WithNullDatasetName_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecord(null, sampleData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dataset name cannot be null or empty");

        verify(repository, never()).save(any());
    }

    @Test
    void insertRecord_WithEmptyDatasetName_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecord("", sampleData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dataset name cannot be null or empty");
    }

    @Test
        // Suppress null parameter warning
    void insertRecord_WithNullData_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecord("test", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Record data cannot be null or empty");
    }

    @Test
    void insertRecord_WithEmptyData_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecord("test", Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Record data cannot be null or empty");
    }


    // BATCH INSERT TESTS
    @Test
    void insertRecords_WithValidData_ShouldReturnSavedRecords() {
        // Given
        List<Map<String, Object>> recordsData = Arrays.asList(
                Map.of("name", "John", "age", 30),
                Map.of("name", "Jane", "age", 25)
        );

        List<DatasetRecord> savedRecords = Arrays.asList(sampleRecord, sampleRecord);
        when(repository.saveAll(any())).thenReturn(savedRecords);

        // When
        List<DatasetRecord> result = datasetService.insertRecords("test", recordsData);

        // Then
        assertThat(result).hasSize(2);
        verify(repository).saveAll(any());
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // Suppress null parameter warning
    void insertRecords_WithNullData_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecords("test", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Records data cannot be null or empty");
    }

    @Test
    void insertRecords_WithEmptyData_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.insertRecords("test", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Records data cannot be null or empty");
    }


    // QUERY TESTS
    @Test
    void groupRecordsByField_WithValidField_ShouldReturnGroupedRecords() {
        // Given
        DatasetRecord record1 = createRecordWithData(Map.of("department", "Engineering", "name", "John"));
        DatasetRecord record2 = createRecordWithData(Map.of("department", "Marketing", "name", "Jane"));

        when(repository.findByDatasetName("test")).thenReturn(Arrays.asList(record1, record2));

        // When
        Map<String, List<Map<String, Object>>> result =
                datasetService.groupRecordsByField("test", "department");

        // Then
        assertThat(result).hasSize(2);
        assertThat(result.get("Engineering")).hasSize(1);
        assertThat(result.get("Marketing")).hasSize(1);
    }

    @Test
        // Suppress null parameter warning
    void groupRecordsByField_WithNullDataset_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.groupRecordsByField(null, "department"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dataset name cannot be null or empty");
    }

    @Test
        // Suppress null parameter warning
    void groupRecordsByField_WithNullField_ShouldThrowException() {
        // When & Then
        assertThatThrownBy(() -> datasetService.groupRecordsByField("test", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field name cannot be null or empty");
    }

    @Test
    void sortRecordsByField_WithValidField_ShouldReturnSortedRecords() {
        // Given
        when(repository.findByDatasetNameSortedByJsonFieldAsc("test", "age"))
                .thenReturn(Collections.singletonList(sampleRecord));

        // When
        List<Map<String, Object>> result =
                datasetService.sortRecordsByField("test", "age", "asc");

        // Then
        assertThat(result).isNotNull();
        verify(repository).findByDatasetNameSortedByJsonFieldAsc("test", "age");
    }

    @Test
    void getAllRecords_WithValidDataset_ShouldReturnAllRecords() {
        // Given
        when(repository.findByDatasetName("test")).thenReturn(Collections.singletonList(sampleRecord));

        // When
        List<Map<String, Object>> result = datasetService.getAllRecords("test");

        // Then
        assertThat(result).isNotNull();
        verify(repository).findByDatasetName("test");
    }

    @Test
    void getDatasetStats_WithValidDataset_ShouldReturnStats() {
        // Given
        when(repository.countByDatasetName("test")).thenReturn(5L);
        when(repository.findByDatasetName("test")).thenReturn(Collections.singletonList(sampleRecord));

        // When
        Map<String, Object> result = datasetService.getDatasetStats("test");

        // Then
        assertThat(result).containsKey("dataset");
        assertThat(result).containsKey("totalRecords");
        assertThat(result.get("totalRecords")).isEqualTo(5L);
    }


    // HELPER METHODS

    private DatasetRecord createRecordWithData(Map<String, Object> data) {
        DatasetRecord record = new DatasetRecord();
        record.setDatasetName("test");
        record.setRecordDataFromMap(data);
        return record;
    }
}
