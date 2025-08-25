package com.example.service;

import com.example.entity.DatasetRecord;
import com.example.repository.DatasetRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
@RequiredArgsConstructor
@Slf4j
public class DatasetServiceImpl implements DatasetService{

    private final DatasetRecordRepository repository;

    // Insert single record
    public DatasetRecord insertRecord(String datasetName, Map<String, Object> recordData) {
        log.info("Starting record insertion for dataset: {}", datasetName);

        // Validation
        validateInput(datasetName, recordData);

        try {
            // Create record using Lombok builder
            DatasetRecord record = DatasetRecord.builder()
                    .datasetName(datasetName.trim().toLowerCase())
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();

            record.setRecordDataFromMap(recordData);

            DatasetRecord savedRecord = repository.save(record);

            log.info("Successfully inserted record ID: {} into dataset: {} with {} fields",
                    savedRecord.getId(), datasetName, recordData.size());

            return savedRecord;

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Failed to insert record into dataset: %s, Record data size: %d fields, Error: %s",
                    datasetName, recordData.size(), e.getMessage()
            );
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    // Batch insert records
    public List<DatasetRecord> insertRecords(String datasetName, List<Map<String, Object>> recordsData) {
        log.info("Starting batch insert of {} records for dataset: {}",
                recordsData != null ? recordsData.size() : 0, datasetName);

        if (recordsData == null || recordsData.isEmpty()) { // ← FIXED: Check null first
            throw new IllegalArgumentException("Records data cannot be null or empty");
        }

        try {
            List<DatasetRecord> records = recordsData.stream()
                    .map(data -> {
                        DatasetRecord record = DatasetRecord.builder()
                                .datasetName(datasetName.trim().toLowerCase())
                                .build();
                        record.setRecordDataFromMap(data);
                        return record;
                    })
                    .collect(Collectors.toList());

            List<DatasetRecord> savedRecords = repository.saveAll(records);

            log.info("Successfully batch inserted {} records into dataset: {}",
                    savedRecords.size(), datasetName);

            return savedRecords;

        } catch (Exception e) {
            log.error("Batch insert failed for dataset: {}", datasetName, e);
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }
    // Group records by field
    public Map<String, List<Map<String, Object>>> groupRecordsByField(String datasetName, String groupByField) {
        log.info("Grouping records by field '{}' for dataset: {}", groupByField, datasetName);

        validateDatasetName(datasetName);
        validateFieldName(groupByField);

        try {
            List<DatasetRecord> records = repository.findByDatasetName(datasetName.toLowerCase());

            if (records.isEmpty()) {
                log.warn("No records found for dataset: {}", datasetName);
                return new LinkedHashMap<>();
            }

            Map<String, List<Map<String, Object>>> groupedRecords = records.stream()
                    .map(DatasetRecord::getRecordDataAsMap)
                    .filter(data -> data.containsKey(groupByField))
                    .collect(Collectors.groupingBy(
                            data -> {
                                Object value = data.get(groupByField);
                                return value != null ? String.valueOf(value) : "null";
                            },
                            LinkedHashMap::new,
                            Collectors.toList()
                    ));

            log.info("Successfully grouped {} records into {} groups for dataset: {}",
                    records.size(), groupedRecords.size(), datasetName);

            return groupedRecords;

        } catch (Exception e) {
            log.error("Grouping failed for dataset: {} by field: {}", datasetName, groupByField, e);
            throw new RuntimeException("Grouping operation failed: " + e.getMessage(), e);
        }
    }

    // Sort records by field
    public List<Map<String, Object>> sortRecordsByField(String datasetName, String sortByField, String order) {
        log.info("Sorting records by field '{}' ({}) for dataset: {}", sortByField, order, datasetName);

        validateDatasetName(datasetName);
        validateFieldName(sortByField);

        try {
            List<DatasetRecord> records;

            if ("desc".equalsIgnoreCase(order) || "descending".equalsIgnoreCase(order)) {
                records = repository.findByDatasetNameSortedByJsonFieldDesc(datasetName.toLowerCase(), sortByField);
            } else {
                records = repository.findByDatasetNameSortedByJsonFieldAsc(datasetName.toLowerCase(), sortByField);
            }

            if (records == null || records.isEmpty()) { // ← FIXED: Check for empty
                return Collections.emptyList();
            }

            List<Map<String, Object>> result = records.stream()
                    .filter(record -> record.getRecordData() != null) // ← FIXED: Filter null JSON
                    .map(DatasetRecord::getRecordDataAsMap)
                    .filter(data -> data.containsKey(sortByField))
                    .collect(Collectors.toList());

            log.info("Successfully sorted {} records by field '{}' for dataset: {}",
                    result.size(), sortByField, datasetName);

            return result;

        } catch (Exception e) {
            log.error("Sorting failed for dataset: {} by field: {}", datasetName, sortByField, e);
            throw new RuntimeException("Sorting operation failed: " + e.getMessage(), e);
        }
    }

    // Get all records
    public List<Map<String, Object>> getAllRecords(String datasetName) {
        log.info("Retrieving all records for dataset: {}", datasetName);

        validateDatasetName(datasetName);

        try {
            List<DatasetRecord> records = repository.findByDatasetName(datasetName.toLowerCase());
            if (records == null || records.isEmpty()) { // ← FIXED: Check for empty
                return Collections.emptyList();
            }

            List<Map<String, Object>> result = records.stream()
                    .filter(record -> record.getRecordData() != null) // ← FIXED: Filter null JSON
                    .map(DatasetRecord::getRecordDataAsMap)
                    .collect(Collectors.toList());

            log.info("Retrieved {} records for dataset: {}", result.size(), datasetName);
            return result;

        } catch (Exception e) {
            log.error("Failed to retrieve records for dataset: {}", datasetName, e);
            throw new RuntimeException("Failed to retrieve records: " + e.getMessage(), e);
        }
    }

    // Get dataset statistics
    public Map<String, Object> getDatasetStats(String datasetName) {
        log.info("Generating statistics for dataset: {}", datasetName);

        validateDatasetName(datasetName);

        try {
            long totalRecords = repository.countByDatasetName(datasetName.toLowerCase());
            Map<String, Object> stats = new LinkedHashMap<>();

            stats.put("dataset", datasetName);
            stats.put("totalRecords", totalRecords);
            stats.put("exists", totalRecords > 0);

            if (totalRecords > 0) {
                List<DatasetRecord> sampleRecords = repository.findByDatasetName(datasetName.toLowerCase())
                        .stream()
                        .filter(record -> record.getRecordData() != null) // ← FIXED: Filter null JSON
                        .limit(100)
                        .collect(Collectors.toList());

                if (!sampleRecords.isEmpty()) { // ← FIXED: Check if filtered list is empty
                    Set<String> allFields = new HashSet<>();
                    Map<String, Set<String>> fieldTypes = new HashMap<>();

                    for (DatasetRecord record : sampleRecords) {
                        Map<String, Object> dataMap = record.getRecordDataAsMap();
                        allFields.addAll(dataMap.keySet());

                        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                            String field = entry.getKey();
                            Object value = entry.getValue();
                            String type = value != null ? value.getClass().getSimpleName() : "null";
                            fieldTypes.computeIfAbsent(field, k -> new HashSet<>()).add(type);
                        }
                    }

                    stats.put("availableFields", allFields);
                    stats.put("fieldCount", allFields.size());
                    stats.put("fieldTypes", fieldTypes);
                }
            }

            log.info("Generated comprehensive stats for dataset: {} ({} records)",
                    datasetName, totalRecords);

            return stats;

        } catch (Exception e) {
            log.error("Failed to generate stats for dataset: {}", datasetName, e);
            throw new RuntimeException("Failed to generate dataset statistics: " + e.getMessage(), e);
        }
    }

    // Get all datasets
    public List<Map<String, Object>> getAllDatasets() {
        log.info("Retrieving all datasets with metadata");

        try {
            List<String> datasetNames = repository.findDistinctDatasetNames();

            List<Map<String, Object>> datasets = datasetNames.stream()
                    .map(name -> {
                        Map<String, Object> info = new LinkedHashMap<>();
                        info.put("name", name);
                        info.put("recordCount", repository.countByDatasetName(name));
                        return info;
                    })
                    .collect(Collectors.toList());

            log.info("Retrieved {} datasets", datasets.size());
            return datasets;

        } catch (Exception e) {
            log.error("Failed to retrieve datasets list", e);
            throw new RuntimeException("Failed to retrieve datasets: " + e.getMessage(), e);
        }
    }

    // Validation methods
    private void validateInput(String datasetName, Map<String, Object> recordData) {
        if (datasetName == null || !StringUtils.hasText(datasetName.trim())) {
            throw new IllegalArgumentException("Dataset name cannot be null or empty");
        }

        if (datasetName.trim().length() > 100) {
            throw new IllegalArgumentException("Dataset name must be less than 100 characters");
        }

        if (recordData == null || recordData.isEmpty()) {
            throw new IllegalArgumentException("Record data cannot be null or empty");
        }
    }

    private void validateDatasetName(String datasetName) {
        if (datasetName == null || !StringUtils.hasText(datasetName.trim())) {
            throw new IllegalArgumentException("Dataset name cannot be null or empty");
        }
    }

    private void validateFieldName(String fieldName) {
        if (!StringUtils.hasText(fieldName)) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
    }
}
