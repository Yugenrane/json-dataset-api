package com.example.service;

import com.example.entity.DatasetRecord;

import java.util.List;
import java.util.Map;

public interface DatasetService {
    DatasetRecord insertRecord(String datasetName, Map<String, Object> recordData);
    List<DatasetRecord> insertRecords(String datasetName, List<Map<String, Object>> recordsData);
    Map<String, List<Map<String, Object>>> groupRecordsByField(String datasetName, String groupByField);
    List<Map<String, Object>> sortRecordsByField(String datasetName, String sortByField, String order);
    List<Map<String, Object>> getAllRecords(String datasetName);
    Map<String, Object> getDatasetStats(String datasetName);
    List<Map<String, Object>> getAllDatasets();
}
