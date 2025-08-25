package com.example.controller;

import com.example.entity.DatasetRecord;
import com.example.service.DatasetService; // ← Use interface, NOT implementation
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dataset")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
@Slf4j
public class DatasetController {

    private final DatasetService datasetService; // ← FIXED: Use interface

    @PostMapping("/{datasetName}/record")
    public ResponseEntity<Map<String, Object>> insertRecord(
            @PathVariable String datasetName,
            @RequestBody Map<String, Object> recordData) {

        try {
            DatasetRecord savedRecord = datasetService.insertRecord(datasetName, recordData);

            Map<String, Object> response = new HashMap<>();
            response.put("message", "Record added successfully");
            response.put("dataset", datasetName);
            response.put("recordId", savedRecord.getId());
            response.put("timestamp", savedRecord.getCreatedAt());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (IllegalArgumentException e) {
            log.error("Validation error for dataset: {}", datasetName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validation failed");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);

        } catch (Exception e) {
            log.error("Failed to insert record for dataset: {}", datasetName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to insert record");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    @GetMapping("/{datasetName}/query")
    public ResponseEntity<Map<String, Object>> queryRecords(
            @PathVariable String datasetName,
            @RequestParam(required = false) String groupBy,
            @RequestParam(required = false) String sortBy,
            @RequestParam(required = false, defaultValue = "asc") String order) {

        try {
            Map<String, Object> response = new HashMap<>();

            if (groupBy != null && !groupBy.trim().isEmpty()) {
                // Group-by operation
                Map<String, List<Map<String, Object>>> groupedRecords =
                        datasetService.groupRecordsByField(datasetName, groupBy);
                response.put("groupedRecords", groupedRecords);
                response.put("operation", "groupBy");
                response.put("field", groupBy);

            } else if (sortBy != null && !sortBy.trim().isEmpty()) {
                // Sort-by operation
                List<Map<String, Object>> sortedRecords =
                        datasetService.sortRecordsByField(datasetName, sortBy, order);
                response.put("sortedRecords", sortedRecords);
                response.put("operation", "sortBy");
                response.put("field", sortBy);
                response.put("order", order);

            } else {
                // Return all records
                List<Map<String, Object>> allRecords =
                        datasetService.getAllRecords(datasetName);
                response.put("records", allRecords);
                response.put("operation", "getAll");
            }

            response.put("dataset", datasetName);
            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.error("Validation error for query on dataset: {}", datasetName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validation failed");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);

        } catch (Exception e) {
            log.error("Failed to query dataset: {}", datasetName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to query records");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/{datasetName}/info")
    public ResponseEntity<Map<String, Object>> getDatasetInfo(@PathVariable String datasetName) {
        try {
            Map<String, Object> stats = datasetService.getDatasetStats(datasetName);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Failed to get info for dataset: {}", datasetName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to get dataset info");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/list")
    public ResponseEntity<Map<String, Object>> getAllDatasets() {
        try {
            List<Map<String, Object>> datasets = datasetService.getAllDatasets();
            Map<String, Object> response = new HashMap<>();
            response.put("datasets", datasets);
            response.put("count", datasets.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to get datasets list", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to get datasets");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
