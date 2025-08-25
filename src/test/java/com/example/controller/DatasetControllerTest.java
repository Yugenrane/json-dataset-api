package com.example.controller;

import com.example.entity.DatasetRecord;
import com.example.service.DatasetService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(DatasetController.class)
class DatasetControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean  // ← CHANGED: Use @MockitoBean instead of @MockBean
    private DatasetService datasetService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void insertRecord_WithValidRequest_ShouldReturn201() throws Exception {
        // Given
        Map<String, Object> recordData = Map.of("name", "John Doe", "age", 30);

        DatasetRecord savedRecord = DatasetRecord.builder()
                .id(1L)
                .datasetName("test_dataset")
                .createdAt(LocalDateTime.now())
                .build();

        when(datasetService.insertRecord(eq("test_dataset"), any())).thenReturn(savedRecord);

        // When & Then
        mockMvc.perform(post("/api/dataset/test_dataset/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(recordData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").value("Record added successfully"))
                .andExpect(jsonPath("$.recordId").value(1))
                .andExpect(jsonPath("$.dataset").value("test_dataset"));
    }

    @Test
    void queryRecords_WithGroupBy_ShouldReturnGroupedData() throws Exception {
        // Given
        Map<String, List<Map<String, Object>>> groupedData = Map.of(
                "Engineering", List.of(Map.of("name", "John", "department", "Engineering"))
        );

        when(datasetService.groupRecordsByField("test_dataset", "department"))
                .thenReturn(groupedData);

        // When & Then
        mockMvc.perform(get("/api/dataset/test_dataset/query")
                        .param("groupBy", "department"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupedRecords").exists())
                .andExpect(jsonPath("$.operation").value("groupBy"))
                .andExpect(jsonPath("$.field").value("department"));
    }

    @Test
    void queryRecords_WithSortBy_ShouldReturnSortedData() throws Exception {
        // Given
        List<Map<String, Object>> sortedData = List.of(
                Map.of("name", "John", "age", 25),
                Map.of("name", "Jane", "age", 30)
        );

        when(datasetService.sortRecordsByField("test_dataset", "age", "asc"))
                .thenReturn(sortedData);

        // When & Then
        mockMvc.perform(get("/api/dataset/test_dataset/query")
                        .param("sortBy", "age")
                        .param("order", "asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sortedRecords").exists())
                .andExpect(jsonPath("$.operation").value("sortBy"));
    }

    @Test
    void queryRecords_WithNoParams_ShouldReturnAllRecords() throws Exception {
        // Given
        List<Map<String, Object>> allRecords = List.of(
                Map.of("name", "John", "age", 30)
        );

        when(datasetService.getAllRecords("test_dataset")).thenReturn(allRecords);

        // When & Then
        mockMvc.perform(get("/api/dataset/test_dataset/query"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records").exists())
                .andExpect(jsonPath("$.operation").value("getAll"));
    }

    @Test
    void getDatasetInfo_WithValidDataset_ShouldReturnInfo() throws Exception {
        // Given
        Map<String, Object> stats = Map.of(
                "dataset", "test_dataset",
                "totalRecords", 5,
                "exists", true
        );

        when(datasetService.getDatasetStats("test_dataset")).thenReturn(stats);

        // When & Then
        mockMvc.perform(get("/api/dataset/test_dataset/info"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.dataset").value("test_dataset"))
                .andExpect(jsonPath("$.totalRecords").value(5));
    }

    @Test
    void getAllDatasets_ShouldReturnDatasetsList() throws Exception {
        // Given
        List<Map<String, Object>> datasets = List.of(
                Map.of("name", "dataset1", "recordCount", 10),
                Map.of("name", "dataset2", "recordCount", 5)
        );

        when(datasetService.getAllDatasets()).thenReturn(datasets);

        // When & Then
        mockMvc.perform(get("/api/dataset/list"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.datasets").isArray())
                .andExpect(jsonPath("$.count").value(2));
    }

    @Test
    void insertRecord_WithServiceException_ShouldReturn500() throws Exception {
        // Given
        Map<String, Object> recordData = Map.of("name", "John");

        when(datasetService.insertRecord(any(), any()))
                .thenThrow(new RuntimeException("Database error"));

        // When & Then
        mockMvc.perform(post("/api/dataset/test/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(recordData)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Failed to insert record"));
    }
}
