package com.example.repository;

import com.example.entity.DatasetRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DatasetRecordRepository extends JpaRepository<DatasetRecord, Long> {

    List<DatasetRecord> findByDatasetName(String datasetName);

    List<DatasetRecord> findByDatasetNameOrderByCreatedAtDesc(String datasetName);

    long countByDatasetName(String datasetName);

    boolean existsByDatasetName(String datasetName);

    @Modifying
    @Transactional
    void deleteByDatasetName(String datasetName);

    @Query(value = """
        SELECT * FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :sortField)) IS NOT NULL
        ORDER BY JSON_EXTRACT(record_data, CONCAT('$.', :sortField)) ASC
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameSortedByJsonFieldAsc(
            @Param("datasetName") String datasetName,
            @Param("sortField") String sortField
    );

    @Query(value = """
        SELECT * FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :sortField)) IS NOT NULL
        ORDER BY JSON_EXTRACT(record_data, CONCAT('$.', :sortField)) DESC
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameSortedByJsonFieldDesc(
            @Param("datasetName") String datasetName,
            @Param("sortField") String sortField
    );

    @Query(value = """
        SELECT * FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :fieldName)) = CAST(:fieldValue AS JSON)
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameAndJsonFieldValue(
            @Param("datasetName") String datasetName,
            @Param("fieldName") String fieldName,
            @Param("fieldValue") String fieldValue
    );

    @Query(value = """
        SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(record_data, CONCAT('$.', :fieldName))) as field_value 
        FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :fieldName)) IS NOT NULL
        ORDER BY field_value
        """, nativeQuery = true)
    List<String> findDistinctJsonFieldValues(
            @Param("datasetName") String datasetName,
            @Param("fieldName") String fieldName
    );

    @Query("SELECT DISTINCT dr.datasetName FROM DatasetRecord dr ORDER BY dr.datasetName")
    List<String> findDistinctDatasetNames();

    @Query(value = """
        SELECT dataset_name, COUNT(*) as record_count,
               MIN(created_at) as first_record,
               MAX(created_at) as last_record
        FROM dataset_records 
        WHERE dataset_name = :datasetName
        GROUP BY dataset_name
        """, nativeQuery = true)
    Optional<Object[]> getDatasetStatistics(@Param("datasetName") String datasetName);

    @Query("""
        SELECT dr FROM DatasetRecord dr 
        WHERE dr.datasetName = :datasetName 
        AND dr.createdAt BETWEEN :startDate AND :endDate 
        ORDER BY dr.createdAt DESC
        """)
    List<DatasetRecord> findRecordsByDateRange(
            @Param("datasetName") String datasetName,
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate
    );

    @Query("""
        SELECT dr FROM DatasetRecord dr 
        WHERE dr.datasetName = :datasetName 
        AND dr.createdAt >= :since 
        ORDER BY dr.createdAt DESC
        """)
    List<DatasetRecord> findRecentRecords(
            @Param("datasetName") String datasetName,
            @Param("since") LocalDateTime since
    );

    @Modifying
    @Transactional
    @Query("DELETE FROM DatasetRecord dr WHERE dr.datasetName = :datasetName AND dr.createdAt < :cutoffDate")
    int deleteOldRecords(
            @Param("datasetName") String datasetName,
            @Param("cutoffDate") LocalDateTime cutoffDate
    );

    @Query(value = """
        SELECT * FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :fieldName)) LIKE CONCAT('%', :searchTerm, '%')
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameAndJsonFieldContains(
            @Param("datasetName") String datasetName,
            @Param("fieldName") String fieldName,
            @Param("searchTerm") String searchTerm
    );

    @Query(value = """
        SELECT * FROM dataset_records 
        WHERE dataset_name = :datasetName 
        AND JSON_EXTRACT(record_data, CONCAT('$.', :fieldName)) BETWEEN :minValue AND :maxValue
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameAndJsonFieldInRange(
            @Param("datasetName") String datasetName,
            @Param("fieldName") String fieldName,
            @Param("minValue") Number minValue,
            @Param("maxValue") Number maxValue
    );
}
