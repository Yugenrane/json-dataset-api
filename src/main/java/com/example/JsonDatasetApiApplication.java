package com.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class JsonDatasetApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(JsonDatasetApiApplication.class, args);

        log.info("=".repeat(60));
        log.info("🚀 JSON Dataset API is running successfully!");
        log.info("🌐 Base URL: http://localhost:8080");
        log.info("📚 API Endpoints:");
        log.info("   POST /api/dataset/{name}/record - Insert Record");
        log.info("   GET  /api/dataset/{name}/query  - Query Records");
        log.info("   GET  /api/dataset/list          - List Datasets");
        log.info("🔧 Spring Boot: 3.5.5");
        log.info("☕ Java: 17");
        log.info("🗄️  Database: MySQL with JSON Support");
        log.info("=".repeat(60));
    }
}
