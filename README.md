# JSON Dataset API

## 📌 Project Overview

The **JSON Dataset API** is a Spring Boot application that provides RESTful endpoints for managing datasets stored in a relational database. Each dataset can hold multiple records in JSON format, and the API supports various operations like insertion, grouping, sorting, and retrieving statistics.

---

## 🚀 Features

* Insert single or multiple records into a dataset.
* Retrieve all records from a dataset.
* Group records by a specific field.
* Sort records by a specific field in ascending/descending order.
* Retrieve statistics about a dataset (e.g., total records).
* List all datasets with their records.

---

## 🏗️ Tech Stack

* **Java 17**
* **Spring Boot 3.3.5**
* **Spring Data JPA**
* **MySQL**
* **Maven**

---

## 📂 Project Structure

```
com.example
│
├── controller
│   └── DatasetController.java   # REST endpoints
│
├── entity
│   └── DatasetRecord.java       # Entity representing a dataset record
│
├── repository
│   └── DatasetRecordRepository.java # JPA Repository
│
├── service
│   ├── DatasetService.java      # Service Interface
│   └── DatasetServiceImpl.java  # Service Implementation
│
├── JsonDatasetApiApplication.java # Main Spring Boot application
└── JsonDatasetApiApplicationTests.java # Test class
```

---

## ⚙️ Configuration

The application uses **MySQL** as the database. Update the `application.yml` with your database credentials:

```yaml
spring:
  application:
    name: json-dataset-api

  datasource:
    url: jdbc:mysql://localhost:3306/dataset_db
    username: root
    password: your_password
    driver-class-name: com.mysql.cj.jdbc.Driver

  jpa:
    database-platform: org.hibernate.dialect.MySQL8Dialect
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        format_sql: true

server:
  port: 8080
```

---

## ▶️ Running the Application

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/json-dataset-api.git
   cd json-dataset-api
   ```
2. Configure MySQL and update `application.yml`.
3. Build and run the application:

   ```bash
   mvn spring-boot:run
   ```
4. Access API at: [http://localhost:8080](http://localhost:8080)

---

## 📌 API Endpoints

### Insert a Record

```http
POST /datasets/{datasetName}/records
```

**Request Body:**

```json
{
  "name": "John",
  "age": 25,
  "city": "Mumbai"
}
```

### Insert Multiple Records

```http
POST /datasets/{datasetName}/records/batch
```

**Request Body:**

```json
[
  { "name": "Alice", "age": 30 },
  { "name": "Bob", "age": 28 }
]
```

### Get All Records

```http
GET /datasets/{datasetName}/records
```

### Group Records by Field

```http
GET /datasets/{datasetName}/group?field=age
```

### Sort Records by Field

```http
GET /datasets/{datasetName}/sort?field=age&order=asc
```

### Get Dataset Statistics

```http
GET /datasets/{datasetName}/stats
```

### Get All Datasets

```http
GET /datasets
```

---

## ✅ Testing

Run tests with:

```bash
mvn test
```

---

