# Azure-End-to-End-Banking-Data-Engineering-Pipeline


## 📊 Dataset & Ingestion Overview

* **Data Source:** Public REST API (`randomuser.me`) simulating continuous real-world retail banking customer account creation and transactions.
* **Volume & Batch Size:** **1,000 records per execution batch**, ingested continuously.
* **Data Format:** Raw nested JSON format.
* **Attributes Included:** Customer Demographics, Full Name, Contact Details (Email, Cell), Geographic Location (Address, City, Coordinates), Registration Timestamps, and Account UUIDs.
* **Ingestion Architecture:** Scheduled Azure Data Factory (ADF) pipeline running on a 1-hour recurrence cycle, pushing raw data directly into **Azure Data Lake Storage Gen2 (Bronze Layer)**.

* 
*

ADF_pipeline_running_VIDEO(
"https://github.com/user-attachments/assets/5f956e90-922a-4ab5-b8f8-f30453e97795"
)
<img width="1908" height="863" alt="Screenshot 2026-08-03 121754" src="https://github.com/user-attachments/assets/7f418c44-cdf9-4d17-a6e6-70a7db8ad76d" />




#  Data Ingestion & Processing Phase (PySpark & Databricks)

## 📋 Executive Summary
This phase of the enterprise data pipeline focuses on ingesting raw, complex customer data payloads from cloud storage, unnesting multi-layered JSON structures, and building a high-volume transactional simulation engine using PySpark on Databricks. The resulting cleansed tables are persisted into Delta Lake, creating a reliable, highly optimized foundation for downstream analytics engineering and reporting.

---

## 🏗️ Technical Architecture & Data Flow


### 1. Cloud Storage Connection & Ingestion
* **Functional Description:** Configures secure access keys to connect PySpark directly to Azure Data Lake Storage (ADLS) Bronze containers (`abfss://bronze@...`) and inspects raw landing files.
* **Business Value:** Enables seamless, encrypted data ingestion from enterprise cloud storage without exposing sensitive system credentials.

### 2. Raw JSON Schema Processing
* **Functional Description:** Reads complex raw JSON files containing multi-level nested customer data and infers underlying schemas dynamically.
* **Business Value:** Guarantees reliable data ingestion even when raw source payloads contain varying record structures.

### 3. Array Unpacking & Record Exploding
* **Functional Description:** Utilizes `explode_outer` transformation to unnest deeply embedded JSON arrays (`results`) into individual row items without losing parent metadata (`info`).
* **Business Value:** Prevents record loss during extraction and converts raw nested API structures into usable row formats.

### 4. Struct Flattening & Multi-Level Extraction
* **Functional Description:** Unpacks high-level nested structs (`info.*` and `result.*`) into flat columns across the entire dataset.
* **Business Value:** Transforms complex JSON data into a clean, flat table format easily readable by business users and downstream systems.

### 5. Attribute Normalization & Column Cleaning
* **Functional Description:** Standardizes individual user attributes—including national IDs, geographic coordinates, full addresses, login parameters (MD5, Salt), profile pictures, registration timelines, and DOB metadata—using `.withColumn()` and drops redundant struct containers.
* **Business Value:** Normalizes demographics, location data, and customer profiles into standardized business fields ready for audit and risk analysis.

### 6. Synthetic Transaction Simulation Engine
* **Functional Description:** Extracts active customer identifiers (`login_uuid`) and programmatically generates thousands of realistic financial transactions (Deposits, Withdrawals, Transfers, Payments) with randomized monetary amounts and timestamps.
* **Business Value:** Simulates real-world banking traffic to benchmark system performance, validate balance tracking logic, and prepare for fraud detection modeling.

### 7. Delta Lake Transaction Table Persistence
* **Functional Description:** Converts generated financial records into a PySpark DataFrame and writes the dataset as an optimized Delta table (`db_bankingproject.default.transactions`).
* **Business Value:** Creates a durable, high-performance transaction ledger stored in enterprise Lakehouse format.

### 8. Cleansed Master Data Persistence
* **Functional Description:** Writes the fully normalized and flattened customer master table into Delta Lake (`db_bankingproject.default.raw_banking`).
* **Business Value:** Establishes a clean "Single Source of Truth" base table ready for dbt transformation models and executive reporting layers.





