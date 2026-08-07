# Azure-End-to-End-Banking-Data-Engineering-Pipeline


## 📊 Dataset & Ingestion Overview

* **Data Source:** Public REST API (`randomuser.me`) simulating continuous real-world retail banking customer account creation and transactions.
* **Volume & Batch Size:** **1,000 records per execution batch**, ingested continuously.
* **Data Format:** Raw nested JSON format.
* **Attributes Included:** Customer Demographics, Full Name, Contact Details (Email, Cell), Geographic Location (Address, City, Coordinates), Registration Timestamps, and Account UUIDs.
* **Ingestion Architecture:** Scheduled Azure Data Factory (ADF) pipeline running on a 1-hour recurrence cycle, pushing raw data directly into **Azure Data Lake Storage Gen2 (Bronze Layer)**.

* 
*
<img width="1908" height="863" alt="Screenshot 2026-08-03 121754" src="https://github.com/user-attachments/assets/7f418c44-cdf9-4d17-a6e6-70a7db8ad76d" />


* **ADF_VIDEO:

https://github.com/user-attachments/assets/d5799bd1-64df-4c3d-a98a-336567e01263





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


* ** PYSPARK_VIDEO:

https://github.com/user-attachments/assets/3ddaea4f-0550-488c-94d3-60512567d182








# Enterprise Banking Analytics Engineering & Data Transformation (dbt)

## 📋 Overview
This phase models, enriches, and validates raw Lakehouse ingestion tables (`raw_banking` and `transactions`) using dbt (data build tool). By establishing a modular three-tier DAG architecture (Staging ➔ Intermediate ➔ Marts) backed by custom SQL assertions and schema tests, the pipeline delivers audit-ready, low-latency reporting assets for executive decision-making.

---

## 🏗️ Analytics Engineering Lineage (DAG Architecture)




### 1. Source Declaration (`Source_project.yml`)
* **Functional Description:** Maps raw Delta Lake tables (`raw_banking` and `transactions`) under the `db_bankingproject.default` schema into the dbt ecosystem.
* **Business Value:** Decouples raw source systems from downstream transformation logic, enabling seamless schema tracking.

### 2. User Staging Layer (`stg_banking_users.sql`)
* **Functional Description:** Cleans raw user records, casts data types, handles null values using `coalesce` defaults, and standardizes identity fields, locations, registration metrics, login credentials, and profile image URLs.
* **Business Value:** Creates a clean, standardized baseline table of customer profiles free of missing values or formatting inconsistencies.

### 3. Transaction Staging Layer (`stg_transactions.sql`)
* **Functional Description:** Casts transaction IDs and user keys, normalizes transaction types using string trimming and lowercasing (`lower(trim(...))`), and standardizes event timestamps.
* **Business Value:** Guarantees uniform categorization across financial events (Deposits, Withdrawals, Transfers, Payments).

### 4. User Metrics Aggregation Layer (`int_users_transactions_summary.sql`)
* **Functional Description:** Joins staging tables to calculate user-level transaction statistics, including total volume, deposit/withdrawal totals, net balance estimates, activity timelines (`days_since_last_transaction`), and business rules:
  * **Account Health Status:** Categorizes users into `Highly Active`, `Dormant`, `Churned`, or `Inactive / New` based on transaction recency.
  * **Customer Value Tiering:** Segments customers into `Tier 1 (VIP)`, `Tier 2 (Gold)`, `Tier 3 (Standard)`, or `zero_balance`.
  * **Risk Overdrawn Flag:** Sets `is_overdrawn_flag` to `true` if net estimated balance falls below zero.
* **Business Value:** Centralizes complex business rules, health scoring, and financial risk flags before exposure to analytical endpoints.

### 5. Master Customer Dimension (`dim_customer_360.sql`)
* **Functional Description:** Combines staging user attributes with intermediate summary metrics to produce a unified Master Customer View (1 row per `user_id`).
* **Business Value:** Provides a 360-degree analytical dimension for executive reporting, customer retention analysis, and targeted marketing.

### 6. Transaction Fact Mart (`fct_banking_transactions.sql`)
* **Functional Description:** Builds a transaction-level fact table enriched with core customer metadata (name, email, location, age) via user key joins (1 row per `transaction_id`).
* **Business Value:** Enables granular slice-and-dice financial reporting across geographic locations, demographic segments, and event types.

---

## 🧪 Automated Data Quality & Audit Framework

### 1. Custom Singular Data Integrity Assertions (`assert_marts_data_integrity.sql`)
* **Check 1 (Net Balance Integrity):** Verifies that `estimated_net_balance` in `dim_customer_360` strictly equals `total_deposit_amount - total_withdrawal_amount`.
* **Check 2 (Non-Negative Transaction Check):** Audits `fct_banking_transactions` to flag any invalid, negative transaction monetary values.
* **Check 3 (Orphan Transactions Check):** Left joins transaction records against `dim_customer_360` to capture unmatched user accounts.
* **Business Value:** Prevents broken accounting, orphan records, or corrupted financial figures from reaching production reporting tools.

### 2. Schema Validation Tests (`Marts_Schema.yml`)
* **Primary Key Constraints:** Enforces `unique` and `not_null` rules on `user_id` and `transaction_id`.
* **Accepted Values Validation:** Enforces strict domain constraints on categorical fields, including account health statuses (`Highly Active`, `Dormant`, `Churned`, `Inactive / New`) and customer value tiers (`Tier 1 (VIP)`, `Tier 2 (Gold)`, `Tier 3 (Standard)`, `zero_balance`).


* ** DBT_VIDEO:
* 

https://github.com/user-attachments/assets/1754a7cd-bc3c-49dd-892d-54d0e7f69a0f




### 🛠️ Custom dbt Macros (Code Reusability)
- **`classify_transaction_value`**: Implemented a custom Jinja macro to dynamically categorize transactions based on monetary thresholds (`High Value`, `Medium Value`, `Low Value`).
- **DRY Principle**: Utilized reusable SQL macros across models to avoid redundant `CASE WHEN` statements and maintain code consistency.
-<img width="1737" height="510" alt="Screenshot 2026-08-07 105159" src="https://github.com/user-attachments/assets/5e395b0d-54b7-45b2-8125-8da7b1207b74" />



# CI/CD Pipeline Setup for dbt & Databricks

 GitHub Actions to enforce automated continuous integration (CI) and deployment testing for dbt transformations on Azure Databricks.

---

## ⚙️ Automated Workflow (`dbt_ci.yml`)

The CI/CD pipeline triggers automatically on every **Push** or **Pull Request** to the `main` branch. It ensures code quality, dependency resolution, and successful model builds before any changes are merged.

### Workflow Execution Steps

1. **Environment Setup**: Provisions an `ubuntu-latest` runner with Python 3.10 and installs `dbt-core` along with the `dbt-databricks` adapter.
2. **Dynamic Profile Generation**: Securely constructs a temporary `~/.dbt/profiles.yml` connection file on the runner using GitHub Encrypted Secrets.
3. **Dependency Installation**: Runs `dbt deps` to fetch required external dbt packages (e.g., `dbt_utils`).
4. **Build & Test Execution**: Executes `dbt build` to compile SQL models, run transformations, and execute data quality tests against the target Databricks SQL Warehouse.

---

## 🔒 Configured Secrets

To maintain security, database credentials are never hardcoded. The pipeline relies on the following GitHub Repository Secrets (**Settings > Secrets and variables > Actions**):

* **`DBT_HOST`**: Databricks workspace host URL (e.g., `adb-xxxxxxxx.azuredatabricks.net`).
* **`DBT_TOKEN`**: Databricks Personal Access Token (PAT) for authentication.
* **`DBT_HTTP_PATH`**: HTTP Path of the target SQL Warehouse / Compute cluster.

---

* ** CI/CD PIPELINE_VIDEO:

https://github.com/user-attachments/assets/37cabb2c-b6d0-47b1-a486-df7dfe886e8f




<img width="1901" height="676" alt="Screenshot 2026-08-07 121003" src="https://github.com/user-attachments/assets/9957b79f-2a30-48d0-8fe8-b2093a558257" />
<img width="1680" height="722" alt="Screenshot 2026-08-07 120657" src="https://github.com/user-attachments/assets/1ee8cb69-4c06-45e9-a815-571028f9ae64" />


## 🚀 Local CI Verification

To test the same pipeline commands locally prior to pushing:

```bash
# Install dependencies
dbt deps

# Compile models and run tests
dbt build














