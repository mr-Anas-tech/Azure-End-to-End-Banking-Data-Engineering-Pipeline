# Enterprise Banking Data Engineering & Analytics Platform

[ REST API (Source) ]
│
▼ (Hourly Trigger)
[ Azure Data Factory ]
│
▼
[ ADLS Gen2 - Bronze Layer ]
│
▼ (PySpark - Flatten & Clean)
[ Azure Databricks - Silver Layer ]
│
▼ (dbt - Data Modeling & Quality Tests)
[ Azure Databricks - Gold Layer ]
│
├─────────────────────────────────────────┐
▼                                         ▼
[ Azure Synapse Analytics ]             [ Python Analytics Script ]
│                                         │
└────────────────────┬────────────────────┘
▼
[ Power BI Dashboard ]



## 🛠️ Tools Used & Core Function

| Tool / Technology | Purpose & Role in Pipeline |
| :--- | :--- |
| **Azure Data Factory (ADF)** | Automated hourly REST API ingestion and pipeline orchestration. |
| **Azure Data Lake Storage Gen2** | Multi-tier Medallion architecture storage (`Bronze`, `Silver`, `Gold`). |
| **Azure Databricks & PySpark** | Large-scale JSON parsing, array flattening, and synthetic transactions simulation. |
| **dbt (data build tool)** | Modular SQL modeling (`Staging` $\rightarrow$ `Intermediate` $\rightarrow$ `Marts`) and quality testing. |
| **Azure Synapse Analytics** | Serverless SQL engine using Managed Identity for zero-cost storage querying. |
| **GitHub Actions** | CI/CD automation for dbt compilation, test execution, and Databricks notebook sync. |
| **Azure Key Vault & Entra ID** | Zero-trust authentication, secret scopes, and dynamic MSAL token access. |
| **Python (Pandas, PyODBC)** | Statistical risk profiling and secure database integration. |
| **Power BI** | Executive reporting dashboard for processed volume ($12M), liquidity, and demographic KPIs. |                


## 📊 Dataset & Ingestion Overview

* **Data Source:** Public REST API (`randomuser.me`) simulating continuous real-world retail banking customer account creation and transactions.
* **Volume & Batch Size:** **1,000 records per execution batch**, ingested continuously.
* **Data Format:** Raw nested JSON format.
* **Attributes Included:** Customer Demographics, Full Name, Contact Details (Email, Cell), Geographic Location (Address, City, Coordinates), Registration Timestamps, and Account UUIDs.
* **Ingestion Architecture:** Scheduled Azure Data Factory (ADF) pipeline running on a 1-hour recurrence cycle, pushing raw data directly into **Azure Data Lake Storage Gen2 (Bronze Layer)**.

* 
<img width="1905" height="732" alt="Screenshot 2026-08-09 102256" src="https://github.com/user-attachments/assets/db944659-6569-42ea-a8ff-5dd43675a40b" />

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
- 
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

* ---

### 2. dbt Transformation Pipeline (`README_dbt_Transformation.md`)

# dbt Orchestration

---

## Orchestration & Data Quality

* **Target Environment**: Deployed against cloud data warehouses with automated compilation and run targets.

---

<img width="1832" height="901" alt="Screenshot 2026-08-08 110932" src="https://github.com/user-attachments/assets/18438670-2b79-4f08-b68e-16dab19b9e91" />


* ** CI/CD PIPELINE_VIDEO:

https://github.com/user-attachments/assets/37cabb2c-b6d0-47b1-a486-df7dfe886e8f




<img width="1901" height="676" alt="Screenshot 2026-08-07 121003" src="https://github.com/user-attachments/assets/9957b79f-2a30-48d0-8fe8-b2093a558257" />


<img width="1680" height="722" alt="Screenshot 2026-08-07 120657" src="https://github.com/user-attachments/assets/1ee8cb69-4c06-45e9-a815-571028f9ae64" />


## 🚀 Local CI Verification

To test the same pipeline commands locally prior to pushing:
# bash
# Install dependencies
dbt deps

# Compile models and run tests
dbt build 



# PySpark Data Lakehouse Movement & Databricks Orchestration

## Overview
This workflow governs the data movement across the Data Lakehouse storage tiers (**Silver** and **Gold**) using PySpark on Azure Databricks, configured with automated job triggers for scheduled execution.

---

## Data Layer Architecture

* **Silver Layer (Staging & Intermediate)**: Receives cleaned operational data, normalized schemas, and intermediate joins from upstream processes.
* **Gold Layer (Data Marts)**: Stores final aggregated business metrics, transactional facts (`fct_`), and dimension models ready for analytical querying and reporting.

---

## Execution & Workflow Mechanics

1. **Storage Tiering**:
   * Intermediate transformed datasets are persisted into the `silver` storage container.
   * Analytical business marts are formatted and written directly to the `gold` container and Databricks Metastore catalog (`db_bankingproject`).

2. **Automated Trigger Setup**:
   * **Databricks Jobs Engine**: A scheduled workflow trigger is configured to execute notebook runs at defined intervals.
   * **State Handling**: Ensures idempotent writes to prevent record duplication during scheduled runs.

---

## Security Configuration

Secrets are dynamically accessed at runtime via Azure Key Vault integration using Databricks Secret Scopes:

# python
# Secure environment setup
storage_account_name = "bankingprojectstacc"
storage_account_key = dbutils.secrets.get(scope='keyvault-scope', key='storage-access-key')

# Spark session authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", 
    storage_account_key
)


* ** TRIGER_PYSPARK:

  
 https://github.com/user-attachments/assets/3ab92e90-d243-4807-8c13-38126aac7b54


* ** Data Landing in gold and silver containers/layers:


https://github.com/user-attachments/assets/8a25fca5-ec04-40b2-9c31-84721ee22a76



# Azure Databricks CI/CD Data Pipeline

This project automates the deployment of PySpark notebooks and scripts from GitHub directly into an Azure Databricks workspace using GitHub Actions.

## 📌 Project Overview
Instead of manually exporting and importing code into Databricks, this pipeline ensures that every commit pushed to the `main` branch automatically syncs with the target production workspace in real time.

## ⚙️ Key Benefits & Code Impact
* **Automated Code Sync**: Eliminates manual file uploads. Your latest local code is always up to date in the Databricks workspace.
* **Production Consistency**: Ensures that code running in the Databricks environment strictly matches the tested source code in GitHub.
* **Safe Version Control**: Facilitates easy rollbacks—restoring a previous commit on GitHub automatically reverts the codebase in Databricks.

## 🔐 Required GitHub Secrets
To authenticate the workflow, set up the following secrets in **GitHub Repo > Settings > Secrets and variables > Actions**:
* `DATABRICKS_HOST`: Your Databricks workspace URL (without trailing slashes).
* `DATABRICKS_TOKEN`: A Databricks Personal Access Token (PAT) configured with `workspace` scope.

  
 <img width="1862" height="591" alt="Screenshot 2026-08-08 121934" src="https://github.com/user-attachments/assets/600ce5e9-5f1e-4ef3-bd99-e29548a2040a" />


 # Azure Synapse Analytics 

An enterprise-grade cloud data warehousing and analytics platform engineered on Azure Synapse Analytics. This system decouples storage from compute, implements zero-trust identity authentication, and automates continuous data marts execution.

---

## 🏗️ Architecture & System Design
---

## 🔒 Enterprise Security & Identity Model

* **Password-less Access**: Eliminates hardcoded storage keys and SAS tokens by utilizing Azure System-Assigned Managed Identity (`IDENTITY = 'Managed Identity'`).
* **Database Master Key**: Encrypts scoped credentials at the database level to ensure strict compliance and zero credential leakage in repositories.

---

## ⚡ Serverless Data Lakehouse Engine

* **External Data Source**: Establishes high-throughput ABFS connection directly to the `gold` container in Azure Data Lake Storage Gen2.
* **Storage-Decoupled Querying**: Employs `OPENROWSET` over columnar Parquet files, allowing query execution on data lakes without physical ingestion or storage duplication costs.
* **Dimensional Data Marts**:
  * `dim_customer_360`: Unified 360-degree customer profile dimension.
  * `fct_banking_transactions`: High-performance transactional fact view for BI consumers.

---

## 🔄 Automated Pipeline Orchestration

* **Execution Pipeline (`banking_trig`)**: Validated production pipeline handling downstream dataset refreshes.
* **Scheduled Triggers (`banking_landing`)**: Automated batch execution cycles running at defined intervals for continuous data loading.


* **SYNAPSE_VIDEO:

https://github.com/user-attachments/assets/925d587c-a6c1-4b34-8a59-a35613e5fcce




# Enterprise Lakehouse & Executive Analytics Platform

## Overview
An end-to-end Enterprise Data Platform built to ingest, transform, analyze, and visualize high-volume banking and transaction datasets. The pipeline processes multi-million dollar transaction volumes through a modern lakehouse architecture, leveraging automated data transformation workflows, advanced Python statistical risk profiling, and an interactive executive Power BI dashboard.

---

## Executive Analytics & Data Insights

### 1. Net Cash Flow & Liquidity Generation by Country
* **Liquidity Generation Leaders:** **Denmark**, **United States**, and **United Kingdom** generate the highest positive net liquidity (~+$200k), indicating strong capital retention and low churn risk.
* **Capital Outflow Risks:** **Iran**, **Canada**, and **Finland** face severe negative net cash flows (~-$200k), where customer withdrawals significantly outpace incoming deposits.
* **Strategic Recommendation:** Scale high-yield savings and investment products in top-tier liquidity markets (Denmark/US). Deploy retention campaigns and targeted transfer perks in high-outflow regions to reduce capital flight.

### 2. Customer Distribution & Market Penetration
* **Core User Base:** The **United States**, **Spain**, and **Ireland** lead in active account volume, maintaining over 300 active users each.
* **Underrepresented Markets:** **Mexico** displays the lowest overall user adoption (~180 customers), despite showing moderate transactional activity.
* **Strategic Recommendation:** Focus marketing and customer acquisition budgets on high-conversion markets (US, UK, Spain), while launching localized onboarding initiatives in low-volume regions like Mexico.

### 3. Demographic Value Driver Analysis
* **Primary Revenue Drivers:** Customers aged **31–45** account for the highest average transaction volume (~$16.5k), demonstrating high long-term value and stability.
* **Strategic Recommendation:** Design tailored financial products—such as premium credit tiers for the 31–45 cohort and simplified, secure wealth-preservation services for the 60+ demographic—to maximize Customer Lifetime Value (CLV).

### 4. Acquisition Cohort & Growth Dynamics
* **Peak Onboarding Era:** Customer registrations hit an all-time surge around 2009–2010, surpassing 400+ new account signups in a single year.
* **Baseline Stability:** From 2011 to 2022, account registrations remained stable (200–300 users/year), followed by a sharp drop in the most recent acquisition cycle.
* **Strategic Recommendation:** Audit the onboarding funnel and marketing spend to identify root causes of the recent acquisition decline, and reactivate high-conversion acquisition channels used during peak growth phases.

---

## Power BI Executive Dashboard Highlights

| Metric | Value | Business Focus |
| :--- | :--- | :--- |
| **Total Volume Processed** | **$12M** | Platform-wide monetary throughput |
| **Total Transactions** | **4,999** | End-to-end processed transaction log |
| **Total Customers** | **615** | Active customer account base |
| **Healthy Customer Count** | **615** | Zero non-performing / defaulted accounts |

- **Transaction Category Split:** Low-Value transactions account for **95.82%** ($12M) of total activity, while Medium-Value transactions represent **4.18%** ($1M).
- **Transaction Types:** Monitored seamlessly across **Withdrawals**, **Payments**, **Deposits**, and **Transfers** to evaluate real-time regional liquidity balances.

---

## Security & Best Practices
- **Entra ID Token Authentication:** Database access is authenticated dynamically using Azure Entra ID (MSAL tokens) via `InteractiveBrowserCredential`.
- **Data Governance:** Sensitive parameters, tokens, and tenant configurations are securely masked and loaded via environment variables.


# Dashboard_LINK:
https://drive.google.com/file/d/14IdzzKbl1W56FWYnYJDiH5R7nAEdNIdY/view?usp=sharing


<img width="1095" height="372" alt="newplot (12)" src="https://github.com/user-attachments/assets/3eee8ba8-30f2-43b9-a7e1-52ea4ce61d31" />


# Analytics_ Video:

https://github.com/user-attachments/assets/a25ec678-f6e0-4329-9f99-cc6dee43091d
















