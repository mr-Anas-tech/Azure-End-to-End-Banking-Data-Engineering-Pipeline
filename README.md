# Azure-End-to-End-Banking-Data-Engineering-Pipeline


## 📊 Dataset & Ingestion Overview

* **Data Source:** Public REST API (`randomuser.me`) simulating continuous real-world retail banking customer account creation and transactions.
* **Volume & Batch Size:** **1,000 records per execution batch**, ingested continuously.
* **Data Format:** Raw nested JSON format.
* **Attributes Included:** Customer Demographics, Full Name, Contact Details (Email, Cell), Geographic Location (Address, City, Coordinates), Registration Timestamps, and Account UUIDs.
* **Ingestion Architecture:** Scheduled Azure Data Factory (ADF) pipeline running on a 1-hour recurrence cycle, pushing raw data directly into **Azure Data Lake Storage Gen2 (Bronze Layer)**.

* 
*


https://github.com/user-attachments/assets/5f956e90-922a-4ab5-b8f8-f30453e97795
<img width="1908" height="863" alt="Screenshot 2026-08-03 121754" src="https://github.com/user-attachments/assets/7f418c44-cdf9-4d17-a6e6-70a7db8ad76d" />





