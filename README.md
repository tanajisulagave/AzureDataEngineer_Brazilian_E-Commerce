# Brazilian E-Commerce: End-to-End Data Engineering Project

This project demonstrates a complete Azure-based data engineering solution using Medallion Architecture (Bronze → Silver → Gold) for a Brazilian E-Commerce dataset. It handles data ingestion, processing, transformation, and reporting using Azure services.

---

## 🧱 Architecture Overview

<img width="1660" height="705" alt="Brazil-E-Commers" src="https://github.com/user-attachments/assets/4bc14820-3640-4b79-ad61-541d136c492b" /> <!-- Update with actual image path -->

### 🔄 Medallion Architecture Layers:
- **Bronze**: Raw data from GitHub ingested using ADF and stored in ADLS Gen2.
- **Silver**: Cleaned, transformed data using Azure Databricks.
- **Gold**: Aggregated and modeled data in Azure Synapse Analytics.
- **Reporting**: Visualized using Power BI.

---

## 🚀 Tech Stack

| Layer        | Service Used                  |
|-------------|-------------------------------|
| Ingestion    | Azure Data Factory (ADF)      |
| Storage      | Azure Data Lake Storage Gen2  |
| Transformation | Azure Databricks (PySpark) |
| Curated Layer | Azure Synapse (Serverless SQL) |
| Reporting    | Power BI                      |
| Version Control | GitHub                    |

---

## 📂 Project Workflow

### 1. **Data Ingestion (ADF)**
- Reads raw data (CSV/JSON) from GitHub.
- Loads into **Bronze Layer** in ADLS Gen2.

### 2. **Data Transformation (Databricks)**
- Reads raw data from Bronze.
- Cleans, filters, joins and writes to **Silver Layer** in ADLS Gen2.

### 3. **Data Aggregation (Synapse)**
- Reads cleaned data from Silver.
- Applies business logic, aggregation, dimensional modeling.
- Stores output in **Gold Layer** using Serverless SQL Pool.

### 4. **Reporting (Power BI)**
- Connects to Synapse to create interactive dashboards and reports.

---

<img width="1321" height="742" alt="image" src="https://github.com/user-attachments/assets/9146074f-08e9-476d-8c35-36112d72f23e" />\
<img width="1471" height="833" alt="image" src="https://github.com/user-attachments/assets/2f80fd91-2031-4dba-a640-a4b988d1323d" />\
<img width="1473" height="825" alt="image" src="https://github.com/user-attachments/assets/c8850214-0248-4b3b-abc4-e3dea88c375a" />


