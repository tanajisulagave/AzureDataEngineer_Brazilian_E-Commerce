**🔧 Overview of the Architecture**
This is a classic modern data platform using Azure services and following the Medallion Architecture (Bronze → Silver → Gold), integrated with reporting in Power BI.

**🗂️ Project Components**

***1. Source Control (GitHub)*** \
Purpose: Stores raw data files and possibly orchestration scripts, configuration files, or source metadata.
Usage: Acts as the starting point from where data is ingested using ADF.
2. ADF (Azure Data Factory)
Role: Orchestrator & ETL Tool

Actions:

Connects to GitHub (source).

Ingests raw data into Azure Data Lake Storage (ADLS) Gen2 Bronze layer.

Supports both batch ingestion and incremental loads using metadata-driven pipelines.

Key Features:

Pipelines, Linked Services, Datasets, Data Flows.

Triggering schedules (e.g., daily/hourly).

3. Bronze Layer (ADLS Gen2)
Role: Raw Data Storage

Characteristics:

Stores raw, unprocessed data from source systems.

Schema-on-read, no transformations.

Common formats: CSV, JSON, Parquet.

Benefits:

Acts as a data lake archive.

Provides traceability and auditability of raw data.

4. Silver Layer (ADLS Gen2 + Azure Databricks)
Role: Cleaned and Transformed Data

Transformation Tool: Azure Databricks

Actions:

Cleans, filters, deduplicates, and joins datasets.

Converts raw formats to optimized ones like Delta Lake or Parquet.

Implements business logic.

Benefits:

Removes noise and inconsistencies.

Ready for enrichment and business consumption.

5. Gold Layer (Synapse Analytics)
Role: Curated Data for Analysis

Storage/Query Engine: Azure Synapse Serverless SQL Pool

Actions:

Consumes cleaned data from the Silver layer.

Performs aggregations, dimensional modeling (Star/Snowflake schema), and final business rules.

Exposes views or tables to Power BI or downstream consumers.

Benefits:

Highly optimized for analytics.

Low-latency querying with Serverless SQL Pool.

Easy integration with Power BI.
<img width="1660" height="705" alt="Brazil-E-Commers" src="https://github.com/user-attachments/assets/bd09ff26-ad81-4858-868e-e98b1fb1c22e" />

6. Reporting (Power BI)
Role: Business Intelligence Layer

Actions:

Connects to Synapse via DirectQuery or Import mode.

Provides dashboards, reports, KPIs for end-users.

Supports data slicing, drilldowns, and filters.

End Users: Business Analysts, Executives, Stakeholders.

<img width="1321" height="742" alt="image" src="https://github.com/user-attachments/assets/9146074f-08e9-476d-8c35-36112d72f23e" />
<img width="1471" height="833" alt="image" src="https://github.com/user-attachments/assets/2f80fd91-2031-4dba-a640-a4b988d1323d" />
<img width="1473" height="825" alt="image" src="https://github.com/user-attachments/assets/c8850214-0248-4b3b-abc4-e3dea88c375a" />

🛠️ Pipeline Flow Summary
scss
Copy
Edit
GitHub (Source) 
   ↓
ADF (Orchestration + Ingestion)
   ↓
Bronze (Raw in ADLS Gen2)
   ↓
Azure Databricks (Transformations)
   ↓
Silver (Cleaned in ADLS Gen2)
   ↓
Synapse Serverless SQL (Curation)
   ↓
Gold (Aggregated + Modeled in Synapse)
   ↓
Power BI (Reporting & Analytics)
