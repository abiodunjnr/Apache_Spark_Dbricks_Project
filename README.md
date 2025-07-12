# Apache_Spark_dbricks
# 🏥 Northside Hospital Data Pipeline — Azure Databricks + ADLS + Power BI

This project is a robust, end-to-end **big data pipeline** developed for **Northside Hospital**, leveraging the power of **Apache Spark on Azure Databricks**, **Azure Data Lake Storage Gen2 (ADLS)**, **Azure Data Factory (ADF)**, and **Power BI**. 
It demonstrates a production-grade architecture that ingests real-world healthcare data, performs distributed transformations using Spark, and delivers insightful, real-time dashboards for healthcare decision-makers.

---

## 📌 Project Summary

The pipeline was designed to:
- Ingest and transform hospital data from **Kaggle** directly into **Databricks**.
- Apply **Spark RDD and DataFrame transformations** for structured, schema-enforced processing.
- Implement the **Medallion architecture** (Silver + Gold layers) in **Azure Data Lake Storage Gen2**.
- Visualize critical **healthcare KPIs** through **Power BI dashboards**, refreshed on schedule.
- Automate the full workflow using **Azure Data Factory** with triggers and cluster control.

> This project highlights deep technical fluency in Spark internals, scalable cloud architecture, and real-world business impact through healthcare analytics.

---

## 🧱 Architecture Overview

- **Data Source**: Hospital data from Kaggle (`kanakbaghel/hospital-management-dataset`)
- **Processing Engine**: Apache Spark (RDD, DataFrame, SQL)
- **Storage Layers**: ADLS Gen2 (Silver = cleaned parquet, Gold = snapshot CSV)
- **Orchestration**: Azure Data Factory triggers Databricks notebooks
- **Visualization**: Power BI dashboards via direct ADLS connection

---

## 🔍 Business Context

Northside Hospital aimed to streamline and monitor key healthcare metrics such as billing, appointment activity, and doctor performance. By designing a real-time data pipeline that connects ingestion to visualization, the solution empowers decision-makers with:

- Faster access to billing KPIs
- Appointment trend tracking by method and status
- Doctor-specific workload and billing insights
- Automation to ensure up-to-date data for reporting

---

## 🧠 Key Features & Innovations

### ✅ Data Engineering
- Direct **Kaggle API integration** in Databricks using `kagglehub`
- Transformed CSV files → Pandas → Spark → Parquet
- Created **temporary SQL views** using Spark SQL for analytics
- Snapshot views exported to **Gold layer** as CSV for BI reporting

### ✅ Medallion Architecture (Lakehouse)
- **Silver Layer**: Cleaned and structured data (Parquet in ADLS)
- **Gold Layer**: Final, aggregated snapshot data for analysis (CSV)
- Skipped **Bronze Layer** as data was already structured — justified by context

### ✅ Data Orchestration
- **Azure Data Factory** pipeline to automate:
  - Notebook execution
  - Cluster management
  - Trigger scheduling

### ✅ Secure Architecture
- Secrets managed using **Databricks Secret Scopes**
- ADLS Gen2 accessed using **SAS tokens** and scoped configs
- Resources grouped under an Azure **Resource Group** for unified policy enforcement

---

## 📊 Power BI Dashboard: Northside Hospital KPIs

Published report includes the following key visuals:

| KPI                            | Description                                                      |
|--------------------------------|------------------------------------------------------------------|
| **Appointments by Payment Method** | Compare how patients are paying (insurance, cash, etc.)         |
| **Visit Reasons**             | Breakdown of visit types (checkup, emergency, follow-up, etc.)  |
| **Total Bills Over Time**     | Trends in total billed amount per month                         |
| **Bills by Doctor**           | Doctor-wise revenue generation                                  |
| **Appointment Status**        | Current status of appointments (scheduled, completed, missed)   |

> 📈 The dashboard is published to **Power BI Service** and updated via scheduled refresh from the **Gold layer**.

---

## 🛠️ Technology Stack

| Category         | Tools / Services                               |
|------------------|------------------------------------------------|
| **Programming**  | Python, PySpark, SQL                           |
| **Big Data**     | Apache Spark (RDDs, DataFrames, SQL)           |
| **Storage**      | Azure Data Lake Gen2, DBFS                     |
| **Compute**      | Azure Databricks (Clusters & Notebooks)        |
| **Orchestration**| Azure Data Factory (Triggers & Pipelines)      |
| **BI & Reporting**| Power BI Desktop & Power BI Service           |
| **Security**     | Databricks Secret Scopes, Azure RBAC           |
| **Source Control**| GitHub (Version control, collaboration)       |

---

## Pipeline Steps

### 1. **Ingest Kaggle Data**
- Authenticate with Kaggle via API keys (stored in Databricks secrets)
- Download hospital data: `patients.csv`, `doctors.csv`, `appointments.csv`, `treatments.csv`, `billing.csv`

### 2. **Transform & Save to Silver**
- Read data as Pandas DataFrames
- Convert to Spark DataFrames and save as **Parquet** to `/mnt/silver`

### 3. **Create Views & Aggregate**
- Load parquet files as **temporary Spark SQL views**
- Create snapshot via SQL that joins all entities and computes KPIs

### 4. **Export to Gold Layer**
- Save snapshot result as **CSV** to Gold ADLS container (`/final_table/hospital_export_csv`)
- Used `spark.write.csv()` with headers and overwrite mode

### 5. **Visualize with Power BI**
- Connect Power BI directly to Gold container
- Build interactive visuals and publish to Power BI Service

### 6. **Automate with ADF**
- ADF pipeline triggers notebook execution on schedule
- Ensures fresh data is processed and visualized automatically


##  Getting Started

> To replicate this project in your Azure environment:

1. Set up an Azure subscription and create:
   - Azure Databricks workspace
   - ADLS Gen2 containers: `silver`, `gold`
   - Azure Data Factory
   - Power BI account

2. In Databricks:
   - Create secret scopes for Kaggle and ADLS keys
   - Install `kagglehub` via `%pip install kagglehub`
   - Run the provided notebooks in sequence

3. In Power BI:
   - Open `.pbix` file and connect to your Gold layer
   - Publish to Power BI Service

4. In Azure Data Factory:
   - Import the pipeline JSON
   - Set up schedule triggers to automate refreshes

---

## About the Author

**Name**: [Toluwani Adefisoye]  
**Email**: [toluenenate@gmail.com]  
**GitHub**: [github.com/abiodunjnr/]
**LinkedIn**: [lhttps://www.linkedin.com/in/toluwani-adefisoye-766ab5221/]

---

## Final Thoughts

This project is a **real-world application** of distributed big data processing, cloud-native storage, and interactive analytics. It demonstrates how to build a **scalable, secure, and automated healthcare analytics platform** using modern tools like Spark, Databricks, and Power BI.

---

