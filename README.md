# Indian_Retail_Analytics_AWS_Snowflake_Streams_Tasks_Databricks_Python_Spark_SQL_Power_BI

An enterprise-grade, end-to-end cloud data engineering pipeline implementing a **Medallion (Bronze/Silver/Gold) Architecture**. This system continuously ingests, transforms, models, and visualizes 250,000+ transactional e-commerce records optimized for the Indian retail market.

---

## 🏗️ System Architecture & Data Flow

```text
---

## 🛠️ Tech Stack & Key Concepts Covered
* **Cloud Storage:** **AWS S3** acting as the highly durable raw Data Lake Landing Zone.
* **Data Warehouse:** **Snowflake** managing the centralized Compute & Storage environment.
* **Distributed Compute:** **Databricks (Serverless Python)** utilizing an optimized Pandas translation engine to handle heavy lookup enrichments.
* **Continuous Ingestion:** **Snowpipe** listening natively to AWS SQS queues to load micro-batches instantly.
* **Change Data Capture (CDC):** **Snowflake Streams** tracking row-level append history to minimize downstream cloud compute costs.
* **Orchestration:** **Snowflake Tasks** executing multi-statement SQL procedural transactions on a strict cron schedule.
* **Data Protection & Governance:** **Snowflake Time Travel (30-Day Retention)** for fail-safe auditing, point-in-time querying, and data corruption recovery.
* **Business Intelligence:** **Power BI Desktop** connected via **DirectQuery** to deliver real-time metric rendering.

---

## 📦 Dataset Specifications (Indian E-Commerce Context)
The project processes a relational Indian E-Commerce dataset across three primary entities:
1. **Sales Transactions:** Logs `Order_ID`, `Quantity`, `Unit_Price`, `Payment_Mode` (UPI, COD, Net Banking, Cards), and order delivery markers.
2. **Customer Demographics:** Localized mapping tracking `Customer_Name`, `Gender`, `Customer_Tier`, and geographic location profiles across Indian cities and states (e.g., Uttar Pradesh, Delhi, Tamil Nadu).
3. **Product Catalog:** Stores inventory data containing `Product_Name`, `Category`, and `Brand`.

---

## ⚙️ Detailed Pipeline Implementation Phases

### 1. Ingestion Layer (AWS ➔ Snowpipe ➔ Bronze)
* Configured an **AWS IAM Storage Integration Role** and trust policy handshake to establish secure access to S3 without embedding raw access keys.
* Built modular **Snowpipe Ingestion Engines** optimized using positional mapping metrics (`$1, $2, $3...`) to systematically wrap flat incoming multi-source CSV files into clean, readable JSON `VARIANT` payloads inside Snowflake Bronze tables: `STG_RAW_SALES`, `STG_RAW_CUSTOMERS`, and `STG_RAW_PRODUCTS`.

### 2. Transformation Layer (Bronze ➔ Databricks ➔ Silver)
* Created isolated native **Snowflake CDC Streams** over the Bronze layer to trap incoming modifications.
* Implemented a **Databricks Serverless Pipeline** using Python to securely pull delta rows from the active streams via an optimized `DictCursor` connection.
* Applied strict type conversions, resolved casing/linter structural conflicts, enriched records using multi-table dataframe merges, and bulk-appended the consolidated data back to the Snowflake Silver schema layer as a **One Big Table (`ONE_BIG_TABLE`)**.
* Secured sensitive environment variables by injecting local session runtime context credentials via `dbutils.widgets.text`, eliminating hardcoded production passwords.

### 3. Analytics & Orchestration Layer (Silver ➔ Tasks ➔ Gold Star Schema)
* Modeled an enterprise **Star Schema** in the Gold Layer composed of a transactional fact table (`FACT_SALES`) surrounded by dimensional reference lookup sheets (`DIM_CUSTOMERS` and `DIM_PRODUCTS`).
* Created a scheduled **Snowflake SQL Task** (`sync_gold_star_schema_task`) designed to automatically fire every 30 minutes, handling structural data synchronization through dynamic incremental `MERGE` statement sweeps.
* Activated a **30-day Time Travel retention window** across all Gold tables to ensure enterprise data protection against accidental script drops or processing errors.

### 4. Visualization Layer (Gold ➔ Power BI DirectQuery)
* Connected **Power BI Desktop** directly to the live Snowflake Gold Layer using **DirectQuery** mode to push visual rendering compute back onto Snowflake.
* Designed a semantic star-schema model relationship map linking fact and dimension primary keys.
* Created visualizations tracking **Total Revenue (₹)**, **Digital Payment Adaptation Density (UPI vs. COD)**, and **Geographic Sales Heatmaps** across Indian states.

---

## 📂 Repository File Structure
```text
├── .github/                       # GitHub workflow actions
├── snowflake_pipeline_setup.sql   # Consolidated Gold/Silver/Bronze DDL & Tasks scripts
├── databricks_pipeline.py         # Clean Python CDC transformation notebook code
└── README.md                      # Documentation file
```

---

## 🚀 How to Replicate and Run This Project

1. **Database Script Deployment:** Execute the complete `snowflake_pipeline_setup.sql` script inside your Snowflake worksheet console to initialize databases, pipelines, roles, and schema infrastructure.
2. **AWS Cloud Event Linkage:** Copy the generated SQS ARN string from the Snowflake pipe parameters list and attach it as an active **All Object Created Event Notification** inside your target S3 bucket properties.
3. **Databricks Execution Context:** Open Databricks, link it to your GitHub Repository folder, paste the `databricks_pipeline.py` file, fill out the environment variables widget panel, and run the pipeline!
