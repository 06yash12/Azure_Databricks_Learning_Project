# Azure Databricks Learning & Hands-on Project

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1B3A57?style=flat&logoColor=white)

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Starting Point — Azure Free Subscription](#2-starting-point--azure-free-subscription)
3. [Azure Account, Resource Setup, and Resource Group](#3-azure-account-resource-setup-and-resource-group)
4. [ADLS Gen2 as the Storage Layer](#4-adls-gen2-as-the-storage-layer)
5. [Azure Databricks Workspace Creation](#5-azure-databricks-workspace-creation)
6. [Connecting Databricks with Azure Storage](#6-connecting-databricks-with-azure-storage)
7. [Adding Data and Building the First Ingestion Workflow](#7-adding-data-and-building-the-first-ingestion-workflow)
8. [Ingestion Folder — Hands-on Ingestion Patterns](#8-ingestion-folder--hands-on-ingestion-patterns)
9. [Transformation Folder — Bronze, Silver, Gold](#9-transformation-folder--bronze-silver-gold)
10. [Lakeflow Spark Declarative Pipelines Basics](#10-lakeflow-spark-declarative-pipelines-basics)
11. [Real-World Lakeflow SDP Medallion Pipeline](#11-real-world-lakeflow-sdp-medallion-pipeline)
12. [Databricks SQL, Alerts, and AI/BI Assets](#12-databricks-sql-alerts-and-aibi-assets)
13. [Genie — AI Assistant and Genie Space](#13-genie--ai-assistant-and-genie-space)
14. [Jobs and Orchestration](#14-jobs-and-orchestration)
15. [Delta Lake and Optimization Concepts](#15-delta-lake-and-optimization-concepts)
16. [Security and Governance](#16-security-and-governance)
17. [Final Outputs](#17-final-outputs)
18. [Key Skills and Overall Learning](#18-key-skills-and-overall-learning)

---

## 1. Introduction

This repository documents my **Azure Databricks learning and hands-on project**, completed as part of the **Udemy course by Ansh Lamba (Databricks MVP & Instructor)**.

I completed the course to explore modern **data engineering, analytics, and broader Databricks ecosystem technologies**, understand Databricks services and terminology, learn how common cloud data workflows are structured, and gain practical hands-on experience building automated and modern data pipelines on Azure.

Rather than being a generic tutorial, this document reflects the actual learning path I followed in this workspace — starting from Azure setup, then storage and ingestion, then transformations, Lakeflow Spark Declarative Pipelines, SQL analytics, jobs, optimization, and finally governance/security.

---

## 2. Starting Point — Azure Free Subscription

The project began with the **Azure $200 / 1-month free subscription**, which was used as the initial cloud environment for exploring Azure-native data engineering services.

This first step mattered because it provided a practical sandbox to learn:

* how Azure resources are created and organized
* how storage and compute are separated
* how Databricks fits into the Azure ecosystem
* how enterprise-style cloud data projects are structured from day one

---

## 3. Azure Account, Resource Setup, and Resource Group

After activating the Azure account, the next step was setting up the project environment properly in Azure.

A dedicated **Azure Resource Group** was created to organize the main services used throughout the project. This is the standard Azure starting point because it keeps related services together for lifecycle management, permissions, and cost tracking.

The core services created and used during the project were:

| Resource Type | Actual Detail in Project | Role in Learning Project |
| --- | --- | --- |
| Azure Data Lake Storage Gen2 | Storage account: `yashdatabricks` | Central storage layer |
| Azure Databricks Workspace | Workspace tied to this environment | Main analytics and engineering platform |
| Azure Key Vault integration | Secret scope: `adb-keyvault-yash` | Secret and credential handling |
| Azure Event Hubs | Namespace `yasheventhub`, topic `eventhubtopic` | Streaming ingestion practice |

<img width="1919" height="915" alt="Screenshot 2026-08-18 003734" src="https://github.com/user-attachments/assets/c8056575-2b83-4bd0-b776-ded7c0ac6421" />

---

## 4. ADLS Gen2 as the Storage Layer

The storage foundation for the project was **Azure Data Lake Storage Gen2**.

The storage account used across the repository is:

* `yashdatabricks`

The main container used across the project is:

* `raw`

This was the first major architectural concept learned in practice: **separating storage from compute**. Databricks handled processing, while ADLS Gen2 stored raw files, staged files, processed Delta data, checkpoints, schema locations, external sinks, and optimization examples.

The project uses the `abfss://` protocol throughout:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/<path>
```

Important path groups used later in the project:

| Path Area | Purpose |
| --- | --- |
| `staging/` | E-commerce source files |
| `rideshare/` | Ride-sharing source datasets |
| `standard/` | General ingestion practice files |
| `standard/autoloader_*` | Auto Loader source, checkpoint, and sink |
| `standard/volume_dir` | External volume location |
| `optimize/` | Delta Lake optimization experiments |
| `sinks/` | External sink for Lakeflow append flow |

<img width="1909" height="516" alt="image" src="https://github.com/user-attachments/assets/c8bb2ff1-651a-476e-98e4-046c7b9e22ec" />

---

<img width="1291" height="547" alt="image" src="https://github.com/user-attachments/assets/385002cb-049e-4d6a-a6ea-469f4f1ceb3a" />

---

## 5. Azure Databricks Workspace Creation

Once storage was available, the next major step was creating and launching the **Azure Databricks Workspace**.

This is where the learning shifted from Azure infrastructure into the Databricks platform itself — notebooks, SQL, Delta Lake, Unity Catalog, Lakeflow Spark Declarative Pipelines, Jobs, and security.

Within this workspace, the project evolved into a structured learning repository under:

```text
/Users/ya***12**@outlook.com/YashDatabricks/
```

Top-level folders in the workspace:

* `Ingestion/`
* `Transformation/`
* `LDP_Basics/`
* `LDP_realworld/`
* `Databricks_SQL/`
* `Databricks_Job/`
* `Optimize/`
* `Security/`

<img width="1290" height="516" alt="image" src="https://github.com/user-attachments/assets/502cf8a4-657e-4105-92c0-175983bc14c6" />

---

## 6. Connecting Databricks with Azure Storage

After the workspace was ready, the next hands-on step was connecting Databricks to Azure Storage and verifying that data in ADLS could be read successfully.

This was validated throughout the project by directly reading files from ADLS using `abfss://` paths in notebooks. The earliest example is:

**Notebook path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/Ingest_NB`

That notebook reads a CSV file from:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/standard/notebook/flights.csv
```

This step was important because it established the base working pattern used everywhere else in the project:

* data lands in ADLS Gen2
* Databricks reads the data from cloud storage
* data is transformed and written back as Delta tables or managed assets

---

## 7. Adding Data and Building the First Ingestion Workflow

With storage access working, the next phase was loading source data into ADLS and building the first ingestion workflows.

Two main source groups appear repeatedly across the project:

### 7.1 E-Commerce Source Data

Stored under:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/staging/
```

| Source Folder | Format | Used Later In |
| --- | --- | --- |
| `staging/customers` | CSV | `Transformation/bronze` |
| `staging/products` | Parquet | `Transformation/bronze` |
| `staging/orders` | JSON (multi-line) | `Transformation/bronze` |

### 7.2 Ride-Sharing Source Data

Stored under:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare/
```

Folders discovered and used in the project:

* `customers/`
* `drivers/`
* `payments/`
* `trips/`
* `vehicles/`

These source folders later fed both a batch bootstrap process and a streaming Lakeflow SDP pipeline.

<img width="1280" height="456" alt="image" src="https://github.com/user-attachments/assets/26562047-4452-4547-a076-848340bcf19d" />

### 7.3 Data Resources

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Data Resource/`

All datasets used throughout the project were **provided by the course instructor (Ansh Lamba)** as structured learning resources. Rather than sourcing or creating raw files independently, these pre-prepared datasets were the starting point for every ingestion, transformation, and pipeline exercise.

The folder contains files across multiple formats:

| File Type | Role in the Project |
| --- | --- |
| CSV | Customer records, trips data, basic notebook ingestion |
| JSON | Orders and flights data, Auto Loader and COPY INTO exercises |
| Parquet | Products dataset, Bronze layer ingestion |
| YAML | Configuration and pipeline definition files |

Having all source data centralized in one folder made it straightforward to map each ingestion pattern to the correct source format without managing separate data downloads or external dependencies.

---

## 8. Ingestion Folder — Hands-on Ingestion Patterns

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/`

This folder represents the first major hands-on learning block of the course. Instead of relying on a single ingestion method, it explores multiple ingestion patterns that are commonly used in Databricks.

### 8.1 `Ingestion/Ingest_NB`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/Ingest_NB`

**What was done**

A CSV file (`flights.csv`) was read from ADLS using Spark with `header=true` and `inferSchema=true`.

**How it worked**

The notebook uses `spark.read.format("csv")` on:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/standard/notebook/flights.csv
```

**Why it was used**

This was the simplest ingestion starting point and served as the first proof that Databricks could read Azure Storage correctly.

<img width="1348" height="560" alt="image" src="https://github.com/user-attachments/assets/aaa5a012-91ff-4cdd-b73a-63015849372f" />

### 8.2 `Ingestion/Autoloader`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/Autoloader`

**What was done**

A streaming ingestion pipeline was built with **Auto Loader** to read JSON files incrementally and land them in Delta format.

**How it worked**

The notebook defines:

* a checkpoint location
* a schema location
* an explicit schema using `StructType`
* a `cloudFiles` source with JSON format
* a Delta sink with `trigger(once=True)`

Actual paths used:

```text
Source:      abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_source
Schema:      abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/checkpoint/schema_location
Checkpoint:  abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/checkpoint
Sink:        abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_sink/data
```

**Why it was used**

This notebook introduced a production-style file ingestion pattern that is more scalable and automation-friendly than manual batch reads.

### 8.3 `Ingestion/COPYINTO`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/COPYINTO`

**What was done**

A Delta table `azuredatabricks_catalog.ingest.flights_json` was loaded from JSON files using `COPY INTO`.

<img width="1399" height="309" alt="image" src="https://github.com/user-attachments/assets/0dda6bcc-3a57-4f95-8d1d-cd859b50a8de" />

**How it worked**

The notebook executes:

* `CREATE TABLE azuredatabricks_catalog.ingest.flights_json`
* `COPY INTO ... FROM 'abfss://raw@yashdatabricks.dfs.core.windows.net/standard/copyinto'`
* `FILEFORMAT = JSON`
* multiline and schema merge options

The notebook also re-runs `COPY INTO` after new data variation appears, which demonstrates how the target table can absorb schema evolution.

**Why it was used**

This notebook was useful for understanding a SQL-first ingestion pattern that is easy to operationalize for external file loads.

### 8.4 `Ingestion/CTAS`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/CTAS`

**What was done**

A new Delta table was created using **CTAS** (`CREATE TABLE AS SELECT`) at:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/standard/ctas
```

The target table created was:

* `azuredatabricks_catalog.ingest.cats_table`

It was built from `azuredatabricks_catalog.ingest.flights_json` with a filter:

* `WHERE loyaltypoints IS NOT NULL`

**Why it was used**

This notebook helped connect ingestion with transformation — showing how a new curated table can be created directly from a query result.

### 8.5 `Ingestion/JDBC`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/JDBC`

**What was done**

Relational data was ingested from Azure SQL using JDBC and stored as a Delta table.

**How it worked**

The notebook defines a JDBC connection to:

* host: `azuredatabricksserveryash.database.windows.net`
* database: `azuredatabricksdb`
* table: `dbo.ordersnew`

It then reads the data with `spark.read.jdbc(...)` and writes the result to:

* `azuredatabricks_catalog.ingest.sql_data`

**Why it was used**

This was the structured database ingestion pattern in the project and helped show how Databricks can unify file-based and relational sources.

### 8.6 `Ingestion/API`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/API`

**What was done**

Data was pulled from a public REST API and converted into a Spark DataFrame.

**How it worked**

The notebook calls:

```text
https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json
```

It extracts the `Results` array, converts it into a Spark DataFrame, and writes the output to:

* Delta path: `abfss://raw@yashdatabricks.dfs.core.windows.net/standard/api_data`
* table: `azuredatabricks_catalog.ingest.api_data`

**Why it was used**

This notebook demonstrated that ingestion is not limited to files and databases — APIs are also valid enterprise data sources.

<img width="1341" height="452" alt="image" src="https://github.com/user-attachments/assets/93eb63e8-6634-4816-a6dc-fa560a303412" />

### 8.7 `Ingestion/Volumes`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/Volumes`

**What was done**

Both a **managed volume** and an **external volume** were created and queried.

**How it worked**

The notebook creates:

* `azuredatabricks_catalog.ingest.managed_volume`
* `azuredatabricks_catalog.ingest.external_volume`

The external volume points to:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/standard/volume_dir
```

Data is then queried from:

* `/Volumes/azuredatabricks_catalog/ingest/managed_volume/rawdata/flights.csv`
* `/Volumes/azuredatabricks_catalog/ingest/external_volume/flights.csv`

**Why it was used**

This notebook helped explain Unity Catalog Volumes as a governed storage abstraction for file access inside Databricks.

<img width="1299" height="512" alt="image" src="https://github.com/user-attachments/assets/3fde1f7d-3da4-4e19-816e-9ab05e52c02d" />

### 8.8 Other Notebook

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/Ingestion/Scratchpad`

This notebook exists as a working area but does not materially contribute to the documented end-to-end flow.

---

## 9. Transformation Folder — Bronze, Silver, Gold

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Transformation/`

After learning ingestion patterns, the next step in the course journey was understanding how raw data is converted into analytics-ready tables using the **Medallion Architecture**.

### 9.1 Bronze — Raw Standardized Storage

**Notebook:** `/Users/ya***12**@outlook.com/YashDatabricks/Transformation/bronze`

**What was done**

The e-commerce source files from `staging/` were loaded into Unity Catalog Bronze tables.

**How it worked**

* `customers` was read from CSV with both inferred and explicit schemas
* `products` was read from Parquet and also tested with `COPY INTO`
* `orders` was read from multi-line JSON
* all three were written as Delta tables under `azuredatabricks_catalog.bronze`

Actual targets created and used:

* `azuredatabricks_catalog.bronze.customers`
* `azuredatabricks_catalog.bronze.products`
* `azuredatabricks_catalog.bronze.orders`

**Why it was used**

This stage introduced the idea that Bronze should preserve the raw source as closely as possible while standardizing storage into Delta.

### 9.2 Silver — Cleaning and Feature Enrichment

**Notebook:** `/Users/ya***12**@outlook.com/YashDatabricks/Transformation/silver`

**What was done**

The Bronze customer data was cleaned, standardized, filtered, deduplicated, and enriched.

**How it worked**

The notebook demonstrates several hands-on PySpark operations:

| Learning Pattern | Example in Notebook |
| --- | --- |
| Null handling | `dropna()`, `fillna("unknown")`, `fillna(0, subset=['age'])` |
| Deduplication | `dropDuplicates()`, `dropDuplicates(subset=['email'])` |
| Filtering | country-based filters for India / USA |
| Set operations | `union`, `intersect`, `exceptAll` |
| Derived columns | email domain extraction |
| Conditional logic | `when(...)` to create customer `flag` categories |

**Why it was used**

This stage was where raw data started becoming useful business data. It also gave hands-on practice with the core PySpark transformation patterns used repeatedly in real projects.

### 9.3 Gold — One Big Table and Analytical Views

**Notebook:** `/Users/ya***12**@outlook.com/YashDatabricks/Transformation/Gold`

**What was done**

The Silver `orders`, `customers`, and `products` tables were joined into a denormalized Gold table.

**How it worked**

The notebook reads:

* `azuredatabricks_catalog.silver.orders`
* `azuredatabricks_catalog.silver.products`
* `azuredatabricks_catalog.silver.customers`

Then joins them into:

* `azuredatabricks_catalog.gold.obt`

A Delta `MERGE` pattern is used so that reruns can upsert data instead of blindly duplicating it. The executed notebook shows the resulting Gold table count as:

* **30,000 rows**

The notebook also creates business-facing analytical outputs:

* grouped counts by `country` and `customer_type`
* pivot results by `country` and `flag`
* a simple custom UDF example

**Why it was used**

This stage connected the engineering pipeline to analytics consumption — turning cleaned operational data into a table ready for SQL and reporting.

### 9.4 `Transformation/dbutils`

This notebook exists in the folder, but the main documented transformation flow is driven by `bronze`, `silver`, and `Gold`.

---

## 10. Lakeflow Spark Declarative Pipelines Basics

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_Basics/`

After working through notebook-based ingestion and transformations, the next learning step was understanding **Lakeflow Spark Declarative Pipelines (SDP)**.

This folder acts as the foundation layer before the larger real-world pipeline.

### 10.1 Key Assets in `LDP_Basics`

| Path | Purpose |
| --- | --- |
| `LDP_Basics/Baisc` | notebook-based exploration of SDP basics |
| `LDP_Basics/ldp_source` | source notebook used in the basics area |
| `LDP_Basics/explorations/explore` | exploration notebook |
| `LDP_Basics/transformations/basics.py` | basic streaming table chain |
| `LDP_Basics/transformations/append_flow.py` | append-flow example |
| `LDP_Basics/transformations/stream_sql.sql` | SQL-based pipeline syntax |
| `LDP_Basics/transformations/matatrialized_view.py` | materialized view pattern |
| `LDP_Basics/transformations/event_hub.py` | Azure Event Hubs ingestion |

### 10.2 `transformations/basics.py`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_Basics/transformations/basics.py`

**What was done**

A simple three-step declarative flow was created:

* `ingest_data`
* `transform_data`
* `served_data`

**How it worked**

The file uses:

* `@dp.table`
* `@dp.temporary_view`
* streaming reads from `azuredatabricks_catalog.dummy.src_tbl`

It casts `age`, adds timestamps, and appends markers like `transformed` and `served`.

**Why it was used**

This was the cleanest introduction to how declarative data pipelines differ from notebook-by-notebook imperative workflows.

<img width="1919" height="906" alt="Screenshot 2026-08-18 010928" src="https://github.com/user-attachments/assets/20eabe67-8d73-4bb0-910e-c6da5510f984" />

### 10.3 `transformations/append_flow.py`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_Basics/transformations/append_flow.py`

**What was done**

Two source streams (`source_a_tbl`, `source_b_tbl`) were appended into a shared external Delta sink.

**Actual sink path:**

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/sinks
```

**Why it was used**

This demonstrated multi-source ingestion into a single destination using Lakeflow append flows.

### 10.4 `transformations/event_hub.py`

**Path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_Basics/transformations/event_hub.py`

**What was done**

Azure Event Hubs data was consumed through Kafka configuration in a Lakeflow pipeline.

**Actual event source details:**

* namespace: `yasheventhub`
* topic: `eventhubtopic`
* connection string: `spark.conf.get("EH_CONN_STR")`

**Why it was used**

This showed how cloud-native streaming services connect into Databricks pipelines.

<img width="1919" height="905" alt="Screenshot 2026-08-18 005559" src="https://github.com/user-attachments/assets/1c8fcd12-e12d-40fd-8d8f-564a5fca5729" />

### 10.5 `transformations/stream_sql.sql` and `matatrialized_view.py`

These files extend the learning beyond Python-based pipeline definitions into:

* SQL-driven streaming pipeline syntax
* materialized-view style declarative modeling
  
---

## 11. Real-World Lakeflow SDP Medallion Pipeline

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_realworld/`

Once the SDP basics were understood, the next stage was a more realistic medallion pipeline built on ride-sharing data.

### 11.1 Batch Bootstrap with `bronze_map`

**Notebook:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_realworld/bronze_map`

**What was done**

The ride-sharing source folders were enumerated from ADLS and loaded into `azuredatabricks_catalog.ldp_medallion` tables.

**How it worked**

The notebook lists folders under:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare
```

It then loops through the folders, renames the `updated_datetime` column appropriately, and writes all entities except `trips/` into Delta tables.

**Why `trips/` was excluded**

Because the trips dataset is handled later as a streaming source in the SDP pipeline.

### 11.2 Pipeline Transformation Files

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/LDP_realworld/ldp_medallion_pipeline/transformations/`

| File | Role |
| --- | --- |
| `bronze_trips.py` | streaming Bronze ingestion of trips CSV files |
| `silver_trips.py` | streaming Silver cleanup and enrichment |
| `gold_obt.py` | Gold wide-table stream-static join |
| `stg_dimensions.py` | temporary staging views for dimensions |
| `dimensions.py` | persisted dimension tables |
| `fact.py` | fact table creation |

<img width="1919" height="906" alt="Screenshot 2026-08-18 010958" src="https://github.com/user-attachments/assets/3cd1f1e2-7b53-464f-8bbf-91cf7890d169" />

### 11.3 `bronze_trips.py`

Reads trips as a stream using `cloudFiles` from:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare/trips
```

This introduces streaming Bronze ingestion in declarative pipeline form.

### 11.4 `silver_trips.py`

Reads the `bronze_trips` stream, normalizes `trip_status` using `upper(...)`, and adds `silver_processed_at`. The file also includes data-quality expectation definitions.

### 11.5 `gold_obt.py`

Builds the ride-sharing Gold table by joining the `silver_trips` stream with batch dimension tables:

* `azuredatabricks_catalog.ldp_medallion.customers`
* `azuredatabricks_catalog.ldp_medallion.drivers`
* `azuredatabricks_catalog.ldp_medallion.vehicles`
* `azuredatabricks_catalog.ldp_medallion.payments`

A **10-minute watermark** is applied on `updated_datetime` before the join.

This file is one of the most important project assets because it combines:

* streaming ingestion
* watermarking
* stream-static joins
* wide-table analytical modeling

### 11.6 `stg_dimensions.py`, `dimensions.py`, and `fact.py`

After `gold_obt` is built, the pipeline breaks the wide data model into analytical structures:

* staging temporary views for dimensions
* persisted dimensions
* a fact table with the key business measures and foreign keys

This step turns the pipeline into a more complete star-schema style modeling exercise.

---

## 12. Databricks SQL, Alerts, and AI/BI Assets

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Databricks_SQL/`

After data was modeled into curated tables, the next learning step was using Databricks SQL assets for querying, alerting, and business-facing analytics.

Assets present in the folder:

| Asset | Type | Learning Use |
| --- | --- | --- |
| `customer_query` | Saved Query | SQL exploration on curated data |
| `standalone_stream` | Saved Query | SQL work on streaming-related objects |
| `sql_file.sql` | SQL File | Reusable SQL authoring |
| `New Alert` | Alert | Alerting on SQL results |
| `Ride Sharing Operations Analytics` | Genie Space | AI/BI natural-language exploration |

**Why this stage mattered**

It shifted the learning focus from engineering-only work into consumption and analytics — how curated data becomes queryable, explainable and actionable.

---

## 13. Genie — AI Assistant and Genie Space

Two distinct Genie capabilities were explored during this project: **Genie Code** (the Databricks AI assistant) and a **Genie Space** built directly over the ride-sharing dataset.

### 13.1 Genie Code — AI-Assisted Development

**Genie Code** was used as an in-workspace AI assistant. It helped with: generating and structuring the full project documentation

> The `Documentation.md` and `README.md` files in this repository were written with the help of Genie Code — making it a real part of the project workflow.

### 13.2 Genie Space — Ride Sharing Operations Analytics

**Asset:** `Databricks_SQL/Ride Sharing Operations Analytics`

A dedicated **Genie Space** was created over the ride-sharing data in `azuredatabricks_catalog.ldp_medallion`. This is a Databricks AI/BI space that allows natural-language prompts to query data without writing SQL.

**What was done**

The Genie Space was pointed at the ride-sharing tables and used to explore the dataset through prompts — asking questions about trips, drivers, customers, and payments and getting back query results and charts automatically.

**What this demonstrated**

* how Genie can act as an **AI data agent** over governed Unity Catalog tables
* how non-engineers can explore structured data using natural language
* the practical use of AI/BI tooling on top of a pipeline that was built from scratch

<img width="1316" height="507" alt="image" src="https://github.com/user-attachments/assets/2a45691e-4fc8-4a4c-862e-ca041341795e" />

<img width="1281" height="604" alt="image" src="https://github.com/user-attachments/assets/270904c4-c077-411a-968b-e9264490fc54" />

---

## 14. Jobs and Orchestration

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Databricks_Job/`

Once notebooks and SQL patterns were in place, the next learning area was **automation**.

This folder focuses on parameterized execution, dynamic paths, and passing values across tasks.

### 14.1 `Databricks_Job/execute`

**What was done**

A notebook was built to accept runtime parameters for catalog, schema, and table names.

**How it worked**

It defines widgets:

* `schema_name`
* `catalog_name`
* `tabel_name`

and reads the requested table dynamically:

```python
df = spark.read.table(f"{fetch_catalog}.{fetch_schema}.{fetch_tabel}")
```

It also attempts to retrieve cross-task values using:

```python
dbutils.jobs.taskValues.get(taskKey="parameters", key="params")
```

**Why it was used**

This introduced job parameterization and multi-task communication.

### 14.2 `Databricks_Job/ingest_loop`

**What was done**

A reusable ingestion notebook was structured around two parameters:

* `container`
* `folder_path`

**Why it was used**

It demonstrates how the same notebook can be reused for different ingestion inputs without changing code.

### 14.3 Additional Job Assets

| Asset | Purpose |
| --- | --- |
| `parameters` | task parameter setup |
| `sql_output` | SQL output handling |
| `SQL_param_file.sql` | parameterized SQL in file form |
| `test_records` | output/record checking |
| `excute_render` | rendered execution workflow |
| `params_ingest` | ingestion with parameters |

<img width="1917" height="917" alt="Screenshot 2026-08-18 020045" src="https://github.com/user-attachments/assets/dca744c7-0fd9-44f6-a369-06ebd8651019" />

---

## 15. Delta Lake and Optimization Concepts

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Optimize/`

After orchestration, the project moves into platform concepts that improve reliability and performance.

### 15.1 `Optimize/delta_lake`

**What was done**

A Delta table was created at:

```text
abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/sales_tbl
```

and then exercised through:

* `INSERT`
* `UPDATE`
* `DELETE`
* `DESCRIBE HISTORY`
* `VERSION AS OF`
* `RESTORE`

**Why it was used**

This notebook gave practical understanding of what makes Delta Lake different from plain files: versioning, audit history, recovery, and transactional operations.

### 15.2 `Optimize/chche&presist`

This notebook explores `cache()` and `persist()` behavior and notes an important runtime limitation encountered during learning:

> `PERSIST TABLE` is not supported on serverless compute.

That made it useful not only as an optimization notebook, but also as a practical lesson about runtime-specific behavior.

### 15.3 `Optimize/DDL`

This notebook practices:

* `CREATE TABLE`
* `DROP TABLE`
* `CREATE SCHEMA`
* `DROP SCHEMA`
* `DESCRIBE TABLE`

It also shows writing a DataFrame into `azuredatabricks_catalog.dummy.src_tbl`, which is later reused in the Lakeflow basics examples.

---

## 16. Security and Governance

**Folder path:** `/Users/ya***12**@outlook.com/YashDatabricks/Security/`

The final major learning block focused on governance and secure data access inside Unity Catalog.

### 16.1 `Security/Grant & Revoke`

This notebook explores Unity Catalog access management using commands like `SHOW GRANT ON METASTORE` and introduces grant/revoke concepts for securables.

### 16.2 `Security/secrets`

This notebook demonstrates secret handling through Azure Key Vault-backed Databricks secrets.

Actual scope and key confirmed in the project:

* scope: `adb-keyvault-yash`
* key: `dbcreds`

It uses:

```python
dbutils.secrets.get("adb-keyvault-yash", "dbcreds")
```

This was the main credential-management learning point in the repository.

### 16.3 `Security/CLS`

This notebook demonstrates **Column-Level Security** using a column mask on:

* `azuredatabricks_catalog.gold.employees`

A masking function returns `0` for non-admin users and actual salary for admin users.

<img width="1843" height="457" alt="image" src="https://github.com/user-attachments/assets/8c25dbb3-f9ab-42d4-8f90-9108eeb1c088" />

### 16.4 `Security/RLS`

This notebook demonstrates **Row-Level Security** using:

* `azuredatabricks_catalog.gold.mapping_rls`
* `azuredatabricks_catalog.gold.orders_rls`

A filter function checks `current_user()` and restricts visible rows by region.

<img width="1683" height="474" alt="image" src="https://github.com/user-attachments/assets/292ea4d5-0ffa-4df8-b67b-e5b1bdc8f318" />


### 16.5 `Security/ABAC`

This notebook demonstrates **Attribute-Based Access Control** using tagged columns on the `profiles` table and policy-oriented masking/filtering logic for PII-like fields such as address and SSN.

Together, these notebooks made the security section one of the strongest governance-focused parts of the project.

<img width="1919" height="907" alt="Screenshot 2026-08-18 003936" src="https://github.com/user-attachments/assets/764ff2dc-872b-41d6-9809-8bd79af7eb1a" />

---

## 17. Final Outputs

By the end of the learning journey, the workspace contained a complete set of ingestion, transformation, pipeline, SQL, job, optimization, and governance assets.

### 17.1 Unity Catalog Structures Used

```text
azuredatabricks_catalog
├── bronze
├── silver
├── gold
├── ingest
├── ldp_medallion
└── ldp_basics
```

### 17.2 Key Tables and Outputs Produced

| Area | Output |
| --- | --- |
| E-commerce Medallion | Bronze, Silver, and `azuredatabricks_catalog.gold.obt` |
| Gold OBT | Executed notebook shows **30,000 rows** |
| Ingestion Practice | `ingest.flights_json`, `ingest.cats_table`, `ingest.sql_data`, `ingest.api_data` |
| Delta Learning | `ingest.sales_tbl`, `sales_tbl2` |
| Ride-Sharing SDP | `ldp_medallion` dimensions, streaming tables, `fact`, and Gold OBT |
| Security Learning | `gold.employees`, `gold.orders_rls`, `gold.profiles` |
| SQL / AI-BI | saved queries, alert, Genie space |

### 17.3 Final Folder Map

| Folder | Main Learning Theme |
| --- | --- |
| `Ingestion/` | source ingestion methods |
| `Transformation/` | Medallion architecture |
| `LDP_Basics/` | Lakeflow SDP foundations |
| `LDP_realworld/` | realistic SDP medallion pipeline |
| `Databricks_SQL/` | SQL analytics and AI/BI |
| `Databricks_Job/` | orchestration and automation |
| `Optimize/` | Delta and performance concepts |
| `Security/` | governance and access control |

<img width="1262" height="564" alt="image" src="https://github.com/user-attachments/assets/59499120-9cf4-40bc-af29-1381819923dd" />

---

## 18. Key Skills and Overall Learning

### 18.1 Skills and Concepts Practiced

* Azure resource setup and service organization
* ADLS Gen2 as a cloud storage foundation
* Databricks workspace setup and storage integration
* Multi-format ingestion: CSV, JSON, Parquet, JDBC, API, Event Hubs
* Auto Loader, `COPY INTO`, CTAS, and Volumes
* Medallion architecture using Bronze, Silver, and Gold layers
* PySpark transformations for cleaning, filtering, deduplication, enrichment, and joins
* Delta Lake transactional features, history, and restore
* Lakeflow Spark Declarative Pipelines for batch and streaming design
* Databricks SQL, Alerts, and Genie assets
* Jobs, widgets, task values, and reusable automation patterns
* Unity Catalog security: secrets, grants, CLS, RLS, and ABAC

### 18.2 Overall Learning Summary

This project represents my hands-on completion journey through an Azure Databricks learning path built around real platform components rather than isolated theory. Starting from Azure subscription setup and storage creation, I progressively moved into ingestion patterns, medallion transformations, Lakeflow SDP, SQL analytics, Jobs, Delta Lake optimization, and Unity Catalog governance.

The most valuable part of the project was not just creating individual notebooks, but understanding **how all of these technologies connect together** inside the Databricks ecosystem.

---

*Completed as part of the Udemy course by Ansh Lamba (Databricks MVP & Instructor).*
