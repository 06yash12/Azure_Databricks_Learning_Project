
# Azure Databricks — Hands-on Learning Project

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1B3A57?style=flat&logoColor=white)

Hands-on project completed as part of the **Udemy course by Ansh Lamba (Databricks MVP & Instructor)**.

---

## Certifications

| Certification | Verification |
| --- | --- |
| 🎓 Udemy — Azure Databricks Course Completion | [View Certificate](https://www.udemy.com/certificate/UC-b6511451-82d4-4bce-91de-867eb6910aa4/) |
| 🏅 DP-750 — Microsoft Azure Databricks Data Engineer | [Verify Credential](https://learn.microsoft.com/en-us/users/1207yash/credentials/3e012f782a2f59fe) |
| 📄 Full Project Documentation | [Read Documentation](./Documentation.md) |

---

A complete Azure Databricks learning journey — built from scratch, starting with Azure account and storage setup, through ingestion, medallion transformations, streaming pipelines, SQL analytics, job orchestration, Delta Lake internals and Unity Catalog security. Every concept was implemented with real, executed notebooks and pipeline files in this workspace.

---

## What's Covered

Started from zero — set up an **Azure free subscription**, created a **Resource Group**, provisioned **ADLS Gen2** as the storage layer, and launched an **Azure Databricks Workspace**. From there, every major Databricks data engineering concept was explored hands-on with real executed code.

**Ingestion** — covered every ingestion method: notebook CSV reads, **Auto Loader** for incremental streaming, `COPY INTO`, CTAS, **JDBC from Azure SQL Server**, REST API pulls, and Unity Catalog Volumes.

**Medallion Transformations** — built a full Bronze → Silver → Gold pipeline over e-commerce data. Bronze lands raw files, Silver cleans and enriches, Gold produces a **30,000-row OBT** via multi-table joins with a **Delta MERGE upsert** pattern.

**Lakeflow Spark Declarative Pipelines** — learned SDP foundations (tables, views, materialized views, append flows) then built a real-world **ride-sharing streaming pipeline**: Auto Loader → Bronze → Silver → Gold OBT → Star schema, using stream-static joins with a 10-minute watermark. Also integrated **Azure Event Hubs** via Kafka.

**Databricks SQL & AI/BI** — authored saved queries, set up SQL alerts and explored the **Genie AI/BI space** over ride-sharing data.

**Jobs & Orchestration** — built parameterized notebooks using `dbutils.widgets` and `dbutils.jobs.taskValues` for cross-task value passing and dynamic pipeline execution.

**Delta Lake & Optimization** — practiced full DML operations, `DESCRIBE HISTORY`, **time travel**, `RESTORE`, deletion vectors and cache/persist behavior.

**Security & Governance** — implemented **Column-Level Security**, **Row-Level Security**, **ABAC with PII tags**, Unity Catalog grants and **Azure Key Vault** secret scopes.

---

## Project Structure

| Folder | What's Inside |
| --- | --- |
| [`Data Resource/`](./Data%20Resource/) | Instructor-provided learning datasets — CSV, JSON, Parquet — [details](./Documentation.md#73-data-resources) |
| [`Ingestion/`](./Ingestion/) | CSV, JSON, Parquet, JDBC, API, Auto Loader, COPY INTO, Volumes |
| [`Transformation/`](./Transformation/) | Bronze → Silver → Gold medallion pipeline |
| [`LDP_Basics/`](./LDP_Basics/) | Lakeflow SDP foundations and patterns |
| [`LDP_realworld/`](./LDP_realworld/) | Real-world ride-sharing streaming pipeline |
| [`Databricks_SQL/`](./Databricks_SQL/) | Saved queries, Alerts, Genie AI/BI space |
| [`Databricks_Job/`](./Databricks_Job/) | Parameterized notebooks, orchestration |
| [`Optimize/`](./Optimize/) | Delta Lake, time travel, DDL, cache & persist |
| [`Security/`](./Security/) | Grants, Key Vault, CLS, RLS, ABAC |

---

*Stack: Azure ADLS Gen2 · Azure Databricks · Delta Lake · PySpark · Unity Catalog · Lakeflow SDP · Azure SQL Server · Azure Key Vault · Azure Event Hubs*

<p align="center">
  <b>Thank you for visiting and exploring my project! 🚀</b>
</p>
