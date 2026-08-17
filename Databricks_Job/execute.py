# Databricks notebook source
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("tabel_name", "dim_customers")

# COMMAND ----------

fetch_tabel = dbutils.widgets.get("tabel_name")
fetch_catalog = dbutils.widgets.get("catalog_name")
fetch_schema = dbutils.widgets.get("schema_name")

# COMMAND ----------

df = spark.read.table(f"{fetch_catalog}.{fetch_schema}.{fetch_tabel}")

display(df)

# COMMAND ----------

value = dbutils.jobs.taskValues.get(taskKey="parameters",key="params")
value
