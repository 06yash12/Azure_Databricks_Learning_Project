# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE azuredatabricks_catalog.ingest.cats_table
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://raw@yashdatabricks.dfs.core.windows.net/standard/ctas'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM azuredatabricks_catalog.ingest.flights_json
# MAGIC WHERE loyaltypoints IS NOT NULL;

# COMMAND ----------

