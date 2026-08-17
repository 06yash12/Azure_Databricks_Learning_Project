# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS azuredatabricks_catalog.ingest.managed_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/azuredatabricks_catalog/ingest/managed_volume/rawdata/flights.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS azuredatabricks_catalog.ingest.external_volume
# MAGIC LOCATION 'abfss://raw@yashdatabricks.dfs.core.windows.net/standard/volume_dir';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`/Volumes/azuredatabricks_catalog/ingest/external_volume/flights.csv`

# COMMAND ----------

