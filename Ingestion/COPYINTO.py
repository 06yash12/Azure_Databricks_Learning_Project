# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE azuredatabricks_catalog.ingest.flights_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO azuredatabricks_catalog.ingest.flights_json
# MAGIC FROM 'abfss://raw@yashdatabricks.dfs.core.windows.net/standard/copyinto'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('meargeSchema' = 'true', 'multiLine' = 'true')
# MAGIC COPY_OPTIONS('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ingest.flights_json

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO azuredatabricks_catalog.ingest.flights_json
# MAGIC FROM 'abfss://raw@yashdatabricks.dfs.core.windows.net/standard/copyinto'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('meargeSchema' = 'true', 'multiLine' = 'true')
# MAGIC COPY_OPTIONS('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ingest.flights_json

# COMMAND ----------

