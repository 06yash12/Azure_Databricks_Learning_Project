# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.ldp_basics;

# COMMAND ----------

# DBTITLE 1,Show tables in ldp_basics
# MAGIC %sql
# MAGIC SHOW TABLES IN azuredatabricks_catalog.ldp_basics;

# COMMAND ----------

# DBTITLE 1,Query information_schema for ldp_basics
# MAGIC %sql
# MAGIC SELECT table_name, table_type
# MAGIC FROM azuredatabricks_catalog.information_schema.tables
# MAGIC WHERE table_schema = 'ldp_basics'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Select from events_table_new in ldp_basics
# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_basics.events_table_new

# COMMAND ----------

