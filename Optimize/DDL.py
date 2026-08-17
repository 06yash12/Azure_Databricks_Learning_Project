# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.dummy.src_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.ldp_baiscs;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS azuredatabricks_catalog.lap_baiscs;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE azuredatabricks_catalog.dummy.src_tbl;

# COMMAND ----------

df_customers = spark.read.table("azuredatabricks_catalog.bronze.customers")
display(df_customers)

# COMMAND ----------

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("azuredatabricks_catalog.dummy.src_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS azuredatabricks_catalog.dummy.src_tbl;

# COMMAND ----------

df_customers = spark.read.table("azuredatabricks_catalog.bronze.customers")

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("azuredatabricks_catalog.dummy.src_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE azuredatabricks_catalog.dummy.src_tbl;

# COMMAND ----------

