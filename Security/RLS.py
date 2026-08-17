# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.gold.mapping_rls
# MAGIC (
# MAGIC     email STRING,
# MAGIC     region STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO azuredatabricks_catalog.gold.mapping_rls
# MAGIC VALUES
# MAGIC (
# MAGIC     "yaash1207@outlook.com","east"
# MAGIC ),
# MAGIC (
# MAGIC     "adb_user@yashlambaazgmail.onmicrosoft.com","west"
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.gold.orders_rls
# MAGIC (
# MAGIC     id INT,
# MAGIC     amount INT,
# MAGIC     region STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO azuredatabricks_catalog.gold.orders_rls
# MAGIC VALUES
# MAGIC (
# MAGIC     1,100,"east"
# MAGIC ),
# MAGIC (
# MAGIC     2,200,"west"
# MAGIC ),
# MAGIC (
# MAGIC     3,300,"east"
# MAGIC ),
# MAGIC (
# MAGIC     4,400,"west"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.mapping_rls

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION azuredatabricks_catalog.gold.rls_filter(p_region STRING)
# MAGIC RETURNS BOOLEAN 
# MAGIC RETURN   
# MAGIC EXISTS 
# MAGIC (
# MAGIC     SELECT * FROM azuredatabricks_catalog.gold.mapping_rls 
# MAGIC     WHERE email = current_user() 
# MAGIC     AND region = lower(p_region)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE azuredatabricks_catalog.gold.orders_rls
# MAGIC SET ROW FILTER azuredatabricks_catalog.gold.rls_filter ON (region)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.orders_rls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC EXISTS 
# MAGIC (
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.mapping_rls
# MAGIC WHERE email = current_user()
# MAGIC AND region = "west"
# MAGIC )

# COMMAND ----------

