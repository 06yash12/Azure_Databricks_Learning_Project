# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE azuredatabricks_catalog.gold.employees
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     salary FLOAT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO azuredatabricks_catalog.gold.employees
# MAGIC VALUES
# MAGIC (1, 'John', 100000),
# MAGIC (2, 'Jane', 120000),
# MAGIC (3, 'Bob', 80000);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION azuredatabricks_catalog.gold.salary_mask(salary FLOAT)
# MAGIC RETURN CASE WHEN is_account_group_member('admin') THEN salary ELSE 0 END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE azuredatabricks_catalog.gold.employees 
# MAGIC ALTER COLUMN salary
# MAGIC SET MASK azuredatabricks_catalog.gold.salary_mask;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE azuredatabricks_catalog.gold.employees 
# MAGIC ALTER COLUMN salary
# MAGIC DROP MASK

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.gold.employees

# COMMAND ----------

