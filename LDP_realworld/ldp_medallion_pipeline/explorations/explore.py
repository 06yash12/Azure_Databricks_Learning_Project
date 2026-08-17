# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.gold_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.dim_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.fact

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.*, c.* FROM azuredatabricks_catalog.ldp_medallion.fact f
# MAGIC LEFT JOIN azuredatabricks_catalog.ldp_medallion.dim_customers c 
# MAGIC ON f.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE azuredatabricks_catalog.ldp_medallion.customers
# MAGIC SET customer_name = "xyz",
# MAGIC     customers_updated_datetime = current_timestamp
# MAGIC WHERE customer_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.customers
# MAGIC WHERE customer_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azuredatabricks_catalog.ldp_medallion.dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azuredatabricks_catalog.ldp_medallion.dim_customers
# MAGIC WHERE customer_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azuredatabricks_catalog.ldp_medallion.dim_customers
# MAGIC WHERE customer_id = 1

# COMMAND ----------

