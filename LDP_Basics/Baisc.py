# Databricks notebook source
# DBTITLE 1,Create source_a
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ldp_basics.source_a
# MAGIC
# MAGIC (
# MAGIC     id INT,
# MAGIC     email STRING,
# MAGIC     phone STRING
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create source_b
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ldp_basics.source_b
# MAGIC
# MAGIC (
# MAGIC     id INT,
# MAGIC     email STRING,
# MAGIC     phone STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Insert into source_a and source_b
# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ldp_basics.source_a
# MAGIC VALUES 
# MAGIC ( 1,'john.smith@example.com','123-456-7890'),
# MAGIC ( 2,'jane.doe@example.com','987-654-3210'),
# MAGIC ( 3,'bob.smith@example.com','555-123-4567');
# MAGIC     
# MAGIC INSERT INTO azuredatabricks_catalog.ldp_basics.source_b
# MAGIC VALUES 
# MAGIC ( 4,'jane.doe@example.com','987-654-3210'),
# MAGIC ( 5,'jane.doe@example.com','987-654-3210'),
# MAGIC ( 6,'john.smith@example.com','123-456-7890');
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.read.format("delta")\
.load("abfss://raw@yashdatabricks.dfs.core.windows.net/sinks")
display(df)

# COMMAND ----------

source_param = spark.conf.get("source_table")

display(
    spark.readStream.table(source_param)
)

# COMMAND ----------

