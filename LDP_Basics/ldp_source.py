# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.dummy.src_tbl
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO azuredatabricks_catalog.dummy.src_tbl
# MAGIC VALUES (1, 'John', 25),
# MAGIC        (2, 'Jane', 30),
# MAGIC        (3, 'Bob', 35);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.dummy.src_tbl
# MAGIC VALUES (4, 'John New', 25),
# MAGIC        (5, 'Jane New', 30),
# MAGIC        (6, 'Bob New', 35);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.dummy.src_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.dummy.src_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_baiscs.served_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_baiscs.aggregarted_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.dummy.src_tbl
# MAGIC VALUES (1, 'John', 25),
# MAGIC        (2, 'Jane', 30),
# MAGIC        (3, 'Bob', 35);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.dummy.src_tbl
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_baiscs.aggregarted_table

# COMMAND ----------

