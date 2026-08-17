# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ingest.sales_tbl
# MAGIC (
# MAGIC     sale_id INT,
# MAGIC     store_id INT,
# MAGIC     sale_qty INT,
# MAGIC     amount float
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/sales_tbl"
# MAGIC TBLPROPERTIES(
# MAGIC     delta.enableDeletionVectors = 'false'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ingest.sales_tbl
# MAGIC VALUES (    
# MAGIC     1, 1, 10, 100),
# MAGIC        (2, 1, 20, 200),
# MAGIC        (3, 2, 30, 300),
# MAGIC        (4, 2, 40, 400),
# MAGIC        (5, 3, 50, 500);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ingest.sales_tbl
# MAGIC VALUES (6, 1, 10, 100),
# MAGIC        (7, 1, 20, 200),
# MAGIC        (8, 2, 30, 300),
# MAGIC        (9, 2, 40, 400),
# MAGIC        (10, 3, 50, 500);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE azuredatabricks_catalog.ingest.sales_tbl
# MAGIC SET sale_qty = sale_qty + 100
# MAGIC WHERE sale_id=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM azuredatabricks_catalog.ingest.sales_tbl
# MAGIC WHERE sale_id=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from azuredatabricks_catalog.ingest.sales_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY azuredatabricks_catalog.ingest.sales_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ingest.sales_tbl VERSION AS OF 4

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE azuredatabricks_catalog.ingest.sales_tbl TO VERSION AS OF 4

# COMMAND ----------

# MAGIC %md
# MAGIC # Deletion Vector

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ingest.sales_tbl2  
# MAGIC (
# MAGIC     sale_id INT,
# MAGIC     store_id INT,
# MAGIC     sale_qty INT,
# MAGIC     amount FLOAT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/sales_tbl2"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ingest.sales_tbl2
# MAGIC VALUES (6, 1, 10, 100),
# MAGIC        (7, 1, 20, 200),
# MAGIC        (8, 2, 30, 300),
# MAGIC        (9, 2, 40, 400),
# MAGIC        (10, 3, 50, 500);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE azuredatabricks_catalog.ingest.sales_tbl2
# MAGIC SET sale_qty = sale_qty + 100
# MAGIC WHERE sale_id=6;

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/part-00000-c09e6fb3-c8d9-4eee-bd2b-7343da2df9e5.c000.snappy.parquet")
display(df)   

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM azuredatabricks_catalog.ingest.sales_tbl DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ingest.sales_part 
# MAGIC (
# MAGIC     sale_id INT,
# MAGIC     store_id INT,
# MAGIC     sale_qty INT,
# MAGIC     amount FLOAT,
# MAGIC     year INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/sales_part"
# MAGIC PARTITIONED BY (year)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ingest.sales_part
# MAGIC VALUES (11, 1, 10, 100, 2022),
# MAGIC        (12, 1, 20, 200, 2022),
# MAGIC        (13, 2, 30, 300, 2022),
# MAGIC        (14, 2, 40, 400, 2022),
# MAGIC        (15, 3, 50, 500, 2026),
# MAGIC        (16, 3, 60, 600, 2026),
# MAGIC        (17, 4, 70, 700, 2026),
# MAGIC        (18, 4, 80, 800, 2026);
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ingest.sales_part
# MAGIC WHERE year = 2026

# COMMAND ----------

# MAGIC %md
# MAGIC # OPTIMIZE
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE azuredatabricks_catalog.ingest.sales_part

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE azuredatabricks_catalog.ingest.sales_part ZORDER BY (store_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS azuredatabricks_catalog.ingest.sales_clustered
# MAGIC (
# MAGIC     sale_id INT,
# MAGIC     store_id INT,
# MAGIC     sale_qty INT,
# MAGIC     amount FLOAT,
# MAGIC     year INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://raw@yashdatabricks.dfs.core.windows.net/optimize/sales_clustered"
# MAGIC CLUSTER BY (sale_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO azuredatabricks_catalog.ingest.sales_clustered
# MAGIC VALUES (11, 1, 10, 100, 2022),
# MAGIC        (12, 1, 20, 200, 2022),
# MAGIC        (13, 2, 30, 300, 2022),
# MAGIC        (14, 2, 40, 400, 2022),
# MAGIC        (15, 3, 50, 500, 2026),
# MAGIC        (16, 3, 60, 600, 2026),
# MAGIC        (17, 4, 70, 700, 2026),
# MAGIC        (18, 4, 80, 800, 2026);
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED azuredatabricks_catalog.ingest.sales_clustered

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE azuredatabricks_catalog.ingest.sales_part
# MAGIC REPLACE PARTITIONED BY WITH CLUSTER BY (store_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE azuredatabricks_catalog.ingest.sales_part;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE azuredatabricks_catalog.ingest.sales_part
# MAGIC CLUSTER BY NONE

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE azuredatabricks_catalog.ingest.sales_part;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED azuredatabricks_catalog.ingest.sales_part

# COMMAND ----------

