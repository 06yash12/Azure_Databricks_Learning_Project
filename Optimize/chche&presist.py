# Databricks notebook source
# DBTITLE 1,Read Delta
df = spark.read.format("delta")\
        .load("abfss://raw@yashdatabricks.dfs.core.windows.net/sinks")

display(df)

# COMMAND ----------

# df.cache()
# [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Persist

# COMMAND ----------

# from pyspark.storagelevel import StorageLevel

# df_new = spark.read.format("parquet")\
#         .load("abfss://raw@yashdatabricks.dfs.core.windows.net/ext_table_exist")

# display(df_new)

# COMMAND ----------

# df_new.persist(StorageLevel.DISK_ONLY)
# display(df_new)

# COMMAND ----------

# df.unpersist()
# df_new.unpersist()

# COMMAND ----------

# from pyspark.storagelevel import StorageLevel

# df_new = spark.read.format("parquet")\
#         .load("abfss://raw@yashdatabricks.dfs.core.windows.net/ext_table_exist")

# df_new = df_new.repartition(2)

# df_new = df_new.withColumn("id", df_new.id.cast("string"))

# display(df_new)
