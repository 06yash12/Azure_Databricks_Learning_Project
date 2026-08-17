# Databricks notebook source
dbutils.widgets.text("container","")
dbutils.widgets.text("folder_path","")

# COMMAND ----------

container = dbutils.widgets.get("container")
folder_path = dbutils.widgets.get("folder_path")

# COMMAND ----------

# df = spark.read.format("csv")\
#         .option("header", "true")\
#         .option("inferSchema", "true")\
#         .load(f"abfss://{container}@yashdatabricks.dfs.core.windows.net/{folder_path}")

# df.write.format("delta")\
#         .mode("overwrite")\
#         .option("path", f"abfss://{container}@yashdatabricks.dfs.core.windows.net/ingest_loop/{folder_path}")\
#         .save()