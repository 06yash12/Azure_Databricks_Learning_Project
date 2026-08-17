# Databricks notebook source
folders = dbutils.fs.ls("abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare")
folders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.ldp_medallion

# COMMAND ----------

for data_folder in folders:

    df = spark.read.format("csv").option("header", "true")\
        .option("inferSchema", "true")\
        .load(data_folder.path)

    df = df.withColumnRenamed(
        "updated_datetime",
        f"{data_folder.name.replace('/', '')}_updated_datetime"
    )

    df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(
            f"azuredatabricks_catalog.ldp_medallion.{data_folder.name.replace('/', '')}"
        )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table azuredatabricks_catalog.ldp_medallion.trips

# COMMAND ----------

folders = dbutils.fs.ls("abfss://raw@yashdatabricks.dfs.core.windows.net/rideshare")
for data_folder in folders:

    df = spark.read.format("csv").option("header", "true")\
            .option("inferSchema","true")\
            .load(data_folder.path)
    df = df.withColumnRenamed("updated_datetime",f"{data_folder.name.replace("/","")}_updated_datetime")
    
    if data_folder.name != 'trips/':
        df.write.format("delta")\
                .mode("overwrite")\
                .option("overwriteSchema","true")\
                .saveAsTable(f"azuredatabricks_catalog.ldp_medallion.{data_folder.name.replace("/","")}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.ldp_medallion.vehicles

# COMMAND ----------

