# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

content = dbutils.fs.ls("abfss://raw@yashdatabricks.dfs.core.windows.net/standard/autoloader_raw")

for i in content:
    dbutils.fs.cp(i.path, f"abfss://raw@yashdatabricks.dfs.core.windows.net/autoloader_raw_new/{i.name}")

# COMMAND ----------

delete_content = dbutils.fs.ls("abfss://raw@yashdatabricks.dfs.core.windows.net/autoloader_raw_new")
for i in delete_content:
    dbutils.fs.rm(i.path)

# COMMAND ----------

dbutils.fs.mkdirs("abfss://raw@yashdatabricks.dfs.core.windows.net/autoloader_raw_new/newfolder")

# COMMAND ----------

