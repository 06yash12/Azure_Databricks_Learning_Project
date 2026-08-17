# Databricks notebook source
dbutils.widgets.text("sql_output", "")

# COMMAND ----------

# DBTITLE 1,Read all rows directly from source table
sql_output_text = dbutils.widgets.get("sql_output")
print(sql_output_text)