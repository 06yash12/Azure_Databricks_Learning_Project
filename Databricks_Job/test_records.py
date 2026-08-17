# Databricks notebook source
df =  spark.read.table("azuredatabricks_catalog.ldp_medallion.dim_customers")
df.count()

# COMMAND ----------

dbutils.jobs.taskValues.set("records_count", df.count())