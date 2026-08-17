# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('adb-keyvault-yash')

# COMMAND ----------

value = dbutils.secrets.get("adb-keyvault-yash","dbcreds")
value

# COMMAND ----------

len(value)

# COMMAND ----------

# %sql
# select * from foreign_connection_catalog.dbo.ordersnew

# logs from azure - diagnostics settings ( microsoft workspace) 

# see stream logs analysis in azure - use KQL

# COMMAND ----------

