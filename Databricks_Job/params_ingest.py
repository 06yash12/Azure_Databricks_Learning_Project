# Databricks notebook source
paths = [{
    "container":"raw",
    "folder_path":"rideshare/customers"
},
{
    "container":"raw",
    "folder_path":"rideshare/drivers"
},
{
    "container":"raw",
    "folder_path":"rideshare/trips"
},
{
    "container":"raw",
    "folder_path":"rideshare/vehicles"
},
{
    "container":"raw",
    "folder_path":"rideshare/payments"
}]

# COMMAND ----------

for i in paths:
    print(i['container'])

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "ingest_params",value = paths)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "customer_city_param",value = "Port Alex")