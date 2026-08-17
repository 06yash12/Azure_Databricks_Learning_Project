# Databricks notebook source
params = {
    "catalog_name":"azuredatabricks_catalog",
    "schema_name":"ldp_medallion",
    "table_name":"dim_customers"
}

# COMMAND ----------

dbutils.jobs.taskValues.set(key="catalog_name",value=params.get('catalog_name'))
dbutils.jobs.taskValues.set(key="schema_name",value=params.get('schema_name'))
dbutils.jobs.taskValues.set(key="table_name",value=params.get('table_name'))

# COMMAND ----------

params_list = [{
    "catalog_name":"azuredatabricks_catalog",
    "schema_name":"ldp_medallion",
    "table_name":"dim_customers"
},
    {
    "catalog_name":"azuredatabricks_catalog",
    "schema_name":"ldp_medallion",
    "table_name":"dim_drivers"
},
    {
    "catalog_name":"azuredatabricks_catalog",
    "schema_name":"ldp_medallion",
    "table_name":"dim_vehicles"
}]

# COMMAND ----------

import json

dbutils.jobs.taskValues.set(
    key="params",
    value=json.dumps(params)
)

dbutils.jobs.taskValues.set(
    key="params_list",
    value=json.dumps(params_list)
)

# COMMAND ----------

params['catalog_name']

# COMMAND ----------

dbutils.jobs.taskValues.set(key="catalag_name",value=params.get('catalog_name'))