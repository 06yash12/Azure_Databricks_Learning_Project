# Databricks notebook source
dbutils.widgets.text("params","")
dbutils.widgets.text("params_list","")

# COMMAND ----------

import json

# COMMAND ----------

value = dbutils.widgets.get("params")
params = json.loads(value)
params

# COMMAND ----------

value = dbutils.widgets.get("params_list")
value_list = eval(value)
value_list