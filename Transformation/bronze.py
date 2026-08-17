# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.bronze

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_customers = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://raw@yashdatabricks.dfs.core.windows.net/staging/customers")
display(df_customers)

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

df_customers.schema

# COMMAND ----------

#str schema
my_struct_schema = StructType([StructField('customer_id', IntegerType(), True), StructField('customer_name', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True), StructField('signup_date', DateType(), True), StructField('customer_type', StringType(), True), StructField('age', DoubleType(), True), StructField('updated_at', TimestampType(), True)])


##DDL
my_ddl_schema = """
customer_id STRING,
customer_name STRING,
email STRING,
country STRING,
signup_date DATE,
customer_type STRING,
age DOUBLE,
updated_at TIMESTAMP"""

# COMMAND ----------

df_customers = spark.read.format("csv")\
    .option("header", "true")\
    .schema(my_struct_schema)\
    .load("abfss://raw@yashdatabricks.dfs.core.windows.net/staging/customers")
display(df_customers)

# COMMAND ----------

df_customers = spark.read.format("csv")\
    .option("header", "true")\
    .schema(my_ddl_schema)\
    .load("abfss://raw@yashdatabricks.dfs.core.windows.net/staging/customers")
display(df_customers)

# COMMAND ----------

df_customers.write.format("delta")\
    .mode("append")\
    .saveAsTable("azuredatabricks_catalog.bronze.customers")

# COMMAND ----------

df_products = spark.read.parquet(
    "abfss://raw@yashdatabricks.dfs.core.windows.net/staging/products"
)

df_products.write.format("delta")\
    .mode("append")\
    .saveAsTable("azuredatabricks_catalog.bronze.products")

#old code for products - single file before adding new file 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO azuredatabricks_catalog.bronze.products
# MAGIC FROM 'abfss://raw@yashdatabricks.dfs.core.windows.net/staging/products'
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

df_orders = spark.read.format("json")\
    .option("multiline", "true")\
    .option("inferSchema", "true")\
    .load("abfss://raw@yashdatabricks.dfs.core.windows.net/staging/orders")
display(df_orders)

df_orders.write.format("delta")\
    .mode("append")\
    .saveAsTable("azuredatabricks_catalog.bronze.orders")

# COMMAND ----------

df_orders.schema

# COMMAND ----------

