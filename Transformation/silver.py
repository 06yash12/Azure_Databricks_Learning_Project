# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.silver

# COMMAND ----------

df_customers = spark.read.table("azuredatabricks_catalog.bronze.customers")

display(df_customers)

# COMMAND ----------

display(df_customers.dropna()) # removes all null rows in any col 

# COMMAND ----------

display(df_customers.dropna(subset=['country'])) # remove null in country

# COMMAND ----------

display(df_customers.fillna("unknown")) 
# fill unknown at all null place

# COMMAND ----------

df_customers = df_customers.fillna("unknown")
df_customers = df_customers.fillna(0, subset=['age'])
display(df_customers)

# COMMAND ----------

display(df_customers.dropDuplicates())

# COMMAND ----------

df_customers = df_customers.dropDuplicates()
display(df_customers)

# COMMAND ----------

display(df_customers.dropDuplicates(subset=['email']))

# COMMAND ----------

df_india = df_customers.filter(col("country") == "India")
display(df_india)

# COMMAND ----------

display(df_customers.filter((col("country") == "India") | (col("country") == "USA")))


# COMMAND ----------

df_india_usa = df_customers.filter((col("country") == "India") | (col("country") == "USA"))
display(df_india_usa)


# COMMAND ----------

display(df_customers.filter((col("country") != "India") & (col("country") != "USA")))


# COMMAND ----------

df_india_usa_not = df_customers.filter((col("country") != "India") & (col("country") != "USA"))
display(df_india_usa_not)


# COMMAND ----------

df_combine = df_india_usa.union(df_india_usa_not)
display(df_combine)

# COMMAND ----------

# df_combine = df_india_usa.unionByName(df_india_usa_not)
# display(df_combine)

# misorgranized col - use unionByName to arrenge in col wise combine

# COMMAND ----------

display(df_customers.intersect(df_india_usa))

# COMMAND ----------

df_except = df_customers.exceptAll(df_india_usa)
display(df_except)

# COMMAND ----------

display(df_customers.select("customer_id", "customer_name", "email", "country", "age"))

# COMMAND ----------

display(df_customers.withColumn("domains",split(col("email"),"@").getItem(1)))

# COMMAND ----------

df_customers = df_customers.withColumn("flag", when(col("age") < 15, lit("junior"))
                                      .when((col("age") >= 15) & (col("age") < 30), lit("middle"))
                                      .otherwise(lit("senior")))
display(df_customers)
        

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('azuredatabricks_catalog.silver.customers'):
    delta_obj = DeltaTable.forName(spark,"azuredatabricks_catalog.silver.customers")

    delta_obj.alias("trg").merge(df_customers.alias("src"),\
        "trg.customer_id = src.customer_id")\
        .whenMatchedUpdateAll(condition="src.updated_at > trg.updated_at")\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_customers.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("azuredatabricks_catalog.silver.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC products
# MAGIC

# COMMAND ----------

df_products = spark.read.table("azuredatabricks_catalog.bronze.products")
display(df_products)


# COMMAND ----------

df_products =df_products.fillna("unkoown")
df_products =df_products.fillna(0,subset=["price"])
display(df_products)

# COMMAND ----------

Df_products = df_products.dropDuplicates()
display(Df_products)

# COMMAND ----------

df_products = df_products.withColumn("sliver_processed_at", current_timestamp())
display(df_products)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('azuredatabricks_catalog.silver.products'):
    delta_obj = DeltaTable.forName(spark,"azuredatabricks_catalog.silver.products")

    delta_obj.alias("trg").merge(df_products.alias("src"),\
        "trg.product_id = src.product_id")\
        .whenMatchedUpdateAll(condition="src.updated_at > trg.updated_at")\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_products.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("azuredatabricks_catalog.silver.products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.silver.products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.silver.products
# MAGIC WHERE product_id =1012

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM azuredatabricks_catalog.silver.products

# COMMAND ----------

# MAGIC %md
# MAGIC orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.bronze.orders

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_orders = spark.read.table("azuredatabricks_catalog.bronze.orders")


df_orders = df_orders.fillna(0, subset=["discount", "order_amount", "quantity"])
df_orders = df_orders.fillna("Unknown", subset=["payment_status"])
df_orders = df_orders.fillna("9999-01-01", subset=["order_date"])

df_orders = df_orders.dropDuplicates()

df_orders = df_orders.withColumn("silver_processed_at", current_timestamp())

display(df_orders)

# COMMAND ----------

from pyspark.sql.functions import coalesce, try_to_date, col


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, when, col, lit, coalesce, try_to_date

df_orders = spark.read.table("azuredatabricks_catalog.bronze.orders")

df_orders = df_orders.fillna(0, subset=["discount", "order_amount", "quantity"])
df_orders = df_orders.fillna("Unknown", subset=["payment_status"])

df_orders = df_orders.withColumn("order_date", 
    when(col("order_date").isin("9999-01-01", "9999-01-01 00:00:00"), lit("1999-01-01").cast("date"))
    .otherwise(
        coalesce(
            try_to_date(col("order_date"), "yyyy-MM-dd"),
            try_to_date(col("order_date"), "dd-MM-yyyy"),
            lit("1999-01-01").cast("date")
        )
    )
)

df_orders = df_orders.dropDuplicates()

df_orders = df_orders.withColumn("silver_processed_at", current_timestamp())

display(df_orders)

# COMMAND ----------

from pyspark.sql.types import TimestampType
df_orders = df_orders.withColumn("order_date",col("order_date").cast(TimestampType()) )
display(df_orders)



# COMMAND ----------

df_orders = df_orders.withColumn("updated_at", to_timestamp(from_unixtime(col("updated_at")/1000)))
display(df_orders)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('azuredatabricks_catalog.silver.orders'):
    delta_obj = DeltaTable.forName(spark,"azuredatabricks_catalog.silver.orders")

    delta_obj.alias("trg").merge(df_orders.alias("src"),\
        "trg.order_id = src.order_id")\
        .whenMatchedUpdateAll(condition="src.updated_at > trg.updated_at")\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_orders.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("azuredatabricks_catalog.silver.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.silver.orders

# COMMAND ----------

