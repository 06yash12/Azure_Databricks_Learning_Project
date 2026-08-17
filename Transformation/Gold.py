# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_orders = spark.read.table("azuredatabricks_catalog.silver.orders")
df_products= spark.read.table("azuredatabricks_catalog.silver.products")
df_customers= spark.read.table("azuredatabricks_catalog.silver.customers")

display(df_orders)
display(df_products)
display(df_customers)


# COMMAND ----------

df_join = df_orders.join(df_customers, df_orders['customer_id'] == df_customers['customer_id'], 'left')\
    .join(df_products, df_orders['product_id'] == df_products['product_id'], 'left')\
    .select(
        col("orders.customer_id"),
        col("orders.discount"),
        col("orders.order_amount"),
        col("orders.order_date"),
        col("orders.order_id"),
        col("orders.payment_status"),
        col("orders.product_id"),
        col("orders.quantity"),
        col("orders.updated_at").alias("orders_updated_at"),
        col("orders.silver_processed_at").alias("orders_silver_processed_at"),
        col("customers.customer_name"),
        col("customers.email"),
        col("customers.country"),
        col("customers.signup_date"),
        col("customers.customer_type"),
        col("customers.age"),
        col("customers.updated_at").alias("customers_updated_at"),
        col("customers.flag"),
        col("products.product_name"),
        col("products.category"),
        col("products.price"),
        col("products.supplier"),
        col("products.available"),
        col("products.updated_at").alias("products_updated_at"),
        col("products.sliver_processed_at").alias("products_silver_processed_at")
    )

df_join = df_join.withColumn("gold_processed_at", current_timestamp())
display(df_join)

# COMMAND ----------

df_join.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS azuredatabricks_catalog.gold

# COMMAND ----------

# Data Writing [UPSERT]

from delta.tables import DeltaTable 

# Checking if table exists
if spark.catalog.tableExists("azuredatabricks_catalog.gold.obt"):
    delta_obj = DeltaTable.forName(spark,"azuredatabricks_catalog.gold.obt")


    delta_obj.alias("trg").merge(df_join.alias("src"), 
                                "trg.customer_id = src.customer_id AND trg.order_id = src.order_id AND trg.product_id = src.product_id")\
                            .whenMatchedUpdateAll(condition="src.orders_updated_at > trg.orders_updated_at OR src.customers_updated_at > trg.customers_updated_at OR src.products_updated_at > trg.products_updated_at")\
                            .whenNotMatchedInsertAll()\
                            .execute()

else:
    df_join.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable("azuredatabricks_catalog.gold.obt")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM azuredatabricks_catalog.gold.obt 

# COMMAND ----------

# MAGIC %md
# MAGIC Business **Views**

# COMMAND ----------

df_agg = spark.read.table("azuredatabricks_catalog.gold.obt")

df_agg = df_agg.groupBy("country","customer_type").agg(count("order_id").alias("Total_Orders"),count("customer_id").alias("Total_Customers")).sort("country")

display(df_agg)

# COMMAND ----------

df_pivot = spark.read.table("azuredatabricks_catalog.gold.obt")

df_pivot = df_pivot.groupBy("country").pivot("flag").agg(count("customer_id"))

display(df_pivot)

# COMMAND ----------

def add_prefix(param:str)->str:
    return f"yash_{param}"

# COMMAND ----------

my_udf = udf(add_prefix)

# COMMAND ----------

df_pivot = spark.read.table("azuredatabricks_catalog.gold.obt")

df_pivot = df_pivot.groupBy("country").pivot("flag").agg(count("customer_id"))

df_pivot = df_pivot.withColumn("country",my_udf("country"))
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC **SPARK SQL**

# COMMAND ----------

df = spark.sql("""
          SELECT * FROM azuredatabricks_catalog.gold.obt
          WHERE country = 'India' OR country = 'USA'
          """)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("view_temp")

# COMMAND ----------

display(spark.sql("select * from view_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://raw@yashdatabricks.dfs.core.windows.net/staging/products`

# COMMAND ----------

# MAGIC %md
# MAGIC **# Databricks Unity Catalog UDFs**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION azuredatabricks_catalog.silver.scalar_func(p_num INT)
# MAGIC RETURNS INT  
# MAGIC LANGUAGE SQL 
# MAGIC RETURN p_num*100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT azuredatabricks_catalog.silver.scalar_func(age) 
# MAGIC FROM azuredatabricks_catalog.gold.obt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION azuredatabricks_catalog.silver.scalar_func_python(p_num INT)
# MAGIC RETURNS INT 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS
# MAGIC $$
# MAGIC     
# MAGIC     if p_num:
# MAGIC         p_num = p_num*100
# MAGIC     return p_num
# MAGIC
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT azuredatabricks_catalog.silver.scalar_func_python(age) 
# MAGIC FROM azuredatabricks_catalog.gold.obt

# COMMAND ----------

# MAGIC %md
# MAGIC **## Table Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION azuredatabricks_catalog.silver.table_func(p_age INT)
# MAGIC RETURNS TABLE 
# MAGIC RETURN (
# MAGIC     SELECT * FROM azuredatabricks_catalog.gold.obt WHERE age > p_age
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM azuredatabricks_catalog.silver.table_func(30)

# COMMAND ----------

