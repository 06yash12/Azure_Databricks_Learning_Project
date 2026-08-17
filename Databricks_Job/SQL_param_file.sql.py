# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_city
# MAGIC FROM azuredatabricks_catalog.ldp_medallion.dim_customers
# MAGIC WHERE customer_city = :customer_city_param
# MAGIC LIMIT 10