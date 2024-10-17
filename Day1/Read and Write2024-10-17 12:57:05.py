# Databricks notebook source
#df= spark.read.csv("path")
df = spark.read.csv("/Volumes/my_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df1 = spark.read.json("/Volumes/my_databricks/default/raw/products.json")
df1.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")
df1.write.mode("overwrite").saveAsTable("products")

# COMMAND ----------

df1.display()
