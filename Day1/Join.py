# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_joined=df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"], "inner")

# COMMAND ----------

df_joined=df_sales.join(df_customers,"customer_id","inner")

# COMMAND ----------

df_customers.where("customer_id>2 or customer_city='New Michaelview'").display()

# COMMAND ----------

from pyspark.sql.functions import *
df_customers.sort(col("customer_city").desc()).display()


# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()
df_joined.groupBy(df_customers["customer_id"]).count().orderBy(df_customers["customer_id"]).display()

