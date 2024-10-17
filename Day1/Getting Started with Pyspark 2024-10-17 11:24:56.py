# Databricks notebook source


# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

data=[(1,'a',20) , (2,'b', 30)]
df=spark.createDataFrame(data)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------

data=[(1,'a',20) , (2,'b', 30)]
schema = "id int, name string, age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Functions
# MAGIC Col
# MAGIC
# MAGIC DataFrame functions
# MAGIC .select
# MAGIC .alias
# MAGIC .withColumnRenamed
# MAGIC .withColumnsRenamed
# MAGIC .withColumn
# MAGIC

# COMMAND ----------

df.select("*")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1 = df.select("id", "age")

# COMMAND ----------

df1.display()

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id", "emp_id").display()

# COMMAND ----------

df_new=df.withColumnsRenamed({"id":"emp_id", "name":"emp_name", "age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date", current_date()).display()

# COMMAND ----------

df.withColumn("current_age", current_age()).display()
