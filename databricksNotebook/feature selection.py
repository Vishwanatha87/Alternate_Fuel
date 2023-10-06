# Databricks notebook source
df = spark.read.parquet("/mnt/alternativefuels/alternativefuel/processeddata/")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


