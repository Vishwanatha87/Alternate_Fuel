# Databricks notebook source
df=spark.read.parquet('/mnt/alternativefuels/alternativefuel/processeddata/altvehiclejson/',header=True,inferSchema=True)

# COMMAND ----------

df = df.select('city','country','fuel_type_code','id','state','station_name','BD','station_counts_total_sum','total_results')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.coalesce(1).write.option("header",True).csv("/mnt/alternativefuels/alternativefuel/finaldata/altjson/")

# COMMAND ----------

df1 = spark.read.parquet('/mnt/alternativefuels/alternativefuel/processeddata/lightvehicle/',header=True,inferSchema=True)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1 = df1.drop('Fuel ID','Fuel Configuration ID','Manufacturer URL')

# COMMAND ----------

dbutils.fs.ls("/mnt/alternativefuels/alternativefuel/")

# COMMAND ----------

df1.coalesce(1).write.option("header",True).csv("/mnt/alternativefuels/alternativefuel/finaldata/lightvehicle/")

# COMMAND ----------


