# Databricks notebook source
df=spark.read.csv('/mnt/alternativefuels/alternativefuel/rawdata/light-duty-vehicles-2023-08-07.csv',header=True,inferSchema=True)

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import *
df = df.filter(col("Vehicle ID").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Fuel ID").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Fuel Configuration ID").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Manufacturer ID").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Category ID").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Model Year").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Alternative Fuel Economy City").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Alternative Fuel Economy Highway").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Alternative Fuel Economy Combined").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Conventional Fuel Economy City").rlike('^[0-9]*[.]?[0-9]*$'))
# df= df.filter(col("Conventional Fuel Economy Highway").rlike('^[0-9]*[.]?[0-9]*$'))
# df = df.filter(col("Conventional Fuel Economy Combined").rlike('^[0-9]*[.]?[0-9]*$'))


# COMMAND ----------

help(regexp_replace)

# COMMAND ----------

df.withColumn('Alternative Fuel Economy City',col('Alternative Fuel Economy City').cast('int')).withColumn('Alternative Fuel Economy Highway',col('Alternative Fuel Economy Highway').cast('int')).withColumn('Conventional Fuel Economy City',col('Conventional Fuel Economy City').cast('int')).withColumn('Conventional Fuel Economy Highway',col('Conventional Fuel Economy Highway').cast('int'))

# COMMAND ----------

# df=df.withColumn('Alternative Fuel Economy City',regexp_replace(col('Alternative Fuel Economy City'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Alternative Fuel Economy Highway',regexp_replace(col('Alternative Fuel Economy Highway'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Conventional Fuel Economy City',regexp_replace(col('Conventional Fuel Economy City'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Conventional Fuel Economy Highway',regexp_replace(col('Conventional Fuel Economy Highway'),'^[a-zA-Z]*$',''))

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df = df.select(col('Alternative Fuel Economy City').cast('int')).fillna(9999,subset=['Alternative Fuel Economy City'])

# COMMAND ----------

df2.filter(col('Alternative Fuel Economy City').isNull()).count()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


