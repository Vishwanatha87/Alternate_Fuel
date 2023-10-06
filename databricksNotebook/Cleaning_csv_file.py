# Databricks notebook source
# # df=spark.read.csv('/mnt/alternativefuels/alternativefuel/rawdata/light-duty-vehicles-2023-08-07.csv',header=True,inferSchema=True)

# file_location = "/FileStore/tables/light_duty_vehicles_2023_08_07-1.csv"
# file_type = "csv"

# # CSV options
# infer_schema = True
# first_row_is_header = True
# delimiter = ","

# # The applied options are for CSV files. For other file types, these will be ignored.
# df = spark.read.format(file_type) \
#   .option("inferSchema", infer_schema) \
#   .option("header", first_row_is_header) \
#   .option("sep", delimiter) \
#   .load(file_location)

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

df=df.withColumn('Alternative Fuel Economy City',col('Alternative Fuel Economy City').cast('int')).withColumn('Alternative Fuel Economy Highway',col('Alternative Fuel Economy Highway').cast('int')).withColumn('Conventional Fuel Economy City',col('Conventional Fuel Economy City').cast('int')).withColumn('Conventional Fuel Economy Highway',col('Conventional Fuel Economy Highway').cast('int'))

# COMMAND ----------

df=df.withColumn('Alternative Fuel Economy Combined',col('Alternative Fuel Economy Combined').cast('int')).withColumn('Conventional Fuel Economy Combined',col('Conventional Fuel Economy Combined').cast('int'))

# COMMAND ----------

df=df.withColumn("Model Year",col('Model Year').cast('int'))

# COMMAND ----------

df.select(mode(col('Model Year')).alias('model')).first()['model']

# COMMAND ----------

#df.select(mode(col('Model Year'))).first()['mode(Model Year)']

df = df.fillna(df.select(mode(col('Model Year')).alias("year")).first()['year'],subset=['Model year']).fillna(df.select(avg(col('Alternative Fuel Economy City')).alias('acity')).first()['acity'],subset=['Alternative Fuel Economy City']).fillna(df.select(avg(col('Alternative Fuel Economy Highway')).alias('ahigh')).first()['ahigh'],subset=['Alternative Fuel Economy Highway']).fillna(df.select(avg(col('Alternative Fuel Economy Combined')).alias("ac")).first()['ac'],subset=['Alternative Fuel Economy Combined']).fillna(df.select(avg(col('Conventional Fuel Economy City')).alias("ccity")).first()['ccity'],subset=['Conventional Fuel Economy City']).fillna(df.select(avg(col('Conventional Fuel Economy Highway')).alias("chigh")).first()['chigh'],subset=['Conventional Fuel Economy Highway']).fillna(df.select(avg(col('Conventional Fuel Economy Combined')).alias("cc")).first()['cc'],subset=['Conventional Fuel Economy Combined'])

# COMMAND ----------

df.columns

# COMMAND ----------

# df = df.drop('Electric-Only Range','PHEV Total Range','PHEV Type','Notes','Charging Rate Level 2 (kW)','Charging Rate DC Fast (kW)','Charging Speed Level 1 (miles added per hour of charging)','Charging Speed Level 2 (miles added per hour of charging)','Charging Speed DC Fast (miles added per hour of charging)','Battery Voltage','Battery Capacity Amp Hours','Battery Capacity kWh','Seating Capacity')

# COMMAND ----------

# Engine Type


# COMMAND ----------

#df.select(mode(col('Model Year'))).first()['mode(Model Year)']

df = df.fillna(df.select(mode(col('Transmission Type')).alias("year")).first()['year'],subset=['Transmission Type']).fillna(df.select(mode(col('Engine Type')).alias('acity')).first()['acity'],subset=['Engine Type']).fillna(df.select(mode(col('Engine Size')).alias('ahigh')).first()['ahigh'],subset=['Engine Size']).fillna(df.select(mode(col('Engine Cylinder Count')).alias("ac")).first()['ac'],subset=['Engine Cylinder Count']).fillna(df.select(mode(col('Manufacturer')).alias("ccity")).first()['ccity'],subset=['Manufacturer']).fillna(df.select(mode(col('Category')).alias("chigh")).first()['chigh'],subset=['Category']).fillna(df.select(mode(col('Fuel Code')).alias("cc")).first()['cc'],subset=['Fuel Code']).fillna(df.select(mode(col('Fuel')).alias("cc")).first()['cc'],subset=['Fuel']).fillna(df.select(mode(col('Fuel Configuration Name')).alias("cc")).first()['cc'],subset=['Fuel Configuration Name']).fillna(df.select(mode(col('Drivetrain')).alias("cc")).first()['cc'],subset=['Drivetrain'])

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.drop('Electric-Only Range','PHEV Total Range','PHEV Type','Notes','Charging Rate Level 2 (kW)','Charging Rate DC Fast (kW)','Charging Speed Level 1 (miles added per hour of charging)','Charging Speed Level 2 (miles added per hour of charging)','Charging Speed DC Fast (miles added per hour of charging)','Battery Voltage','Battery Capacity Amp Hours','Battery Capacity kWh','Seating Capacity')

# COMMAND ----------

# df=df.withColumn('Alternative Fuel Economy City',regexp_replace(col('Alternative Fuel Economy City'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Alternative Fuel Economy Highway',regexp_replace(col('Alternative Fuel Economy Highway'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Conventional Fuel Economy City',regexp_replace(col('Conventional Fuel Economy City'),'^[a-zA-Z]*$',''))
# df=df.withColumn('Conventional Fuel Economy Highway',regexp_replace(col('Conventional Fuel Economy Highway'),'^[a-zA-Z]*$',''))

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# df = df.select(col('Alternative Fuel Economy City').cast('int')).fillna(9999,subset=['Alternative Fuel Economy City'])

# COMMAND ----------

# df2.filter(col('Alternative Fuel Economy City').isNull()).count()

# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

df.coalesce(1).write.parquet("/mnt/alternativefuels/alternativefuel/processeddata/lightvehicle")

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------


