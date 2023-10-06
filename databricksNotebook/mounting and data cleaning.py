# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://alternative-fuel@sasushantteam02.blob.core.windows.net',mount_point ='/mnt/alternativefuels',extra_configs = {'fs.azure.account.key.sasushantteam02.blob.core.windows.net':dbutils.secrets.get(scope = 'team02scope', key = 'team02key')})


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

file_location = "/mnt/alternativefuels/alternativefuel/rawdata/alt_fuel_stations (Aug 7 2023).json"
file_type = "json"
infer_schema = True
first_row_is_header = True
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.withColumn('fuel_stations',expr("explode(fuel_stations)"))

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = spark.createDataFrame([],'')

# COMMAND ----------

df1=df1.withColumn('e85_other_ethanol_blends',col("fuel_stations.e85_other_ethanol_blends")).withColumn('ev_connector_types',col("fuel_stations.ev_connector_types")).withColumn('ev_network_ids_posts',col("fuel_stations.ev_network_ids.posts")).withColumn('ev_network_ids_station',col("fuel_stations.ev_network_ids.station")).withColumn('federal_agency_code',col("fuel_stations.federal_agency.code")).withColumn('federal_agency_id',col("fuel_stations.federal_agency.id")).withColumn('federal_agency_name',col("fuel_stations.federal_agency.name")).withColumn('hy_pressures',col("fuel_stations.hy_pressures")).withColumn('hy_standards',col("fuel_stations.hy_standards")).withColumn('lpg_nozzle_types',col("fuel_stations.lpg_nozzle_types"))

# COMMAND ----------

df1=df1.withColumn("access_code",df1.fuel_stations.access_code).\
    withColumn('access_days_time',df1.fuel_stations.access_days_time).\
    withColumn('access_days_time_fr',df1.fuel_stations.access_days_time_fr).\
    withColumn("access_detail_code",df1.fuel_stations.access_detail_code).\
    withColumn("bd_blends",df1.fuel_stations.bd_blends).\
    withColumn("bd_blends_fr",df1.fuel_stations.bd_blends_fr).\
    withColumn("cards_accepted",df1.fuel_stations.cards_accepted).\
    withColumn("city",df1.fuel_stations.city).\
    withColumn("cng_dispenser_num",df1.fuel_stations.cng_dispenser_num).\
    withColumn("cng_fill_type_code",df1.fuel_stations.cng_fill_type_code).\
    withColumn("cng_has_rng",df1.fuel_stations.cng_has_rng).\
    withColumn("cng_psi",df1.fuel_stations.cng_psi).\
    withColumn("cng_renewable_source",df1.fuel_stations.cng_renewable_source).\
    withColumn("cng_total_compression",df1.fuel_stations.cng_total_compression).\
    withColumn("cng_total_storage",df1.fuel_stations.cng_total_storage).\
    withColumn("cng_vehicle_class",df1.fuel_stations.cng_vehicle_class).\
    withColumn("country",df1.fuel_stations.country).\
    withColumn("date_last_confirmed",df1.fuel_stations.date_last_confirmed).\
    withColumn("e85_blender_pump",df1.fuel_stations.e85_blender_pump).\
    withColumn("ev_dc_fast_num",df1.fuel_stations.ev_dc_fast_num).\
    withColumn("ev_level1_evse_num",df1.fuel_stations.ev_level1_evse_num).\
    withColumn("ev_level2_evse_num",df1.fuel_stations.ev_level2_evse_num).\
    withColumn("ev_network",df1.fuel_stations.ev_network).\
    withColumn("ev_network_web",df1.fuel_stations.ev_network_web).\
    withColumn("ev_other_evse",df1.fuel_stations.ev_other_evse).\
    withColumn("ev_pricing",df1.fuel_stations.ev_pricing).\
    withColumn("ev_pricing_fr",df1.fuel_stations.ev_pricing_fr).\
    withColumn("ev_renewable_source",df1.fuel_stations.ev_renewable_source).\
    withColumn("expected_date",df1.fuel_stations.expected_date).\
    withColumn("facility_type",df1.fuel_stations.facility_type).\
    withColumn("fuel_type_code",df1.fuel_stations.fuel_type_code).\
    withColumn("geocode_status",df1.fuel_stations.geocode_status).\
    withColumn("groups_with_access_code",df1.fuel_stations.groups_with_access_code).\
    withColumn("groups_with_access_code_fr",df1.fuel_stations.groups_with_access_code_fr).\
    withColumn("hy_is_retail",df1.fuel_stations.hy_is_retail).\
    withColumn("hy_status_link",df1.fuel_stations.hy_status_link).\
    withColumn("id",df1.fuel_stations.id).\
    withColumn("intersection_directions",df1.fuel_stations.intersection_directions).\
    withColumn("intersection_directions_fr",df1.fuel_stations.intersection_directions_fr).\
    withColumn("latitude",df1.fuel_stations.latitude).\
    withColumn("lng_has_rng",df1.fuel_stations.lng_has_rng).\
    withColumn("lng_renewable_source",df1.fuel_stations.lng_renewable_source).\
    withColumn("lng_vehicle_class",df1.fuel_stations.lng_vehicle_class).\
    withColumn("longitude",df1.fuel_stations.longitude).\
    withColumn("lpg_primary",df1.fuel_stations.lpg_primary).\
    withColumn("maximum_vehicle_class",df1.fuel_stations.maximum_vehicle_class).\
    withColumn("ng_fill_type_code",df1.fuel_stations.ng_fill_type_code).\
    withColumn("ng_psi",df1.fuel_stations.ng_psi).\
    withColumn("ng_vehicle_class",df1.fuel_stations.ng_vehicle_class).\
    withColumn("nps_unit_name",df1.fuel_stations.nps_unit_name).\
    withColumn("open_date",df1.fuel_stations.open_date).\
    withColumn("owner_type_code",df1.fuel_stations.owner_type_code).\
    withColumn("plus4",df1.fuel_stations.plus4).\
    withColumn("rd_blended_with_biodiesel",df1.fuel_stations.rd_blended_with_biodiesel).\
    withColumn("rd_blends",df1.fuel_stations.rd_blends).\
    withColumn("rd_blends_fr",df1.fuel_stations.rd_blends_fr).\
    withColumn("rd_max_biodiesel_level",df1.fuel_stations.rd_max_biodiesel_level).\
    withColumn("restricted_access",df1.fuel_stations.restricted_access).\
    withColumn("state",df1.fuel_stations.state).\
    withColumn("station_name",df1.fuel_stations.station_name).\
    withColumn("station_phone",df1.fuel_stations.station_phone).\
    withColumn("status_code",df1.fuel_stations.status_code).\
    withColumn("street_address",df1.fuel_stations.street_address).\
    withColumn("zip",df1.fuel_stations.zip).drop(col('fuel_stations'))

# COMMAND ----------

df1 = df1.withColumn('BD',col('station_counts.fuels.BD.total')).withColumn("CNG", col("station_counts.fuels.CNG.total")).withColumn("E85", col("station_counts.fuels.E85.total")).withColumn("ELEC_stations", col("station_counts.fuels.ELEC.stations.total")).withColumn("ELEC_total", col("station_counts.fuels.ELEC.total")).withColumn("HY", col("station_counts.fuels.HY.total")).withColumn("LNG", col("station_counts.fuels.LNG.total")).withColumn("LPG", col("station_counts.fuels.LPG.total")).withColumn("RD", col("station_counts.fuels.RD.total")).withColumn("station_counts_total_sum", col("station_counts.total")).withColumn("station_locator_url", col("station_locator_url")).withColumn("total_results", col("total_results"))


# COMMAND ----------

len(df1.columns)

# COMMAND ----------

df2 = df1.drop(col('station_counts'))

# COMMAND ----------

# ev_level2_evse_num

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

len(df2.columns)

# COMMAND ----------

df2.describe().display()

# COMMAND ----------

df2=df2.drop(col('access_days_time_fr'),col('bd_blends_fr'),col('ev_pricing_fr'),col('intersection_directions_fr'),col('plus4'),col('rd_blends_fr'),col("rd_blends"),col('rd_blended_with_biodiesel'),col("owner_type_code"),col('nps_unit_name'),col("ng_vehicle_class"),col("ng_psi"),col("ng_fill_type_code"),col("maximum_vehicle_class"),col("lng_vehicle_class"),col("lng_renewable_source"),col("intersection_directions"),col("hy_status_link"),col("facility_type"),col("expected_date"),col("ev_renewable_source"),col("ev_other_evse"),col("ev_pricing"),col("ev_dc_fast_num"),col("ev_level1_evse_num"),col("cng_vehicle_class"),col("cng_total_storage"),col("cng_total_compression"),col("cng_renewable_source"),col("cng_psi"),col("cng_fill_type_code"),col("cng_dispenser_num"),col("cards_accepted"),col("bd_blends"),col("access_detail_code"),col("federal_agency_name"),col("federal_agency_id"),col("federal_agency_code"),col('rd_max_biodiesel_level'))



# COMMAND ----------

len(df2.columns)

# COMMAND ----------

df2.columns

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.describe().display()

# COMMAND ----------

df2.count()

# COMMAND ----------

#row count --- 73862

df2.filter(col("access_code").isNull()).count()

# COMMAND ----------

df2.groupBy('access_days_time').count().sort(col('count').desc()).display()

# COMMAND ----------

df2.groupBy('date_last_confirmed').count().sort(col('count').desc()).display()

# COMMAND ----------

df2.groupBy('access_code').count().display()

# COMMAND ----------

#help(df.fillna)
df2 = df2.fillna("public",subset=['access_code'])

df2 = df2.fillna("24 hours daily",subset=['access_days_time'])
#2023-08-07
df2 = df2.fillna("2023-08-07",subset=['date_last_confirmed'])

df2 = df2.fillna(2.33,subset=['ev_level2_evse_num'])

df2 = df2.fillna('ChargePoint Network',subset=['ev_network'])
#http://www.chargepoint.com/

df2 = df2.fillna('http://www.chargepoint.com/',subset=['ev_network_web'])
#GPS

df2 = df2.fillna('GPS',subset=['geocode_status'])
#Public

df2 = df2.fillna('Public',subset=['groups_with_access_code'])


#Public
df2 = df2.fillna('Public',subset=['groups_with_access_code_fr'])

df2 = df2.fillna('2021-01-27',subset=['open_date'])
#2021-01-27

df2 = df2.fillna('CA',subset=['state'])

df2 = df2.fillna('888-758-4389',subset=['station_phone'])

df2 = df2.fillna('1201 Pine St',subset=['street_address'])




# COMMAND ----------

df2.count()

# COMMAND ----------

df2.select(col("access_days_time")).distinct().display()

# COMMAND ----------

df2.select(avg(col("ev_level2_evse_num"))).display()

# COMMAND ----------

df2.select(mode(col("ev_network"))).display()

# COMMAND ----------

#ev_network_web
df2.select(mode(col("ev_network_web"))).display()

# COMMAND ----------

#geocode_status
df2.select(mode(col("geocode_status"))).display()


# COMMAND ----------

#groups_with_access_code
df2.select(mode(col("groups_with_access_code"))).display()


# COMMAND ----------

#groups_with_access_code_fr
df2.select(mode(col("groups_with_access_code_fr"))).display()

# COMMAND ----------

df2.select(mode(col("open_date"))).display()

# COMMAND ----------

df2.select(mode(col("state"))).display()

# COMMAND ----------

df2.select(mode(col("station_phone"))).display()

# COMMAND ----------

df2.select(mode(col("street_address"))).display()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2=df2.drop('e85_other_ethanol_blends','ev_connector_types','ev_network_ids_posts','ev_network_ids_station','hy_pressures','hy_standards','lpg_nozzle_types')

# COMMAND ----------

df2.coalesce(1).write.parquet("/mnt/alternativefuels/alternativefuel/processeddata/altvehiclejson")

# COMMAND ----------

dbutils.fs.ls("/mnt/alternativefuels/alternativefuel/processeddata")

# COMMAND ----------


