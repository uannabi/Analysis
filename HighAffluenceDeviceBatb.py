import pyspark.sql.functions as F
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
spark = SparkSession.builder.appName("Games").getOrCreate()
df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/202108/*.parquet')
df.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- device_name: string (nullable = true)
#  |-- device_model: string (nullable = true)
#  |-- device_year_of_release: string (nullable = true)
#  |-- price: float (nullable = true)
#  |-- pricegrade: string (nullable = true)
#  |-- device_manufacturer: string (nullable = true)
#  |-- platform: string (nullable = true)
#  |-- major_os: string (nullable = true)
#  |-- device_affluence_id: string (nullable = true)
#  |-- device_affluence_level: string (nullable = true)
#  |-- poi_affluence_id: string (nullable = true)
#  |-- poi_name: string (nullable = true)
#  |-- unique_id: string (nullable = true)
#  |-- level_id: string (nullable = true)
#  |-- latitude: string (nullable = true)
#  |-- longitude: string (nullable = true)
#  |-- radius: string (nullable = true)
#  |-- geohash: string (nullable = true)
#  |-- affluence_id: string (nullable = true)
#  |-- poi_affluence_level: string (nullable = true)
#  |-- affluence_brq_count: integer (nullable = true)
#  |-- geohash_6: string (nullable = true)
#  |-- home_geohash: string (nullable = true)
#  |-- property_name: string (nullable = true)
#  |-- property_affluence_level: string (nullable = true)
#  |-- property_affluence_id: string (nullable = true)
#  |-- state_only: string (nullable = true)
#  |-- property_score: integer (nullable = true)
#  |-- device_score: integer (nullable = true)
#  |-- poi_score: integer (nullable = true)
#  |-- total_score: float (nullable = true)
#  |-- final_score: float (nullable = true)
#  |-- final_affluence: string (nullable = true)
#  |-- indicators: string (nullable = true)
#  |-- final_affluence_id: string (nullable = true)

geo=spark.read.csv('poi/GeoHashList.csv',header=True)
geo.printSchema()
# root
#  |-- geohas_6: string (nullable = true)
df1=df.select('ifa','device_name','device_model','device_manufacturer','device_affluence_level','geohash')
df1.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- device_name: string (nullable = true)
#  |-- device_model: string (nullable = true)
#  |-- device_manufacturer: string (nullable = true)
#  |-- device_affluence_level: string (nullable = true)
#  |-- geohash: string (nullable = true)

df_highPrice=df1.filter(df1['price']>=550)
df_highPrice.show(10,False)
# +------------------------------------+--------------------------+------------+-------------------+----------------------+------+-------+
# |ifa                                 |device_name               |device_model|device_manufacturer|device_affluence_level|price |geohash|
# +------------------------------------+--------------------------+------------+-------------------+----------------------+------+-------+
# |000417c5-e80c-4682-b678-c596c9cc16fa|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |000f340e-2cde-5c15-97cd-f09061d1021e|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |000f827e-e826-48ba-9cbb-4970fd435daa|Apple iPad                |iPad        |Apple              |Ultra High            |1229.0|null   |
# |001a07d0-f344-4deb-a661-1bb0f4498da5|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |0020c624-4745-4845-b7c3-baf7e40696c5|OPPO Reno 10x Zoom        |CPH1919     |OPPO               |Ultra High            |726.0 |null   |
# |0026f776-7da7-41a7-869c-b24e105ab76b|Samsung Galaxy Note10 Lite|SM-N770F    |Samsung            |Ultra High            |575.0 |null   |
# |0027226b-926c-4750-b538-8bdf8d031aae|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |0030dca2-cbbd-4b3e-9f5f-ebe4c2cb6480|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |00385469-2013-46b1-9f03-9a35cac45cf3|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# |003d4c6f-d2b0-457e-9d4c-025f96a7eeea|Apple iPhone              |iPhone      |Apple              |Ultra High            |1150.0|null   |
# +------------------------------------+--------------------------+------------+-------------------+----------------------+------+-------+
geo=geo.withColumnRenamed('geohas_6','geohash')
my_df=geo.join(df_highPrice,on='geohash',how='left')
my_df.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',ascending=False).show()
# +--------------------+----+
# |         device_name| ifa|
# +--------------------+----+
# |        Apple iPhone|9039|
# |  Samsung Galaxy S8+|1253|
# |  Samsung Galaxy S9+|1200|
# |Samsung Galaxy S7...|1084|
# | Samsung Galaxy S10+|1084|
# |Samsung Galaxy Note8| 983|
# |Samsung Galaxy Note9| 949|
# |   Samsung Galaxy S8| 830|
# |Samsung Galaxy No...| 807|
# |Samsung Galaxy No...| 747|
# |Samsung Galaxy No...| 722|
# |          Apple iPad| 628|
# |          OnePlus 6T| 588|
# |           OnePlus 6| 577|
# |          OnePlus 8T| 556|
# |          OnePlus 7T| 556|
# |       OnePlus 7 Pro| 547|
# |            Realme 6| 496|
# |Samsung Galaxy S2...| 495|
# |           OnePlus 8| 409|
# +--------------------+----+
my_df.distinct().count()
# 32006
handset_list=my_df.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',ascending=False)

output='result/DSR-391/'
handset_list.coalesce(1).write.csv(output + "device_list", mode='overwrite', header=True)

geohas_wise_count=my_df.groupBy('geohash').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',ascending=False)

poi=spark.read.csv('result/DSR-391/fullview-geofence/*.csv',header=True)

area = spark.read.csv('poi/GeoHashList_final.csv', header=True)
df_highPrice.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- device_name: string (nullable = true)
#  |-- device_model: string (nullable = true)
#  |-- device_manufacturer: string (nullable = true)
#  |-- device_affluence_level: string (nullable = true)
#  |-- price: float (nullable = true)
#  |-- geohash: string (nullable = true)

area.printSchema()
# root
#  |-- geohash: string (nullable = true)
#  |-- area: string (nullable = true)

df_new=area.join(df_highPrice,on='geohash',how='left')
df_new.printSchema()
# root
#  |-- geohash: string (nullable = true)
#  |-- area: string (nullable = true)
#  |-- ifa: string (nullable = true)
#  |-- device_name: string (nullable = true)
#  |-- device_model: string (nullable = true)
#  |-- device_manufacturer: string (nullable = true)
#  |-- device_affluence_level: string (nullable = true)
#  |-- price: float (nullable = true)
df_new.groupBy('area').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',ascending=False).show(15,False)
# +--------------+-----+
# |area          |ifa  |
# +--------------+-----+
# |Dhanmonddi    |30609|
# |Bashundhara   |11169|
# |Gulshan       |10487|
# |Banani        |9797 |
# |Uttara        |8514 |
# |Baridhara     |4711 |
# |Banani DOHS   |2900 |
# |Niketon       |2522 |
# |Mahakhali DOHS|924  |
# |Baridhara DOHS|446  |
# |Mahakhali     |41   |
# +--------------+-----+

df_new.distinct().count()
# 82132
device=df_new.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',ascending=False)
device.coalesce(1).write.csv(output + "device_list_final", mode='overwrite', header=True)






