import pyspark.sql.functions as F

df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/202108/*.parquet')
df.printSchema()
df1 = df.select('ifa', 'device_name', 'device_model', 'device_manufacturer', 'device_affluence_level', 'geohash')
df1.printSchema()

list = ['Apple iPhone', 'Samsung Galaxy S10+', 'Samsung Galaxy Note10 Lite', 'Samsung Galaxy Note10+', 'Samsung Galaxy '
                                                                                                       'Note20 Ultra 5G',
        'Samsung Galaxy S21 Ultra 5G', 'Samsung Galaxy S10', 'Samsung Galaxy S6 Edge', 'Samsung Galaxy S20 Ultra 5G',
        'Samsung Galaxy S20+', 'Samsung Galaxy S20 FE', 'Samsung Galaxy Note10', 'Samsung Galaxy S10e',
        'Samsung Galaxy S21+ 5G', 'Samsung Galaxy Note20', 'Samsung Galaxy Note10+ 5G', 'Samsung Galaxy S20+ 5G',
        'Samsung Galaxy S21 5G', 'Samsung Galaxy S10 5G', 'Samsung Galaxy S20 5G', 'Samsung Galaxy S10 Lite',
        'Samsung Galaxy Note20 5G', 'Samsung Galaxy S20']
selected_model = df1.filter((df1.device_name.isin(list)))
selected_model.show(10, False)

output = 'result/DSR-391/'

area = spark.read.csv('poi/GeoHashList_final.csv', header=True)
area.printSchema()

filted_df = area.join(selected_model, on='geohash', how='left')
filted_df.printSchema()
result = filted_df.groupBy('area', 'device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
result.coalesce(1).write.csv(output + "area_and_device_wise_count", mode='overwrite', header=True)
