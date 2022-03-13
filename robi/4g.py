# Robi 4g Projects
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys

country='BD'
time='monthly'

months=[
'202107',
'202108',
'202109',
'202110',
'202111',
'202112',
'202201',
]
path = 'result/2022/Robi/DSR-633/raw/'

# df=spark.read.parquet('/etl/data/brq/sub/connection/monthly/BD/20220101/*.parquet')
for m in months:
    print(m)
    print('Robi/Aktel')
    df=spark.read.parquet('/etl/data/brq/sub/connection/'+time+'/'+country+'/'+m+'/*.parquet')
    df = df.select('ifa', explode('req_connection')).select('ifa', 'col.*')
    robi = df.filter(df['req_carrier_name'] == 'Robi/Aktel')
    robi3g = robi.filter(robi['req_con_type_desc'] == '3G').select('ifa')
    robi4g = robi.filter(robi['req_con_type_desc'] == '4G').select('ifa')
    robi4g.write.parquet(path+"robi4G/"+m)
    print('GrameenPhone')
    gp = df.filter(df['req_carrier_name'] == 'GrameenPhone')
    gp3g = gp.filter(gp['req_con_type_desc'] == '3G').select('ifa')
    gp4g = gp.filter(gp['req_con_type_desc'] == '4G').select('ifa')
    gp4g.write.parquet(path + "gp4G/" + m)
    print('Orascom/Banglalink')
    bl = df.filter(df['req_carrier_name'] == 'Orascom/Banglalink')
    bl3g = bl.filter(bl['req_con_type_desc'] == '3G').select('ifa')
    bl4g = bl.filter(bl['req_con_type_desc'] == '4G').select('ifa')
    bl4g.write.parquet(path + "bl4g/" + m)
#sex Section
# s3://ada-bd-emr/result/2022/Robi/DSR-633/raw/robi4G/
for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    df_gender = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/gender/'+m+'/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    df_gender = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/gender/'+m+'/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    df_gender = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/gender/'+m+'/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)
#age Section
# s3://ada-bd-emr/result/2022/Robi/DSR-633/raw/robi4G/
for m in months:x
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    df_age = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/age/'+m+'/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    df_age = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/age/'+m+'/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    df_age = spark.read.parquet('s3a://ada-platform-components/demographics/output/'+country+'/age/'+m+'/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)
# Affluence
for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    device = spark.read.parquet('/etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
    device = device.select('ifa', 'device.pricegrade')
    dev = geo.join(device, on='ifa')
    aff_co = dev.withColumn("affluence", F.lit(None))
    new = aff_co.withColumn('affluence', F.when((aff_co.pricegrade ==1) , 'high').otherwise(aff_co.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade ==2) , 'mid').otherwise(new.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade == 3), 'low').otherwise(new.affluence))
    final = new.na.fill('Unknow', 'affluence')
    final = final.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    final.show(100,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    device = spark.read.parquet('/etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
    device = device.select('ifa', 'device.pricegrade')
    dev = geo.join(device, on='ifa')
    aff_co = dev.withColumn("affluence", F.lit(None))
    new = aff_co.withColumn('affluence', F.when((aff_co.pricegrade ==1) , 'high').otherwise(aff_co.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade ==2) , 'mid').otherwise(new.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade == 3), 'low').otherwise(new.affluence))
    final = new.na.fill('Unknow', 'affluence')
    final = final.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    final.show(100,0)

for m in months:
    print(m)
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    device = spark.read.parquet('/etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
    device = device.select('ifa', 'device.pricegrade')
    dev = geo.join(device, on='ifa')
    aff_co = dev.withColumn("affluence", F.lit(None))
    new = aff_co.withColumn('affluence', F.when((aff_co.pricegrade ==1) , 'high').otherwise(aff_co.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade ==2) , 'mid').otherwise(new.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade == 3), 'low').otherwise(new.affluence))
    final = new.na.fill('Unknow', 'affluence')
    final = final.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    final.show(100,0)
# app lib
master_df = spark.read.csv('/reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('/reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)
# top app
for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)
# top app
for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)

# life stage
for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/' + country + '/' + m + '/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/robi4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on='ifa').cache()
    freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
    freq_ls.show(10,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/' + country + '/' + m + '/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/gp4G/'+m+'/*.parquet')
    persona_app = geo.join(app, on='ifa').cache()
    freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
    freq_ls.show(10,0)

for m in months:
    print(m)
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/' + country + '/' + m + '/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('result/2022/Robi/DSR-633/raw/bl4g/'+m+'/*.parquet')
    persona_app = geo.join(app, on='ifa').cache()
    freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
    freq_ls.show(10,0)