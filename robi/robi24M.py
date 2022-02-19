# pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
# pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 30G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045
# 


from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys



country='BD
time='monthly'
months =[ '202001',
'202002',
'202003',
'202004',
'202005',
'202006',
'202007',
'202008',
'202009',
'202010',
'202011',
'202012',
'202101',
'202102',
'202103',
'202104',
'202105',
'202106',
'202107',
'202108',
'202109',
'202110',
'202111',
'202112',
]
Month2=['202108',
'202109',
'202110',
'202111',
'202112',
       ]

path = '/result/2022/Robi/DSR-623/raw/'


for m in months:
    df=spark.read.parquet('etl/data/brq/sub/connection/'+time+'/'+country+'/'+m+'/*.parquet')
    connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))
    robi_user = connection.filter(connection['telco'] == 'Robi/Aktel').distinct()
    print('Robi User {}'.format(robi_user.count()))
    robi_user.write.parquet(path+"robi/"+m)
    print('Robi written on Done! for {}'.format(m))
    gp_user = connection.filter(connection['telco'] == 'GrameenPhone').distinct()
    print('GrameenPhone User {}'.format(gp_user.count()))
    gp_user.write.parquet(path+"gp/"+m)
    print('GP written on Done! for {}'.format(m))
    bl_user = connection.filter(connection['telco'] == 'Orascom/Banglalink').distinct()
    print('Banglalink User {}'.format(bl_user.count()))
    bl_user.write.parquet(path + "bl/" + m)
    print('BL written on Done! for {}'.format(m))

for m in Month2:
    print(m)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    df_gender = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/gender/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)

for m in Month2:
    print(m)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    df_gender = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/gender/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)

for m in Month2:
    print(m)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    df_gender = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/gender/*.parquet')
    df_gender = df_gender.withColumnRenamed('prediction', 'gender')
    df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)



for i in months:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.drop('prediction')
    df_age = df_age.withColumnRenamed('label', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for i in months:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.drop('prediction')
    df_age = df_age.withColumnRenamed('label', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for i in months:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.drop('prediction')
    df_age = df_age.withColumnRenamed('label', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)


for i in Month2:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for i in Month2:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

for i in Month2:
    print(i)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/age/*.parquet')
    df_age = df_age.withColumnRenamed('prediction', 'age')
    df_age_segment = geo.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)


geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/202107/*.parquet')
device = spark.read.parquet('etl/data/brq/sub/device/monthly/'+country+'/202107/*.parquet')

for m in months:
    print(m)
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    device = spark.read.parquet('etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
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
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    device = spark.read.parquet('etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
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
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    device = spark.read.parquet('etl/data/brq/sub/device/monthly/'+country+'/'+m+'/*.parquet')
    device = device.select('ifa', 'device.pricegrade')
    dev = geo.join(device, on='ifa')
    aff_co = dev.withColumn("affluence", F.lit(None))
    new = aff_co.withColumn('affluence', F.when((aff_co.pricegrade ==1) , 'high').otherwise(aff_co.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade ==2) , 'mid').otherwise(new.affluence))
    new = new.withColumn('affluence', F.when((aff_co.pricegrade == 3), 'low').otherwise(new.affluence))
    final = new.na.fill('Unknow', 'affluence')
    final = final.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    final.show(100,0)

master_df = spark.read.csv('reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)



for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)


for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    app = brq2.join(finalapp_df, on='bundle', how='left').cache()
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/' + m + '/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/' + m + '/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)

for m in months:
    print(m)
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+m+'/*.parquet')
    brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/' + m + '/*.parquet')
    persona_app = geo.join(app, on ='ifa')
    top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    top_app = top_app.filter(top_app['asn'] != 'null')
    top_app.show(20,0)


for m in months:
    print(m)
    df = spark.read.parquet('etl/data/brq/sub/connection/monthly/'+country+'/'+m+'/*.parquet')
    df2 = df.select('ifa', explode('mm_connection')).select('ifa', 'col.*')
    df3 = df2.select('ifa', 'mm_con_type_desc', 'mm_carrier_name')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
    df4=geo.join(df3,on='ifa',how='left')
    dual_simmers = df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
    dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))
    dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))
    dual_simmers.groupBy('dual_sim', 'sims').agg(F.countDistinct('ifa').alias('freq')).sort('freq',ascending=False).show(10,False)

for m in months:
    print(m)
    df = spark.read.parquet('etl/data/brq/sub/connection/monthly/'+country+'/'+m+'/*.parquet')
    df2 = df.select('ifa', explode('mm_connection')).select('ifa', 'col.*')
    df3 = df2.select('ifa', 'mm_con_type_desc', 'mm_carrier_name')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/gp/'+m+'/*.parquet')
    df4=geo.join(df3,on='ifa',how='left')
    dual_simmers = df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
    dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))
    dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))
    dual_simmers.groupBy('dual_sim', 'sims').agg(F.countDistinct('ifa').alias('freq')).sort('freq',ascending=False).show(10,False)

for m in months:
    print(m)
    df = spark.read.parquet('etl/data/brq/sub/connection/monthly/'+country+'/'+m+'/*.parquet')
    df2 = df.select('ifa', explode('mm_connection')).select('ifa', 'col.*')
    df3 = df2.select('ifa', 'mm_con_type_desc', 'mm_carrier_name')
    geo = spark.read.parquet('/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
    df4=geo.join(df3,on='ifa',how='left')
    dual_simmers = df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
    dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))
    dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))
    dual_simmers.groupBy('dual_sim', 'sims').agg(F.countDistinct('ifa').alias('freq')).sort('freq',ascending=False).show(10,False)

