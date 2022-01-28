# pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
# pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 30G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045


from pyspark.sql.functions import *

import pyspark.sql.functions as F

import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
spark = SparkSession.builder.appName("Games").getOrCreate()

country = 'BD'
year = '202106'

data_list = ['001','002','003','004','005','006','007','008','009','010','011','012', '101', '102', '103', '104', '105','106']

master_df = spark.read.csv('reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)
url = 's3a://ada-bd-emr/result/20210608/Monthly'

for d in data_list:
    path = 'etl/data/brq/agg/agg_brq/monthly/BD/202' + d + '/*.parquet'
    df = spark.read.parquet(path)
    persona = df.select('ifa')
    join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
    join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
    select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
    finalapp_df = join_df2.select(*select_columns)
    brq = df.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = brq.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = persona.join(app, on='ifa')
    col = ['ifa', 'app_l1_name']
    persona_app1 = persona_app.select(*col)
    persona_app2 = persona_app1.filter(persona_app1['app_l1_name'] == 'Games')
    persona_app3 = persona_app2.select('ifa')
    persona_app3.write.csv(url + 'Site' + d, mode='overwrite', header=True)

Month = ['MonthlySite012', 'MonthlySite101', 'MonthlySite102', 'MonthlySite103', 'MonthlySite104', 'MonthlySite105']

for m in Month:
    print('+=============' + m + '===============')
    df = spark.read.csv('s3a://ada-bd-emr/result/20210608/' + m + '/*.csv', header=True) #gameing segment
    length = len(m)
    last_three = m[length - 3:]
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/BD/202' + last_three + '/*.parquet') #full brq data
    connection_d = brq.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
    connection = df.join(connection_d, on='ifa', how='left')
    print('++++++++++ Connection Distribution ++++++++++')
    robi_user = connection.filter(connection['telco'] == 'Robi/Aktel').distinct()
    print('Robi User {}'.format(robi_user.count()))
    gp_user = connection.filter(connection['telco'] == 'GrameenPhone').distinct()
    print('GrameenPhone User {}'.format(gp_user.count()))
    bl_user = connection.filter(connection['telco'] == 'Orascom/Banglalink').distinct()
    print('Banglalink User {}'.format(bl_user.count()))
    print('---------- Connection Distribution Done ---------')
    print('++++++++++ Age Distribution ++++++++++')
    df_age = spark.read.parquet(
        'etl/table/brq/sub/demographics/monthly/' + country + '/' + year + '/age/*.parquet')
    df_age = df_age.drop('prediction')
    df_age = df_age.withColumnRenamed('label', 'age')
    df_age_segment = df.join(df_age, ['ifa'], how='left')
    df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(20, False)
    print('---------- Age Distribution Done ---------')
    print('++++++++++ Sex Distribution ++++++++++')
    df_gender = spark.read.parquet(
        'etl/table/brq/sub/demographics/monthly/' + country + '/' + year + '/gender/*.parquet')
    df_gender = df_gender.drop('prediction')
    df_gender = df_gender.withColumnRenamed('label', 'gender')
    df_gender_segment = df.join(df_gender, ['ifa'], how='left')
    df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(20, False)
    print('---------- Sex Distribution Done ---------')
    df1 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = df1.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = df.join(app, on='ifa')
    print('++++++++++ LifeStage ++++++++++')
    freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',
                                                                                                  ascending=True)
    freq_ls.show(10, False)
    print('---------- Lifestage Done ---------')
    print('++++++++++ Top category ++++++++++')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq',
                                                                                                 ascending=False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(10, False)
    print('---------- TOP category Done ---------')
    print('++++++++++ Top Game ++++++++++')
    TopGame = persona_app.filter(persona_app['app_l1_name'] == 'Games')
    topGame = TopGame.groupBy('app_l2_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    topGame1 = topGame.filter(topGame['app_l2_name'] != 'null')
    topGame1.show(10, False)
    print('---------- TOP game Done ---------')
    print('++++++++++ Top Handset ++++++++++')
    deviceInfo = brq.select('ifa', 'device.device_name', 'device.device_model')
    topDevice = deviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                                ascending=False)
    topDevice.show(20, False)
    print('---------- TOP Handset Done ---------')
    print('xxxxxxxxxxxxxx' + m + ' Done xxxxxxxxxxxxxxx')
# time calculation
for m in Month:
    print('+============= ' + m + ' ===============')
    df = spark.read.csv('s3a://ada-bd-emr/result/20210608/' + m + '/*.csv', header=True)
    length = len(m)
    last_three = m[length - 3:]
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/BD/202' + last_three + '/*.parquet')
    df1 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = df1.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = df.join(app, on='ifa')
    diff_secs_col = col("last_seen").cast("long") - col("first_seen").cast("long")
    delta1 = persona_app.withColumn("diff_hours", diff_secs_col / 3600)
    delta2 = delta1.select('ifa',F.round(delta1["diff_hours"], 0).alias("spent"))
    time_df = delta2.groupBy('spent').agg(F.countDistinct('ifa').alias('count')).sort('spent', ascending=False)
    time_df.show(25, False)
    print('xxxxxxxxxxxxxx ' + m + ' Done xxxxxxxxxxxxxxx')
#TOP APP
for m in Month:
    print('+============= ' + m + ' ===============')
    df = spark.read.csv('s3a://ada-bd-emr/result/20210608/' + m + '/*.csv', header=True)
    length = len(m)
    last_three = m[length - 3:]
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/BD/202' + last_three + '/*.parquet')
    df1 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = df1.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = df.join(app, on='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq',
                                                                                                 ascending=False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    list_df = freq_beh1.select('app_l1_name').collect()
    list1 = list_df[1].app_l1_name
    list2 = list_df[2].app_l1_name
    list3 = list_df[3].app_l1_name
    list4 = list_df[4].app_l1_name
    list5 = list_df[5].app_l1_name
    print(m + ' of top app')
    topApp_df1 = app.filter((app.app_l2_name.isin(list1)) | (app.app_l1_name.isin(list1)))
    persona_app1 = df.join(topApp_df1, on='ifa').cache()
    top_app1 = persona_app1.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app1 = top_app1.filter(top_app1['asn'] != 'null')
    top_app1 = top_app1.withColumnRenamed('asn', list1)
    top_app1.show(20, False)
    topApp_df2 = app.filter((app.app_l2_name.isin(list2)) | (app.app_l1_name.isin(list2)))
    persona_app2 = df.join(topApp_df2, on='ifa').cache()
    top_app2 = persona_app2.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app2 = top_app2.filter(top_app2['asn'] != 'null')
    top_app2 = top_app2.withColumnRenamed('asn', list2)
    top_app2.show(20, False)
    topApp_df3 = app.filter((app.app_l2_name.isin(list3)) | (app.app_l1_name.isin(list3)))
    persona_app3 = df.join(topApp_df3, on='ifa').cache()
    top_app3 = persona_app3.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app3 = top_app3.filter(top_app3['asn'] != 'null')
    top_app3 = top_app3.withColumnRenamed('asn', list3)
    top_app3.show(20, False)
    topApp_df4 = app.filter((app.app_l2_name.isin(list4)) | (app.app_l1_name.isin(list4)))
    persona_app4 = df.join(topApp_df4, on='ifa').cache()
    top_app4 = persona_app4.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app4 = top_app4.filter(top_app4['asn'] != 'null')
    top_app4 = top_app4.withColumnRenamed('asn', list4)
    top_app4.show(20, False)
    topApp_df5 = app.filter((app.app_l2_name.isin(list5)) | (app.app_l1_name.isin(list5)))
    persona_app5 = df.join(topApp_df5, on='ifa').cache()
    top_app5 = persona_app5.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app5 = top_app5.filter(top_app5['asn'] != 'null')
    top_app5 = top_app5.withColumnRenamed('asn', list5)
    top_app5.show(20, False)
    print('xxxxxxxxxxxxxx ' + m + ' Done xxxxxxxxxxxxxxx')
#TOP GAME
for m in Month:
    print('+============= ' + m + ' ===============')
    df = spark.read.csv('s3a://ada-bd-emr/result/20210608/' + m + '/*.csv', header=True)
    length = len(m)
    last_three = m[length - 3:]
    brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/BD/202' + last_three + '/*.parquet')
    df1 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = df1.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = df.join(app, on='ifa')
    TopGame = persona_app.filter(persona_app['app_l1_name'] == 'Games')
    topGame = TopGame.groupBy('app_l2_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    topGame1 = topGame.filter(topGame['app_l2_name'] != 'null')
    list_df = topGame1.select('app_l2_name').collect()
    list1 = list_df[1].app_l2_name
    list2 = list_df[2].app_l2_name
    list3 = list_df[3].app_l2_name
    list4 = list_df[4].app_l2_name
    list5 = list_df[5].app_l2_name
    print(m + ' of top app')
    topApp_df1 = app.filter((app.app_l2_name.isin(list1)))
    persona_app1 = df.join(topApp_df1, on='ifa').cache()
    top_app1 = persona_app1.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app1 = top_app1.filter(top_app1['asn'] != 'null')
    top_app1 = top_app1.withColumnRenamed('asn', list1)
    top_app1.show(10, False)
    topApp_df2 = app.filter((app.app_l2_name.isin(list2)))
    persona_app2 = df.join(topApp_df2, on='ifa').cache()
    top_app2 = persona_app2.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app2 = top_app2.filter(top_app2['asn'] != 'null')
    top_app2 = top_app2.withColumnRenamed('asn', list2)
    top_app2.show(10, False)
    topApp_df3 = app.filter((app.app_l2_name.isin(list3)))
    persona_app3 = df.join(topApp_df3, on='ifa').cache()
    top_app3 = persona_app3.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app3 = top_app3.filter(top_app3['asn'] != 'null')
    top_app3 = top_app3.withColumnRenamed('asn', list3)
    top_app3.show(10, False)
    topApp_df4 = app.filter((app.app_l2_name.isin(list4)))
    persona_app4 = df.join(topApp_df4, on='ifa').cache()
    top_app4 = persona_app4.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app4 = top_app4.filter(top_app4['asn'] != 'null')
    top_app4 = top_app4.withColumnRenamed('asn', list4)
    top_app4.show(10, False)
    topApp_df5 = app.filter((app.app_l2_name.isin(list5)))
    persona_app5 = df.join(topApp_df5, on='ifa').cache()
    top_app5 = persona_app5.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app5 = top_app5.filter(top_app5['asn'] != 'null')
    top_app5 = top_app5.withColumnRenamed('asn', list5)
    top_app5.show(10, False)
    print('xxxxxxxxxxxxxx ' + m + ' Done xxxxxxxxxxxxxxx')

for m in Month:
    print('+============= ' + m + ' ===============')
    df = spark.read.csv('s3a://ada-bd-emr/result/20210608/' + m + '/*.csv', header=True)
    df.count()
    print('xxxxxxxxxxxxxx ' + m + ' Done xxxxxxxxxxxxxxx')
