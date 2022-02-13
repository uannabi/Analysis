# pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
# pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 40G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045


import sys

from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

path = '/etl/data/brq/agg/agg_brq/monthly/BD/20210{5,4,3}/*.parquet'
df=spark.read.parquet(path)
location = df.select('ifa',F.explode('gps.city').alias('district'))
site1=location.filter(location['district']=='Dhaka')
site2=location.filter(location['district']=='Chittagong')
site3=location

path='s3://ada-bd-emr/RobiChurn/20210606/output'

site1.write.parquet(path + "site1", mode='overwrite')
site2.write.parquet(path + "site2", mode='overwrite')
site3.write.parquet(path + "site3", mode='overwrite')

telco = df.select('ifa', 'connection')
connection = telco.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
robi_user = connection.filter(connection['telco']!='Robi/Aktel')

master_df = spark.read.csv('/reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('/reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)

brq = df.select('ifa', F.explode('app')).select('ifa', 'col.*') #top first app extracting
app = brq.join(finalapp_df, on='bundle', how='left').cache()

list = ['site1', 'site2','site3']

for l in list:
    data = spark.read.parquet('RobiChurn/20210606/'+l+'/*.parquet')
    persona=data.join(robi_user,on='ifa',how='left')
    persona_app = persona.join(app, on='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq',ascending=False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20,False)
    list_df = freq_beh1.select('app_l1_name').collect()
    list1 = list_df[1].app_l1_name
    list2 = list_df[2].app_l1_name
    list3 = list_df[3].app_l1_name
    list4 = list_df[4].app_l1_name
    list5 = list_df[5].app_l1_name
    print(l + ' of top app')
    topApp_df1 = app.filter((app.app_l2_name.isin(list1)) | (app.app_l1_name.isin(list1)))
    persona_app1 = persona.join(topApp_df1, on='ifa').cache()
    top_app1 = persona_app1.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app1 = top_app1.filter(top_app1['asn'] != 'null')
    top_app1 = top_app1.withColumnRenamed('asn', list1)
    top_app1.show(20,False)
    topApp_df2 = app.filter((app.app_l2_name.isin(list2)) | (app.app_l1_name.isin(list2)))
    persona_app2 = persona.join(topApp_df2, on='ifa').cache()
    top_app2 = persona_app2.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app2 = top_app2.filter(top_app2['asn'] != 'null')
    top_app2 = top_app2.withColumnRenamed('asn', list2)
    top_app2.show(20,False)
    topApp_df3 = app.filter((app.app_l2_name.isin(list3)) | (app.app_l1_name.isin(list3)))
    persona_app3 = persona.join(topApp_df3, on='ifa').cache()
    top_app3 = persona_app3.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app3 = top_app3.filter(top_app3['asn'] != 'null')
    top_app3 = top_app3.withColumnRenamed('asn', list3)
    top_app3.show(20,False)
    topApp_df4 = app.filter((app.app_l2_name.isin(list4)) | (app.app_l1_name.isin(list4)))
    persona_app4 = persona.join(topApp_df4, on='ifa').cache()
    top_app4 = persona_app4.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app4 = top_app4.filter(top_app4['asn'] != 'null')
    top_app4 = top_app4.withColumnRenamed('asn', list4)
    top_app4.show(20,False)
    topApp_df5 = app.filter((app.app_l2_name.isin(list5)) | (app.app_l1_name.isin(list5)))
    persona_app5 = persona.join(topApp_df5, on='ifa').cache()
    top_app5 = persona_app5.groupBy('asn').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app5 = top_app5.filter(top_app5['asn'] != 'null')
    top_app5 = top_app5.withColumnRenamed('asn', list5)
    top_app5.show(20,False)
    print(l +' is done')


for l in list:
    data = spark.read.parquet('RobiChurn/20210606/'+l+'/*.parquet')
    persona=data.join(robi_user,on='ifa',how='left')
    persona_app = persona.join(app, on='ifa')
    IFA=persona_app.select('ifa').distinct().count()
    print(l+ 'count:{} '.format(IFA))



