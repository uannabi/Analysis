# Author : Zahid Un Nabi
# Request : DSR 120
# Description : Reach Estimate & IFA Extraction
# 1.age
# 2.Personal life stage
# 3.Professional life stage
# 4.Data Usage
# 5.Hanset (Top 10 brands with top 5 models each)
# 6.Handset Usage  ( for how many years they have been using, count not needed, only percentage can work too )

pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 30G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045


from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
from haversine import haversine
from pyspark.sql.window import Window


date = ['202012','202101','202102']
country = 'BD'



brq = spark.read.parquet('/monthly/BD/{202012,202101,202102}/*.parquet')

master_df = spark.read.csv('app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)

df=brq.select('ifa')

brq2 = brq.select('ifa',explode('app')).select('ifa','col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()

persona_app = df.join(app, on ='ifa')
freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
freq_beh1.show(20,0)


games = persona_app.filter(persona_app['app_l1_name']=='Games')
games1=games.groupBy('app_l2_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
games_df= games.select('ifa')
g_list = ['Sports Games','Simulation Games','Action Games','Music Games','Puzzle Games','Racing Games','Role Playing Games','Art and Colouring Games','Educational Games','Board Games','Adventure Games','Educational Games',]
path='/result/20210310/'
for g in g_list :
    df_game=persona_app.filter(persona_app['app_l2_name']==g)
    df_game1=df_game.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
    df_game1.coalesce(1).write.csv(path + g, mode='overwrite', header=True)
    print('Date and time for {} done'.format(g))


#Demographny
df_gender = spark.read.parquet('demographics/monthly/BD/202101/gender/*.parquet')
df_gender = df_gender.drop('prediction')
df_gender = df_gender.withColumnRenamed('label', 'gender')
df_gender_segment = games_df.join(df_gender, ['ifa'], how='left')


# Affluence level

df_age = spark.read.parquet('demographics/monthly/BD/202101/age/*.parquet')
df_age = df_age.drop('prediction')
df_age = df_age.withColumnRenamed('label', 'age')
df_age_segment = games_df.join(df_age, ['ifa'], how='left')
df_age_segment.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

#lifeStage
raw_data = games_df.join(app, on='ifa').cache()
lifeStage = raw_data.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
lifeStage.show(10,0)

#device Information
device_df=brq.select('ifa', 'device.device_vendor', 'device.device_model','device.device_manufacturer', 'device.major_os','device.device_year_of_release')

games_device = games_df.join(device_df,on='ifa',how='left')
games_device.groupBy('device_manufacturer').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False).show(10,False)
games_device.groupBy('device_model').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False).show(10,False)
games_device.groupBy('device_year_of_release').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False).show(10,False)



