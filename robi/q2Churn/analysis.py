# case1: have not used robi from last 2 years
# case2: have not used robi from last december 2021
#Sumani's Churn Model for dhaka district

# pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
# pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 40G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045

from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys


country = 'BD'
year = '202106'

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

#case 1
path = 'etl/data/brq/agg/agg_brq/monthly/BD/202{001,002,003,004,005,006,007,008,009,010,011,012,101,102,103,104}/*.parquet'


df=spark.read.parquet(path)
location = df.select('ifa',F.explode('gps.city').alias('district'))
location1=location.filter(location['district']=='Dhaka')


telco = df.select('ifa', 'connection')
connection = telco.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
connection1 = telco.select('ifa', F.explode('connection.mm_con_type_desc').alias('con_type'))
connection2= telco.select('ifa', F.explode('connection.req_con_type').alias('network'))
con_data = connection.join(connection1, on='ifa', how='left')
con_data1 = con_data.join(connection2,on='ifa',how='left')
all_opr = location1.join(con_data1,on='ifa',how='left')
none_robi = all_opr.filter((all_opr['con_type']=='Cellular') & (all_opr['telco']!='Robi/Aktel'))
none_robi1=none_robi.filter(none_robi['telco']!='NaN')
churn_df = spark.read.csv('/RobiChurn/2021Q2/*.csv',header=True)

g_stat=none_robi1.groupBy('telco').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
g_stat.show()
c_stat=churn_df.groupBy('carrier').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
churn_opr = churn_df.filter((churn_df['carrier']=='grameen') | (churn_df['carrier']=='telenor') | (churn_df['carrier']=='robi') | (churn_df['carrier']=='banglalink') | (churn_df['carrier']=='teletalk'))
churn_opr_ex_robi = churn_df.filter((churn_df['carrier']=='grameen') | (churn_df['carrier']=='banglalink') | (churn_df['carrier']=='teletalk'))

g = churn_df.filter(churn_df['carrier']=='grameen')
t= churn_df.filter(churn_df['carrier']=='telenor')
x1=g.join(t,on='ifa',how='left')
x1.distinct().count()
x2=t.join(g,on='ifa',how='left')
x2.distinct().count()

never_robi = churn_opr_ex_robi.join(none_robi1,on='ifa',how='left').cache()
n_robi=never_robi.distinct()
n_robi1=n_robi.select('ifa','carrier')
n_robi2=n_robi1.groupBy('carrier').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
n_robi2.show()
never_been_robi=n_robi.select('ifa','carrier')
never_been_robi.count()

never_been_robi_d=never_been_robi.distinct()

path = '/RobiChurn/2021Q2/output/25/'

never_been_robi_d.coalesce(3).write.csv(path + "never_been_a_robi", mode='overwrite', header=True)
# case 2
path1 = 'etl/data/brq/agg/agg_brq/monthly/BD/202{101,102,103,104}/*.parquet'
df1=spark.read.parquet(path1)

locationx = df1.select('ifa',F.explode('gps.city').alias('district'))
locationx1=locationx.filter(locationx['district']=='Dhaka')
telcox = df1.select('ifa', 'connection')
connectionx = telcox.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
connectionx1 = telcox.select('ifa', F.explode('connection.mm_con_type_desc').alias('con_type'))
connectionx2= telco.select('ifa', F.explode('connection.req_con_type').alias('network'))

con_data = connectionx.join(connectionx1, on='ifa', how='left')
con_data1 = con_data.join(connectionx2,on='ifa',how='left')
all_oprx = location1.join(con_data1,on='ifa',how='left')

all_oprx = connectionx.join(connectionx1, on='ifa', how='left')
none_robix = all_oprx.filter((all_oprx['con_type']=='Cellular') & (all_oprx['telco']!='Robi/Aktel'))

x_none_r =none_robix.filter(none_robix['telco']!='NaN')

churn_robi_opr = churn_df.filter(churn_df['carrier']=='robi').cache()

robi_before_2021 = churn_robi_opr.join(x_none_r,on='ifa',how='left')

robi20=robi_before_2021.distinct()


# age segment


df_age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/'+country+'/'+year+'/age/*.parquet')
df_age = df_age.drop('prediction')
df_age = df_age.withColumnRenamed('label', 'age')
df_age_segment1 = none_r1.join(df_age, ['ifa'], how='left')
df_age_segment1.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

df_age_segment2 = robi20.join(df_age, ['ifa'], how='left')
df_age_segment2.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200,0)

# sex distribution

df_gender = spark.read.parquet('etl/table/brq/sub/demographics/monthly/' + country + '/' + year + '/gender/*.parquet')
df_gender = df_gender.drop('prediction')
df_gender = df_gender.withColumnRenamed('label', 'gender')
df_gender_segment1 = none_r1.join(df_gender, ['ifa'], how='left')
df_gender_segment1.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200, 0)

df_gender_segment2 = robi20.join(df_gender, ['ifa'], how='left')
df_gender_segment2.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200, 0)


#life stage


master_df = spark.read.csv('reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)


brq = spark.read.parquet('etl/data/brq/agg/agg_brq/monthly/'+country+'/'+year+'/*.parquet')
brq2 = brq.select('ifa',explode('app')).select('ifa','col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()

#noner1 is for case1
persona_app = none_r1.join(app, on ='ifa')
freq_beh = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
freq_beh1 = freq_beh.filter(freq_beh['lifestage_name'] != 'null')
freq_beh1.show(20,0)

Rpersona_app = robi20.join(app, on ='ifa')
Rfreq_beh = Rpersona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
Rfreq_beh1 = Rfreq_beh.filter(freq_beh['lifestage_name'] != 'null')
Rfreq_beh1.show(20,0)


#data usrage
# none robi
persona_app.agg({'brq_count':'max'}).show()
persona_app.agg({'brq_count':'avg'}).show()
persona_app.agg({'brq_count':'min'}).show()


persona_app.filter((persona_app['brq_count']>1000) & (persona_app['brq_count']<192828)).count()
persona_app.filter((persona_app['brq_count']>500) & (persona_app['brq_count']<1000)).count()
persona_app.filter((persona_app['brq_count']>1) & (persona_app['brq_count']<500)).count()

#robi churn

Rpersona_app.agg({'brq_count':'max'}).show()
Rpersona_app.agg({'brq_count':'avg'}).show()
Rpersona_app.agg({'brq_count':'min'}).show()


Rpersona_app.filter((Rpersona_app['brq_count']>1000) & (Rpersona_app['brq_count']<192828)).count()
Rpersona_app.filter((Rpersona_app['brq_count']>500) & (Rpersona_app['brq_count']<1000)).count()
Rpersona_app.filter((Rpersona_app['brq_count']>1) & (Rpersona_app['brq_count']<500)).count()

net = telco.select('ifa', F.explode('connection.req_con_type').alias('network'))
network1=none_r.join(net,on='ifa',how='left')
network1.groupBy('network').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False).show()
net1 = telcox.select('ifa', F.explode('connection.req_con_type').alias('network'))
network2=robi20.join(net1,on='ifa',how='left')
network2.groupBy('network').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False).show()

net1 = telcox.select('ifa', F.explode('connection.req_con_type').alias('network'))


never_been_robi_d.coalesce(3).write.csv(path + "none_robi", mode='overwrite', header=True)
robi20.coalesce(2).write.csv(path + "churn_robi", mode='overwrite', header=True)
