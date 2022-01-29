## Nagad: Whole Bangladdeesh Age group wise
 # App interset and App list
 # Top haandset and Affluence wise model

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
spark = SparkSession.builder.appName("Games").getOrCreate()
s
country = 'BD'
year = '202011'

data_list = ['109', '110', '111']
master_df = spark.read.csv('/reference/app/master_all/all/all/all/app.csv', header=True)
level_df = spark.read.csv('/reference/app/app_level/all/all/all/app_level.csv', header=True)
lifestage_df = spark.read.csv('/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)

df_age = spark.read.parquet(
    '/etl/table/brq/sub/demographics/monthly/BD/2021{10,11,09,08,07,06,05}/age/*.parquet')
df_age = df_age.withColumnRenamed('prediction', 'age')
df_age.groupBy('age').agg(F.countDistinct('ifa')).orderBy('age').show(200, 0)

age1824 = df_age.filter(df_age['age'] == '18-24').select('ifa')
age2534 = df_age.filter(df_age['age'] == '25-34').select('ifa')
age3544 = df_age.filter(df_age['age'] == '35-44').select('ifa')
age50 = df_age.filter(df_age['age'] == '50+').select('ifa')
url = '/result/2021/202112/DSR-574/age-group'

age1824.write.parquet(url + "/age1824")
age2534.write.parquet(url + "/age2534")
age3544.write.parquet(url + "/age3544")
age50.write.parquet(url + "/age50")

brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/' + country + '/' + year + '/*.parquet')
brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()
age_group = ['age1824', 'age2534', 'age3549', 'age50']

## age Top App Category ##
for i in age_group:
    print(i)
    geo = spark.read.parquet('/result/2021/202112/DSR-574/age-group/' + i + '/*.parquet')
    persona_app = geo.join(app, on='ifa')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq',
                                                                                                 ascending=False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(20, 0)

## TELCO DISTRIBUTIONS ##

for i in age_group:
    print('+=============' + i + '===============')
    df = spark.read.parquet(
        '/result/2021/202112/DSR-574/age-group/' + i + '/*.parquet')  # gameing segment
    brq = spark.read.parquet(
        '/etl/data/brq/agg/agg_brq/monthly/BD/202111/*.parquet')  # full brq data
    connection_d = brq.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
    connection = df.join(connection_d, on='ifa', how='left')
    print('++++++++++ Connection Distribution ++++++++++')
    robi_user = connection.filter(connection['telco'] == 'Robi/Aktel').distinct()
    print('Robi User {}'.format(robi_user.count()))
    gp_user = connection.filter(connection['telco'] == 'GrameenPhone').distinct()
    print('GrameenPhone User {}'.format(gp_user.count()))
    bl_user = connection.filter(connection['telco'] == 'Orascom/Banglalink').distinct()
    print('Banglalink User {}'.format(bl_user.count()))

### top app with Avg BRQ count ###
for m in age_group:
    print('+============= ' + m + ' ===============')
    df = spark.read.parquet('/result/2021/202112/DSR-574/age-group/' + m + '/*.parquet')
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/BD/202111/*.parquet')
    df1 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
    app = df1.join(finalapp_df, on='bundle', how='left').cache()
    persona_app = df.join(app, on='ifa', how='left')
    freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq',ascending=False)
    freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
    freq_beh1.show(3, 0)
    list_df = freq_beh1.select('app_l1_name').collect()
    list1 = list_df[1].app_l1_name
    list2 = list_df[2].app_l1_name
    list3 = list_df[3].app_l1_name
    list4 = list_df[4].app_l1_name
    list5 = list_df[5].app_l1_name
    print(m + ' of top app')
    topApp_df1 = app.filter((app.app_l2_name.isin(list1)) | (app.app_l1_name.isin(list1)))
    persona_app1 = df.join(topApp_df1, on='ifa').cache()
    top_app1 = persona_app1.groupBy('asn').agg((F.avg('brq_count').alias('spentHrs')),
                                               F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app1 = top_app1.filter(top_app1['asn'] != 'null')
    top_app1 = top_app1.withColumnRenamed('asn', list1)
    top_app1.show(20, False)
    topApp_df2 = app.filter((app.app_l2_name.isin(list2)) | (app.app_l1_name.isin(list2)))
    persona_app2 = df.join(topApp_df2, on='ifa').cache()
    top_app2 = persona_app2.groupBy('asn').agg((F.avg('brq_count').alias('spentHrs')),
                                               F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app2 = top_app2.filter(top_app2['asn'] != 'null')
    top_app2 = top_app2.withColumnRenamed('asn', list2)
    top_app2.show(20, False)
    topApp_df3 = app.filter((app.app_l2_name.isin(list3)) | (app.app_l1_name.isin(list3)))
    persona_app3 = df.join(topApp_df3, on='ifa').cache()
    top_app3 = persona_app3.groupBy('asn').agg((F.avg('brq_count').alias('spentHrs')),
                                               F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app3 = top_app3.filter(top_app3['asn'] != 'null')
    top_app3 = top_app3.withColumnRenamed('asn', list3)
    top_app3.show(20, False)
    topApp_df4 = app.filter((app.app_l2_name.isin(list4)) | (app.app_l1_name.isin(list4)))
    persona_app4 = df.join(topApp_df4, on='ifa').cache()
    top_app4 = persona_app4.groupBy('asn').agg((F.avg('brq_count').alias('spentHrs')),
                                               F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app4 = top_app4.filter(top_app4['asn'] != 'null')
    top_app4 = top_app4.withColumnRenamed('asn', list4)
    top_app4.show(20, False)
    topApp_df5 = app.filter((app.app_l2_name.isin(list5)) | (app.app_l1_name.isin(list5)))
    persona_app5 = df.join(topApp_df5, on='ifa').cache()
    top_app5 = persona_app5.groupBy('asn').agg((F.avg('brq_count').alias('spentHrs')),
                                               F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
    top_app5 = top_app5.filter(top_app5['asn'] != 'null')
    top_app5 = top_app5.withColumnRenamed('asn', list5)
    top_app5.show(20, False)
    print('xxxxxxxxxxxxxx ' + m + ' Done xxxxxxxxxxxxxxx')
### Top Device ###
for m in age_group:
    print('+============= ' + m + ' ===============')
    df = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age1824/*.parquet')
    brq = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/BD/202111/*.parquet')
    deviceInfo = brq.select('ifa', 'device.device_name', 'device.device_model')
    tgdeviceInfo = df.join(deviceInfo, on='ifa', how='left')
    topDevice = tgdeviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                                  ascending=False)
    topDevice.show(10, False)

for m in age_group:
    df = spark.read.parquet('/result/2021/202112/DSR-574/age-group/' + i + '/*.parquet')
    df.count()

age1824 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age1824/*.parquet')
age1824.count()
age2534 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age2534/*.parquet')
age2534.count()
age3549 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age3549/*.parquet')
age3549.count()
age50 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age50/*.parquet')
age50.count()

### top handset for age 18-24 ###
age1824 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age1824/*.parquet')
tgdeviceInfo = age1824.join(deviceInfo, on='ifa', how='left')
topDevice = tgdeviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                              ascending=False)
topDevice.show(20, False)
### top handset for age 25-34 ###
age2534 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age2534/*.parquet')
tgdeviceInfo = age2534.join(deviceInfo, on='ifa', how='left')
topDevice = tgdeviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                              ascending=False)
topDevice.show(20, False)
### top handset for age 35-49 ###
age3549 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age3549/*.parquet')
tgdeviceInfo = age3549.join(deviceInfo, on='ifa', how='left')
topDevice = tgdeviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                              ascending=False)
topDevice.show(20, False)
### top handset for age 50+ ###
age50 = spark.read.parquet('/result/2021/202112/DSR-574/age-group/age50/*.parquet')
tgdeviceInfo = age50.join(deviceInfo, on='ifa', how='left')
topDevice = tgdeviceInfo.groupBy('device_name', 'device_model').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa',
                                                                                                              ascending=False)
topDevice.show(20, False)

# Affluence data
affluence = spark.read.parquet('/etl/data/brq/sub/affluence/monthly/BD/202111/*.parquet')
affluence = affluence.select('ifa', 'device_name', 'device_model', 'final_affluence_id', 'price')

# classify Affluence basedd on Handset model

Uh = affluence.filter(affluence['price'] >= 750)
h = affluence.filter((affluence['price'] >= 500) & (affluence['price'] <= 750))
m = affluence.filter((affluence['price'] >= 350) & (affluence['price'] <= 500))
l = affluence.filter((affluence['price'] <= 350))

### for age group 1 ###
age1UH = age1824.join(Uh, on='ifa', how='left')
age1UHG = age1UH.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age1UHG.show(10, False)

age1H = age1824.join(h, on='ifa', how='left')
age1HG = age1H.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age1HG.show(6, False)

age1M = age1824.join(m, on='ifa', how='left')
age1MG = age1M.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age1MG.show(6, False)

age1L = age1824.join(l, on='ifa', how='left')
age1LG = age1L.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age1LG.show(6, False)
### for age group 2 ###
age2UH = age2534.join(Uh, on='ifa', how='left')
age2UHG = age2UH.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age2UHG.show(10, False)

age2H = age2534.join(h, on='ifa', how='left')
age2HG = age2H.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age2HG.show(6, False)

age2M = age2534.join(m, on='ifa', how='left')
age2MG = age2M.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age2MG.show(6, False)

age2L = age2534.join(l, on='ifa', how='left')
age2LG = age2L.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age2LG.show(6, False)
### for age group 3 ###
age3UH = age3549.join(Uh, on='ifa', how='left')
age3UHG = age3UH.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age3UHG.show(10, False)

age3H = age3549.join(h, on='ifa', how='left')
age3HG = age3H.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age3HG.show(6, False)

age3M = age3549.join(m, on='ifa', how='left')
age3MG = age3M.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age3MG.show(6, False)

age3L = age3549.join(l, on='ifa', how='left')
age3LG = age3L.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age3LG.show(6, False)

## for age group 4 ###
age4UH = age50.join(Uh, on='ifa', how='left')
age4UHG = age4UH.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age4UHG.show(10, False)

age4H = age50.join(h, on='ifa', how='left')
age4HG = age4H.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age4HG.show(6, False)

age4M = age50.join(m, on='ifa', how='left')
age4MG = age4M.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age4MG.show(6, False)

age4L = age50.join(l, on='ifa', how='left')
age4LG = age4L.groupBy('device_name').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age4LG.show(6, False)


 ## age group wise total distribution
aff = spark.read.parquet('/etl/data/brq/sub/affluence/monthly/BD/202111/*.parquet')
affluenceFinal = aff.select('ifa', 'final_affluence')
age1 = age1824.join(affluenceFinal, on='ifa', how='left')
age2 = age1.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)

age2 = age2534.join(affluenceFinal, on='ifa', how='left')
age22 = age2.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age22.show()

age3 = age3549.join(affluenceFinal, on='ifa', how='left')
age32 = age3.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age32.show()

age4 = age50.join(affluenceFinal, on='ifa', how='left')
age42 = age4.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)
age42.show()

