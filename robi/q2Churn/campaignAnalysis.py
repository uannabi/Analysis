import pyspark.sql.functions as F

p1 = 'RobiChurn/robiChurn.csv'
p2= 'RobiChurn/ctg_g_v.csv'
p3 = 'RobiChurn/dhk_g_v.csv'
feb = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/BD/202102/*')
jan  = spark.read.parquet('/etl/data/brq/agg/agg_brq/monthly/BD/202101/*')


df1=jan.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
df2=feb.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
df1=df1.filter(df1['telco']=='Robi/Aktel')
df2=df2.filter(df2['telco']=='Robi/Aktel')


mn = spark.read.csv(p1,header=True)
ctg = spark.read.csv(p2,header=True)
dhk = spark.read.csv(p3,header=True)
master_df = spark.read.csv('app_ref/master_df/*', header=True)
level_df = spark.read.csv('app_ref/level_df/*', header=True)
lifestage_df = spark.read.csv('app_ref/lifestage_df/*', header=True)

join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)
jan2 = jan.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = jan2.join(finalapp_df, on='bundle', how='left').cache()



df=mn.join(df3,on='ifa',how='left')
df.groupBy('carrier').count().show()
df.groupBy('telco').count().show()
df_robi_jan_mn = df.select('ifa')
persona_app = df_robi_jan_mn.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
freq_ls.show()

dfctg_jan=ctg.join(df3,on='ifa',how='left')
dfctg_jan.groupBy('carrier').count().show()
dfctg_jan.groupBy('telco').count().show()
df_robi_jan_ctg = dfctg_jan.select('ifa')
persona_app = df_robi_jan_ctg.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=False)
freq_ls.show()


dfdhk_jan=dhk.join(df3,on='ifa',how='left')
dfdhk_jan.groupBy('carrier').count().show()
dfdhk_jan.groupBy('telco').count().show()
df_robi_jan_dhk = dfdhk_jan.select('ifa')
persona_app = df_robi_jan_dhk.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=False)
freq_ls.show()

dfmn_feb=mn.join(df4,on='ifa',how='left')
dfmn_feb.groupBy('carrier').count().show()
dfmn_feb.groupBy('telco').count().show()
df_robi_feb_mn = dfmn_feb.select('ifa')
persona_app = df_robi_feb_mn.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
freq_ls.show()

dfctg_feb=ctg.join(df4,on='ifa',how='left')
dfctg_feb.groupBy('carrier').count().show()
dfctg_feb.groupBy('telco').count().show()
df_robi_feb_ctg = dfctg_feb.select('ifa')
persona_app = df_robi_feb_ctg.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=False)
freq_ls.show()


dfdhk_feb=dhk.join(df3,on='ifa',how='left')
dfdhk_feb.groupBy('carrier').count().show()
dfdhk_feb.groupBy('telco').count().show()
df_robi_feb_dhk = dfdhk_feb.select('ifa')
persona_app = df_robi_feb_dhk.join(app, on='ifa').cache()
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=False)
freq_ls.show()

df_m = spark.read.csv(p1,header=True)
df_c= spark.read.csv(p2,header=True)
df_d= spark.read.csv(p3,header=True)


jan_myman = df.select('ifa','telco')
jan_myman1 = jan_myman.filter(jan_myman['telco']=='Robi/Aktel')
inclusive_m = mymensing_feb1.join(mymensing_jan1,on='ifa',how='left_anti')

jan_ctg = dfctg_jan.select('ifa','telco')
jan_ctg1 = jan_ctg.filter(jan_ctg['telco']=='Robi/Aktel')

jan_dhk = dfdhk_jan.select('ifa','telco')
jan_dhk = jan_dhk.filter(jan_dhk['telco']=='Robi/Aktel')



dfmyman=dfmn_feb.withColumnRenamed('telco','telco_feb')
dfctg=dfctg_feb.withColumnRenamed('telco','telco_feb')
dfdhk_feb=dfdhk_feb.withColumnRenamed('telco','telco_feb')


feb_myman = dfmyman.select('ifa','telco_feb')
feb_myman1 = feb_myman.filter(feb_myman['telco_feb']=='Robi/Aktel')

feb_ctg = dfctg.select('ifa','telco_feb')
feb_ctg1 = feb_ctg.filter(feb_ctg['telco_feb']=='Robi/Aktel')

feb_dhk = dfdhk_feb.select('ifa','telco_feb')
feb_dhk1 = feb_dhk.filter(feb_dhk['telco_feb']=='Robi/Aktel')



mymensing_jan = df_m.join(df1,on='ifa',how='left')
mymensing_jan.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |   carrier|count|
# +----------+-----+
# |   Grameen|69009|
# |Banglalink|43082|
# +----------+-----+

mymensing_jan.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |     telco|count|
# +----------+-----+
# |      null|82052|
# |Robi/Aktel|14507|
# +----------+-----+

mymensing_feb = df_m.join(df2,on='ifa',how='left')
mymensing_feb.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |     telco|count|
# +----------+-----+
# |      null|83190|
# |Robi/Aktel|13369|
# +----------+-----+

df_m.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |   carrier|count|
# +----------+-----+
# |   Grameen|69009|
# |Banglalink|43082|
# +----------+-----+

df_d.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +-------+------+
# |carrier| count|
# +-------+------+
# |Grameen|418432|
# +-------+------+

df_c.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +-------+-----+
# |carrier|count|
# +-------+-----+
# |Grameen|29034|
# +-------+-----+


mymensing_jan1=mymensing_jan.select('ifa','telco')
mymensing_jan1.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- telco: string (nullable = true)



mymensing_jan1=mymensing_jan1.filter(mymensing_jan1['telco']!='null')
mymensing_jan1.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |     telco|count|
# +----------+-----+
# |Robi/Aktel|14507|
# +----------+-----+

mymensing_feb1=mymensing_feb.select('ifa','telco')
mymensing_feb1=mymensing_feb1.filter(mymensing_feb1['telco']!='null')
mymensing_feb1.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +----------+-----+
# |     telco|count|
# +----------+-----+
# |Robi/Aktel|13369|
# +----------+-----+
dhaka_jan = df_d.join(df1,on='ifa',how='left')

dhaka_feb=df_d.join(df2,on='ifa',how='left')
dhaka_feb.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- district: string (nullable = true)
#  |-- carrier: string (nullable = true)
#  |-- Game_lovers: string (nullable = true)
#  |-- video_lovers: string (nullable = true)
#  |-- telco: string (nullable = true)


dhaka_jan1=dhaka_jan.filter(dhaka_jan['telco']=='Robi/Aktel').select('ifa','telco')
dhaka_feb1=dhaka_feb.filter(dhaka_feb['telco']=='Robi/Aktel').select('ifa','telco')
dhaka_feb1.count()
# 64316
dhaka_feb1.distinct().count()
# 54279
dhaka_feb1=dhaka_feb1.distinct()
dhaka_jan1.distinct().count()
# 58440
dhaka_feb1.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- telco: string (nullable = true)


inclusive_d=dhaka_feb1.join(dhaka_jan1,on='ifa',how='left_anti')

inclusive_d.count()
# 15593
ctg_feb=df_c.join(df2,on='ifa',how='left')
ctg_jan=df_c.join(df1,on='ifa',how='left')
ctg_feb1=ctg_feb.filter(ctg_feb['telco']=='Robi/Aktel').select('ifa','telco').distinct()
ctg_jan1=ctg_jan.filter(ctg_jan['telco']=='Robi/Aktel').select('ifa','telco').distinct()
inclusive_ctg=ctg_feb1.join(ctg_jan1,on='ifa',how='left_anti')
inclusive_ctg.count()
# 1557
ctg_jan.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- district: string (nullable = true)
#  |-- carrier: string (nullable = true)
#  |-- Game_lovers: string (nullable = true)
#  |-- video_lovers: string (nullable = true)
#  |-- telco: string (nullable = true)

ctg_jan.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +-------+-----+
# |carrier|count|
# +-------+-----+
# |Grameen|29034|
# +-------+-----+

ctg_feb.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- district: string (nullable = true)
#  |-- carrier: string (nullable = true)
#  |-- Game_lovers: string (nullable = true)
#  |-- video_lovers: string (nullable = true)
#  |-- telco: string (nullable = true)

ctg_feb.groupBy('carrier').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False).show()
# +-------+-----+
# |carrier|count|
# +-------+-----+
# |Grameen|29034|
# +-------+-----+


m=inclusive_m.join(app,on='ifa')
d=inclusive_d.join(app,on='ifa')
c=inclusive_c.join(app,on='ifa')

m_l.coalesce(1).write.csv(path + "ml", mode='overwrite', header=True)
m_a.coalesce(1).write.csv(path + "ma", mode='overwrite', header=True)
d_l.coalesce(1).write.csv(path + "dl", mode='overwrite', header=True)
d_a.coalesce(1).write.csv(path + "da", mode='overwrite', header=True)
c_l.coalesce(1).write.csv(path + "cl", mode='overwrite', header=True)
c_a.coalesce(1).write.csv(path + "ca", mode='overwrite', header=True)

