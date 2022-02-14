import pyspark.sql.functions as F
list=['202102',
      '202103',
      '202104']
country ='BD'

for l in list:
    df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/'+l+'/*.parquet')
    telco = df.select('ifa', 'connection')
    connection = telco.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
    connection1 = telco.select('ifa', F.explode('connection.mm_con_type_desc').alias('con_type'))
    connection2 = telco.select('ifa',F.explode('connection.req_con_type').alias('network'))
    all_opr = connection.join(connection1, on='ifa', how='left')
    all_opr=all_opr.join(connection2,on='ifa',how='left')
    total_count=all_opr.select('ifa')
    total_count.distinct().count()
    clean=all_opr.filter((all_opr['network']==1) |(all_opr['network']==1)|(all_opr['network']==2 )|(all_opr['network']==4 )| (all_opr['network']==5 )| (all_opr['network']==6 ))
    clean.agg(F.countDistinct('ifa')).show()
    cellular=all_opr.filter(all_opr['con_type']=='Cellular')
    cellular.agg(F.countDistinct('ifa')).show()
    wifi=all_opr.filter(all_opr['network']==2)
    wifi.agg(F.countDistinct('ifa')).show()
    both = all_opr.filter((all_opr['con_type']=='Cellular') & (all_opr['network']==2))
    both.agg(F.countDistinct('ifa')).show()




