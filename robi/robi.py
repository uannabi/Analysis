
# pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
# pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 20 --executor-memory 40G --executor-cores 5 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045

from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
import sys
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import geohash2 as geohash
import pygeohash as pgh
from functools import reduce
from pyspark.sql import *


country='BD'
#march
day =[ '20210301',
'20210302',
'20210303',
'20210304',
'20210305',
'20210306',
'20210307',
'20210308',
'20210309',
'20210310',
'20210311',
'20210312'
       ]
#februarey
day =[ '20210201',
'20210202',
'20210203',
'20210204',
'20210205',
'20210206',
'20210207',
'20210208',
'20210209',
'20210210',
'20210211',
'20210212',
'20210213',
'20210214',
'20210215',
'20210216',
'20210217',
'20210218',
'20210219',
'20210220',
'20210221',
'20210222',
'20210223',
'20210224',
'20210225',
'20210226',
'20210227',
'20210228',
'20210229',
'20210230',
'20210231'
       ]
#january
day =[
'20210101',
'20210102',
'20210103',
'20210104',
'20210105',
'20210106',
'20210107',
'20210108',
'20210109',
'20210110',
'20210111',
'20210112',
'20210113',
'20210114',
'20210115',
'20210116',
'20210117',
'20210118',
'20210119',
'20210120',
'20210121',
'20210122',
'20210123',
'20210124',
'20210125',
'20210126',
'20210127',
'20210128',
'20210129',
'20210130',
'20210131'
]


time='daily'

for d in day:
    df=spark.read.parquet('etl/data/brq/agg/agg_brq/'+time+'/'+country+'/'+d+'/*.parquet')
    connection = df.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
    robi_user = connection.filter(connection['telco'] == 'Robi/Aktel').distinct()
    print('Robi User {}'.format(robi_user.count()))
    gp_user = connection.filter(connection['telco'] == 'GrameenPhone').distinct()
    print('GrameenPhone User {}'.format(gp_user.count()))
    bl_user = connection.filter(connection['telco'] == 'Orascom/Banglalink').distinct()
    print('Banglalink User {}'.format(bl_user.count()))
    print('Date and time for {} done'.format(d))


#robi analysis done


