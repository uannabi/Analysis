#thana wise IFA count

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys

case1 = spark.read.csv('RobiChurn/q3/202108/case1_nonrobi.csv', header=True)

case2 = spark.read.csv('RobiChurn/q3/202108/case2_churnrobi.csv', header=True)

location = spark.read.parquet('etl/data/brq/sub/home-office/home-office-data/BD/2021{06,05,04,03,02,07}/home-location/*.parquet')


location=location.filter(location['home_district']=='Dhaka')

location = location.select('ifa', 'home_district', 'home_thana').cache()
case1_thana = case1.join(location, on='ifa', how='left')
case1_thana.groupBy('home_thana').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False).show(50, False)

case1Print = case1_thana.groupBy('home_thana').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)

case2_thana = case2.join(location, on='ifa', how='left')
case2_thana.groupBy('home_thana').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False).show(50, False)
case2Print = case2_thana.groupBy('home_thana').agg(F.countDistinct('ifa').alias('ifa')).sort('ifa', ascending=False)

output = 'RobiChurn/q3/202108/'

case1Print.coalesce(1).write.csv(output + "case1", mode='overwrite', header=True)
case2Print.coalesce(1).write.csv(output + "case2", mode='overwrite', header=True)

