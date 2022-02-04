from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys

churn = spark.read.csv('RobiChurn/q3/telco_churn_model.csv',header=True)
car_2 = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/2021{03,04,05,06,07}/*')
data=churn.filter((churn['carrier']=='robi') | (churn['carrier']=='grameen')|(churn['carrier']=='banglalink'))
robi=churn.filter(churn['carrier']=='robi')

car_2 = car_2.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
non = car_2.select('*').where(~col("req_carrier_name").like("%Robi%")).distinct()

non=non.select('ifa','req_carrier_name')
non.groupBy('req_carrier_name').count().show()
# +------------------+---------+
# |  req_carrier_name|    count|
# +------------------+---------+
# |Orascom/Banglalink| 59262650|
# |      GrameenPhone|142666518|
# |          TeleTalk|  6345987|
# +------------------+---------+

delta_case2=non.join(data,on='ifa',how='left_anti')
delta_case2.printSchema()
location=spark.read.parquet('etl/data/brq/sub/home-office/home-office-data/BD/2021{06,05,04,03,02,07}/home-location/*.parquet')

dhaka=location.select('ifa').where(col('home_district').like('%Dhaka%')).distinct()
dhaka.printSchema()


non.count()
# 208275155
data.count()
# 18076738
delta_case2.count()
# 102518042
deltacase2=delta_case2.join(dhaka,on='ifa',how='left_anti')
deltacase2.count()
# 94753450

non=non.select('ifa')
data1=data.select('ifa')
non_robi=data1.instersect(non)
non_robi_dahaka=non_robi.instersect(dhaka)
non_robi_dhaka.count()
# 1897167
car = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/*/*')
car = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/202{001,002,003,004,005,006,007,008,009,010,011,012,101}/*')
car = car.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
none = car.select('ifa').where(~col("req_carrier_name").like("%Robi%"))
none.printSchema()
churn = spark.read.csv('RobiChurn/q3/telco_churn_model.csv',header=True)
c2=churn.filter((churn['carrier']=='grameen')|(churn['carrier']=='banglalink'))

c1=c2.select('ifa')


never_robi=c1.intersect(none)
never_robi_dhk=never_robi.intersect(dhaka)
never_robi_dhk.count()
# 1581067
result =non_robi_d

result =  non_robi_dhaka.intersect(never_robi_dhk)
result.count()
# 1581067
result1=never_robi_dhk.intersect(non_robi_dhaka)
result1.count()
# 1581067


from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys
 
churn = spark.read.csv('RobiChurn/q3/telco_churn_model.csv',header=True)
connection_last_six = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/2021{02,03,04,05,06,07}/*.parquet')
connection_last_tharteen = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/202{001,002,003,004,005,006,007,008,009,010,011,012,101}/*.parquet')
connection_all_along=spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/*/*.parquet')
all_churn=churn.filter((churn['carrier']=='robi') | (churn['carrier']=='grameen')|(churn['carrier']=='banglalink'))
robi_churn=churn.filter(churn['carrier']=='robi')
churn_gp_bl=churn.filter((churn['carrier']=='grameen')|(churn['carrier']=='banglalink'))
connection_last_six = connection_last_six.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
connection_last_tharteen = connection_last_tharteen.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
connection_all_along = connection_all_along.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
just_robi_therteen=connection_last_tharteen.select('ifa').where(col("req_carrier_name").like("%Robi%")).distinct()
non_robi_six = connection_last_six.select('ifa').where(~col("req_carrier_name").like("%Robi%")).distinct()
never_been_robi=connection_all_along.select('ifa').where(~col("req_carrier_name").like("%Robi%")).distinct()

location = spark.read.parquet('etl/data/brq/sub/home-office/home-office-data/BD/202{006,012,103,106}/home-location/*.parquet')
dhaka = location.select('ifa').where(col('home_district').like('%Dhaka%')).distinct()


 
non=non.select('ifa','req_carrier_name')
non.groupBy('req_carrier_name').count().show()
# +------------------+---------+
# |  req_carrier_name|    count|
# +------------------+---------+
# |Orascom/Banglalink| 59262650|
# |      GrameenPhone|142666518|
# |          TeleTalk|  6345987|
# +------------------+---------+

location=spark.read.parquet('etl/data/brq/sub/home-office/home-office-data/BD/202{006,012,103,106}/home-location/*.parquet')
 
dhaka=location.select('ifa').where(col('home_district').like('%Dhaka%')).distinct()
dhaka.printSchema()
# root
#  |-- ifa: string (nullable = true)

car = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/*/*')

car = spark.read.parquet('etl/data/brq/sub/connection/monthly/BD/202{001,002,003,004,005,006,007,008,009,010,011,012,101}/*')
car = car.select('ifa', F.explode('req_connection')).select('ifa', 'col.*')
none = car.select('ifa').where(~col("req_carrier_name").like("%Robi%"))
only_robi= car.select('ifa').where(col("req_carrier_name").like("%Robi%"))
non.printSchema()
# root
#  |-- ifa: string (nullable = true)
#  |-- req_carrier_name: string (nullable = true)

sixnonRobi=non.select('ifa')
#  only_robi.printSchema()
# root
#  |-- ifa: string (nullable = true)


case23 = sixnonRobi.join(only_robi,on='ifa',how='left_anti')
case23.count()
# 155239422
case22=case23.join(dhaka,on='ifa',how='left_anti')
case22.count()
# 139779742
case24=only_robi.join(sixnonRobi,on='ifa',how='left_anti')
case24.count()
# 212853791
sixnonRobi=sixnonRobi.distinct()
only_robi=only_robi.distinct()
case23 = sixnonRobi.join(only_robi,on='ifa',how='left_anti')
case23.count()
# 30778128
dhaka=dhaka.distinct()
case22=case23.join(dhaka,on='ifa',how='left_anti')
case22.count()
# 28656367
case21=case22.join(data,on='ifa',how='left_anti')
case21.count()
# 22926527
case22=dhaka.join(case23,on='ifa',how='left_anti')
case20=data.join(case22,on='ifa',how='left_anti')
case20.count()
#
# +--------------------------------------------+
# |ifa|period_2021|period_2020_2021            |
# +--------------------------------------------+
# |00036525-91dd-462a-abbf-76bd25fbe8a0|No|No  |



