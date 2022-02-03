from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys


country = 'BD'
year = '202106'

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

#case 1
path = 'etl/data/brq/agg/agg_brq/monthly/BD/202{001,002,003,004,005,006,007,008,009,010,011,012,' \
       '101,102,103,104,105,106,107}/*.parquet '
path1 = 'etl/data/brq/agg/agg_brq/monthly/BD/202{102,103,104,105,106,107}/*.parquet'
df=spark.read.parquet(path)
location = df.select('ifa',F.explode('gps.city').alias('district'))
location1=location.filter(location['district']=='Dhaka')

persona=location1.select('ifa')

churn = spark.read.csv('RobiChurn/q3/telco_churn_model.csv',header=True)

persona=persona.distinct()

actual=churn.join(persona,on='ifa',how='left')

output='RobiChurn/q3/output/'
actual.coalesce(1).write.csv(result_path + "TG", mode='overwrite', header=True)

telco = df.select('ifa', 'connection')
# telco = df.select('ifa', 'connection')
connection = telco.select('ifa', F.explode('connection.req_carrier_name').alias('telco'))
connection1 = telco.select('ifa', F.explode('connection.mm_con_type_desc').alias('con_type'))
connection2= telco.select('ifa', F.explode('connection.req_con_type').alias('network'))

con_data = connection.join(connection1, on='ifa', how='left')
con_data1 = con_data.join(connection2,on='ifa',how='left')
all_opr = location1.join(con_data1, on='ifa', how='left')

master_data= all_opr.distinct()
telco_data = master_data.filter(master_data['con_type']=='Cellular')
not_robi_telco_data = master_data.filter(master_data['telco']!='Robi/Aktel')

