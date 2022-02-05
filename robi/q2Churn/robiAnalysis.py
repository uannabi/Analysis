#ETL code by roy
sc.addPyFile('data_preprocessor_eskimi.py')
sc.addPyFile('data_preprocessor.py')
sc.addPyFile('geo_grid_base.py')
sc.addPyFile('definitive_churner_utils.py')
sc.addPyFile('telco_utils.py')
sc.addPyFile('definitive_churners.py')

import definitive_churners as dp
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import FloatType
from geo_grid_base import *

logic = 'fextraction'
dp.country = 'BD'
dp.is_development= False
dp.use_mcc_mnc = True
dp.spark = spark = dp.get_spark()

year = 2021
month = 7

df = spark.read.parquet('/telco_churn_predictor/model_2021/predictions/BD/202107/churn_predictions/full/')
df = df.select('ifa')
df = df.dropDuplicates(['ifa'])


while year >= 2020:
    df2 = dp.get_or_read_agg_data(year, month, logic)
    df2 = df2.select('ifa', df2.carrier_connection_type.carrier.alias('%d%02d'%(year, month)))
    df = df.join(df2, ['ifa'], 'left')
    year, month = subtract_month(year, month, 1)

from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import StringType


def check_for_robi(row):
    for carrier_list in row:
        if carrier_list is not None:
            if 'robi' in carrier_list:
                return 'Yes'
    return 'No'


def convert_col(carrier_list):
    carrier_str = ''
    if carrier_list is not None:
        carrier_str = ','.join(carrier_list)
    return carrier_str


check_for_robi_udf = udf(check_for_robi, StringType())
convert_col_udf = udf(convert_col, StringType())

df = df.withColumn("period_2021", check_for_robi_udf(struct([df['2021%02d' % m] for m in range(1, 8)])))
df = df.withColumn("period_2020_2021", check_for_robi_udf(struct([df['2020%02d' % m] for m in range(1, 13)])))

for m in range(1, 13):
    df = df.withColumn('2020%02d_str' % m, convert_col_udf(df['2020%02d' % m]))
    df = df.drop('2020%02d' % m)
    df = df.withColumnRenamed('2020%02d_str' % m, '2020%02d' % m)

for m in range(1, 8):
    df = df.withColumn('2020%02d' % m, convert_col_udf(df['2021%02d' % m]))
    df = df.drop('2021%02d' % m)
    df = df.withColumnRenamed('2021%02d_str' % m, '2020%02d' % m)

df.cache()
df.count()

df.coalesce(1).write.option("sep","|")\
.option("emptyValue", None)\
.option("nullValue", None)\
.csv('/telco_churn_predictor/model_2021/robi/full', header=True, mode='overwrite')

df.select('ifa', 'period_2021', 'period_2020_2021').coalesce(1).write\
.option("sep","|")\
.option("emptyValue", None)\
.option("nullValue", None)\
.csv('/telco_churn_predictor/model_2021/robi/reduced', header=True, mode='overwrite')
