#Nagad Segments for insight only

import pyspark.sql.functions as F

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("Games").getOrCreate()
path = 's3a://ada-bd-emr/result/2022/Naagad/Persona/'
affluence = spark.read.parquet('etl/data/brq/sub/affluence/monthly/BD/202112/*.parquet')
age = spark.read.parquet('etl/table/brq/sub/demographics/monthly/BD/202112/age/*.parquet')
gender = spark.read.parquet('etl/table/brq/sub/demographics/monthly/BD/202112/gender/*.parquet')

affluence.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending=False).show(10,
                                                                                                                  False)
# +---------------+--------+
# |final_affluence|freq    |
# +---------------+--------+
# |Mid            |13531418|
# |High           |12403727|
# |Low            |11434255|
# |Ultra High     |4301961 |
# |{}             |39      |
# +---------------+--------+

affluence = affluence.select('ifa', 'final_affluence')

### low affluence with the age of 18-49 ###

low_aff = affluence.filter(affluence['final_affluence'] == 'Low')
low_age = age.filter((age['prediction'] == '18-24') | (age['prediction'] == '25-34') | (age['prediction'] == '35-49'))
final_low_aff_18_49 = low_age.join(low_aff, on='ifa')
final_low_aff_18_49.count()
# 9378991


### Home Maker/housewife with the age of 24
home_maker_g = gender.filter(gender['prediction'] == 'F')
# 21808064
home_maker_a = age.filter(
    (age['prediction'] == '18-24') | (age['prediction'] == '25-34') | (age['prediction'] == '35-49'))
# 39462117

home_maker = home_maker_a.join(home_maker_g, on='ifa')
# 21698081




