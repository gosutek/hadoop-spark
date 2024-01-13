from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

crime_2010_2019_path = 'hdfs://advdb-master:54310/user/master/crime_data/crimedata-2010-2019.csv'
crime_2020_pre_path = 'hdfs://advdb-master:54310/user/master/crime_data/crimedata-2020-present.csv'
export_path = 'hdfs://advdb-master:54310/user/master/exports/'

spark = SparkSession.builder \
        .appName('preprocessing').getOrCreate()

crime_data_schema = StructType([
    StructField('DR_NO', StringType(), True),
    StructField('Date Rptd', StringType(), True),
    StructField('DATE OCC', StringType(), True),
    StructField('TIME OCC', StringType(), True),
    StructField('AREA', StringType(), True),
    StructField('AREA NAME', StringType(), True),
    StructField('Rpt Dist No', StringType(), True),
    StructField('Part 1-2', DoubleType(), True),
    StructField('Crm Cd', StringType(), True),
    StructField('Crm Cd Desc', StringType(), True),
    StructField('Mocodes', StringType(), True),
    StructField('Vict Age', IntegerType(), True),
    StructField('Vict Sex', StringType(), True),
    StructField('Vict Descent', StringType(), True),
    StructField('Premis Cd', StringType(), True),
    StructField('Premis Desc', StringType(), True),
    StructField('Weapon Used Cd', IntegerType(), True),
    StructField('Weapon Desc', StringType(), True),
    StructField('Status', StringType(), True),
    StructField('Status Desc', StringType(), True),
    StructField('Crm Cd 1', StringType(), True),
    StructField('Crm Cd 2', StringType(), True),
    StructField('Crm Cd 3', StringType(), True),
    StructField('Crm Cd 4', StringType(), True),
    StructField('LOCATION', StringType(), True),
    StructField('Cross Street', StringType(), True),
    StructField('LAT', DoubleType(), True),
    StructField('LON', DoubleType(), True)
    ])

crime_df = spark.read \
        .option('header', 'true') \
        .schema(crime_data_schema) \
        .csv(crime_2010_2019_path) \

second = spark.read \
        .option('header', 'true') \
        .schema(crime_data_schema) \
        .csv(crime_2020_pre_path)

crime_df.union(second)
crime_df = crime_df.withColumn('Date Rptd', to_date(col('Date Rptd'), format='MM/dd/yyyy hh:mm:ss a'))
crime_df = crime_df.withColumn('DATE OCC', to_date(col('DATE OCC'), format='MM/dd/yyyy hh:mm:ss a'))
crime_df.write \
        .option('header', 'true') \
        .csv(export_path, mode='overwrite')
crime_rows = crime_df.count()
print(f'Crime data rows -> {crime_rows}')
crime_df.printSchema()
