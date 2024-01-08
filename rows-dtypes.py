from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import year, count, to_date, col

crime_2010_2019_path = 'hdfs://advdb-master:54310/user/master/crime_data/crimedata-2010-2019.csv'
crime_2020_pre_path = 'hdfs://advdb-master:54310/user/master/crime_data/crimedata-2020-present.csv'

spark = SparkSession.builder \
        .appName('advdb').getOrCreate()

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
    StructField('Weapon Used Cd', StringType(), True),
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
crime_df = crime_df.withColumn('Date Rptd', to_date(col('Date Rptd'), format='MM/dd/yyy hh:mm:ss a'))
crime_df = crime_df.withColumn('DATE OCC', to_date(col('DATE OCC'), format='MM/dd/yyyy hh:mm:ss a'))
crime_rows = crime_df.count()
print(f'Crime data rows -> {crime_rows}')
crime_df.printSchema()

query1_df = crime_df.groupBy(year('Date Rptd').alias('year'),month('Date Rptd').alias('month') \
        .agg(count('DR_NO').alias('crime_total')) \
        .orderBy(col('year').asc(), col('crime_total').desc())
query1_df = test_df.withColumn('#', row_number().over( \
        Window.partitionBy('year') \
        .orderBy(desc('crime_total')))) \
        .filter(col('#') < 4)
query1_df.show()
