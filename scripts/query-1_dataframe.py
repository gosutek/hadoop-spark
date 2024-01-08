from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

export_path = 'hdfs://advdb-master:54310/user/master/exports/'

spark = SparkSession.builder \
        .appName('query-1_DATAFRAME').getOrCreate()

query1_df = spark.read.format('csv').option('header', 'true').load(export_path)
query1_df = query1_df.groupBy(year('Date Rptd').alias('year'),month('Date Rptd').alias('month')) \
        .agg(count('DR_NO').alias('crime_total')) \
        .orderBy(col('year').asc(), col('crime_total').desc())
query1_df = query1_df.withColumn('#', row_number().over( \
        Window.partitionBy('year') \
        .orderBy(desc('crime_total')))) \
        .filter(col('#') < 4)
query1_df.show()

