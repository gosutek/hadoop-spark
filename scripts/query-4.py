from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
import geopy.distance

export_path = 'hdfs://advdb-master:54310/user/master/exports/'
stations_path = 'hdfs://advdb-master:54310/user/master/other_data/LAPD_Police_Stations.csv'

def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km

spark = SparkSession.builder \
        .appName('query-4').getOrCreate()

query4_df = spark.read.format('csv').option('header', 'true').load(export_path)

stations = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(stations_path)

stations.printSchema()
query4_df = query4_df.filter(query4_df['Weapon Used Cd'].startswith('1'))
query4_df = query4_df.withColumn('AREA', query4_df['AREA'].cast('Integer'))
query4_df = query4_df.join(stations, query4_df['AREA'] == stations['PREC'], 'inner')
query4_df = query4_df.groupBy(year('Date Rptd').alias('year')) \
        .agg(count('DR_NO').alias('#')) \
        .orderBy(col('year').asc())
query4_df.show(5)
