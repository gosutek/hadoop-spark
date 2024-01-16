from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
import geopy.distance

export_path = 'hdfs://advdb-master:54310/user/master/exports/'
stations_path = 'hdfs://advdb-master:54310/user/master/other_data/LAPD_Police_Stations.csv'
lib_path = 'hdfs://advdb-master:54310/user/master/lib/'

def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic(( float(lat1), float(lon1) ), ( float(lat2), float(lon2) )).km

def min_distance(lat1, lon1):
    _min = float('inf')
    for pair in pd_coords.keys():
        dst = geopy.distance.geodesic((lat1, lon1), pair).km
        if (dst < _min):
            _min = dst
            min_pair = pair
    return [ _min, pd_coords[min_pair] ]

spark = SparkSession.builder \
        .appName('query-4').getOrCreate()

spark.sparkContext.addPyFile(lib_path + 'geopy.zip')
spark.sparkContext.addPyFile(lib_path + 'geographiclib.zip')

query4_df = spark.read.format('csv').option('header', 'true').load(export_path)

stations = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(stations_path)

pd_coords = { (row['Y'], row['X']) : row['DIVISION'] for row in stations.collect() }
query4_df = query4_df.filter(query4_df['Weapon Used Cd'].startswith('1'))
query4_df = query4_df.withColumn('AREA', query4_df['AREA'].cast('Integer'))
query4_df = query4_df.withColumn('LAT', query4_df['LAT'].cast(DoubleType()))
query4_df = query4_df.withColumn('LON', query4_df['LON'].cast(DoubleType()))
query4_df = query4_df.filter(( query4_df['LAT'] != 0 ) & ( query4_df['LON'] != 0 ))
# Query4-1
join_1 = query4_df.join(stations, query4_df['AREA'] == stations['PREC'], 'inner')
join_1 = join_1.withColumn('pd_distance', \
                                 udf(lambda lat1, lon1, lat2, lon2: \
                                     get_distance(lat1, lon1, lat2, lon2)) \
                                 ( join_1.LAT, join_1.LON, join_1.Y, join_1.X )).cache()
query4_1_df = join_1.groupBy(year('Date Rptd').alias('year')) \
        .agg(round(avg('pd_distance'), 3).alias('average_distance'), count('DR_NO').alias('#')) \
        .orderBy(col('year').asc())
query4_1_df.show(5)
# Query4-2
query4_2_df = join_1.groupBy(col('DIVISION').alias('division')) \
        .agg(round(avg('pd_distance'), 3).alias('average_distance'), count('DR_NO').alias('#')) \
        .orderBy(col('division').desc())
query4_2_df.show(5)
# Query4-3
min_df = query4_df.withColumn('result', \
                                   udf(lambda lat1, lon1: \
                                       min_distance(lat1, lon1), \
                                       StructType([StructField('dst', DoubleType()), StructField('div', StringType())])) \
                                   (query4_df.LAT, query4_df.LON))
min_df = min_df.withColumn('pd_distance', col('result.dst'))
min_df = min_df.withColumn('DIVISION', col('result.div'))
min_df = min_df.drop('result').cache()
query4_3_df = min_df.groupBy(year('Date Rptd').alias('year')) \
        .agg(round(avg('pd_distance'), 3).alias('average_distance'), count('DR_NO').alias('#')) \
        .orderBy(col('year').asc())
query4_3_df.show(5)
# Query4-4
query4_4_df = min_df.groupBy(col('DIVISION').alias('division')) \
        .agg(round(avg('pd_distance'), 3).alias('average_distance'), count('DR_NO').alias('#')) \
        .orderBy(col('division').desc())
query4_4_df.show(5)
