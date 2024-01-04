from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType

path_10_19 = "hdfs://advdb-master:54310/user/master/pyspark/data/crimedata-2010-2019.csv"
path_20_pre = "hdfs://advdb-master:54310/user/master/pyspark/data/crimedata-2020-present.csv"

spark = SparkSession \
        .builder \
        .appname("Advdb") \
        .getOrCreate()

crime_data_schema = StructType([
    StructField("DR_NO", StringType(), True),
    StructField("Date Rptd", DateType(), True),
    StructField("DATE OCC", DateType(), True),
    StructField("TIME OCC", StringType(), True),
    StructField("AREA", StringType(), True),
    StructField("AREA NAME", StringType(), True),
    StructField("Rpt Dist No", StringType(), True),
    StructField("Part 1-2", DoubleType(), True),
    StructField("Crm Cd", StringType(), True),
    StructField("Crm Cd Desc", StringType(), True),
    StructField("Mocodes", StringType(), True),
    StructField("Vict Age", IntegerType(), True),
    StructField("Vict Sex", StringType(), True),
    StructField("Vict Descent", StringType(), True),
    StructField("Premis Cd", StringType(), True),
    StructField("Premis Desc", StringType(), True),
    StructField("Weapon Used Cd", StringType(), True),
    StructField("Weapon Desc", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Status Desc", StringType(), True),
    StructField("Crm Cd 1", StringType(), True),
    StructField("Crm Cd 2", StringType(), True),
    StructField("Crm Cd 3", StringType(), True),
    StructField("Crm Cd 4", StringType(), True),
    StructField("LOCATION", StringType(), True),
    StructField("Cross Street", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True)
    ])

crime_df = spark.read.csv(path_10_10 + "," + path_20_pre, header=True, schema = crime_data_schema)

crime_df.show(5)
