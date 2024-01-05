from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType

DATA_PATH = "hdfs://advdb-master:54310/user/master/crime_data/"
PYSPARK_PATH = "hdfs://advdb-master:54310/user/master/pyspark"

spark = SparkSession.builder \
        .appName("preprocessing").getOrCreate

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

crime_df = spark.read \
        .schema(crime_data_schema) \
        .csv(crime_data_path) \

crime_df.write.save(PYSPARK_PATH, format="csv", mode="overwrite")
