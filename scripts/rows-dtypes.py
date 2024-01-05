from pyspark.sql import SparkSession

PYSPARK_PATH = "hdfs://advdb-master:54310/user/master/pyspark/"

spark = SparkSession.builder \
        .appName("rows-dtypes").getOrCreate()

crime_df = spark.read.csv(PYSPARK_PATH)

crime_rows = crime_df.count()
print(f"Crime data rows -> {crime_rows}")
for col in crime_df.dtypes:
    print(col[0] + " -> " + col[1])
