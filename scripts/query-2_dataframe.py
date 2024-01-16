from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

export_path = "hdfs://advdb-master:54310/user/master/exports/"

spark = SparkSession.builder.appName("query-2_DATAFRAME").getOrCreate()

query2_df = spark.read.format("csv").option("header", "true").load(export_path)
time_of_day = (
    when((col("hour") >= 5) & ((col("hour") <= 11) & (col("minute") <= 59)), "Morning")
    .when(
        (col("hour") >= 12) & ((col("hour") <= 16) & (col("minute") <= 59)), "Afternoon"
    )
    .when(
        (col("hour") >= 17) & ((col("hour") <= 20) & (col("minute") <= 59)), "Evening"
    )
    .otherwise("Night")
)
query2_df = query2_df.withColumn("TIME OCC", to_timestamp(col("TIME OCC"), "HHmm"))
query2_df = query2_df.select(
    hour("TIME OCC").alias("hour"),
    minute("TIME OCC").alias("minute"),
    col("Premis Desc"),
).filter(col("Premis Cd") == "101")
query2_df = query2_df.withColumn("time_of_day", time_of_day)
query2_df = (
    query2_df.groupBy("time_of_day")
    .agg(count("Premis Desc").alias("crimes_commited"))
    .orderBy(col("crimes_commited").desc())
)
query2_df.show(5)
