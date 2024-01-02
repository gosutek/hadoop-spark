from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.read.csv('crimedata-2010-2019.csv', header=True).show()
