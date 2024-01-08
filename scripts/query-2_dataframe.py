from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

export_path = 'hdfs://advdb-master:54310/user/master/exports/'

spark = SparkSession.builder \
        .appName('query-2_DATAFRAME').getOrCreate()
