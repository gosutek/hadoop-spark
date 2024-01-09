import re
from pyspark.sql import SparkSession

export_path = 'hdfs://advdb-master:54310/user/master/exports/'

def time_of_day(str):
    x = re.search(r'(?<=T)[^:]+', str)
    if x is not None:
        iH = int(x.group())
        f = lambda iH, hDwn, hUp, v: v if iH >= hDwn and iH < hUp else None
        return \
                f(iH, 5, 12, 'Morning') or \
                f(iH, 12, 17, 'Afternoon') or \
                f(iH, 17, 21, 'Evening') or \
                'Night'

spark = SparkSession.builder \
        .appName('query-2_RDD').getOrCreate()

query2_df = spark.read.format('csv').option('header', 'true').load(export_path)
rdd = query2_df.rdd
rdd = rdd.filter(lambda row : row['Premis Cd'] == '101')
rdd = rdd.map(lambda row : time_of_day(row['TIME OCC']))
dict = rdd.countByValue()
print(dict.items())
