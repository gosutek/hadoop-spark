from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

export_path = 'hdfs://advdb-master:54310/user/master/exports/'
income_path = 'hdfs://advdb-master:54310/user/master/other_data/LA_income_2015.csv'
revgeo_path = 'hdfs://advdb-master:54310/user/master/other_data/revgecoding.csv'

vict_code = { \
        'A' : 'Other Asian',
        'B' : 'Black',
        'C' : 'Chinese',
        'D' : 'Cambodian',
        'F' : 'Filipino',
        'G' : 'Guamanian',
        'H' : 'Hispanic/Latin/Mexican',
        'I' : 'American Indian/Alaskan Native',
        'J' : 'Japanese',
        'K' : 'Korean',
        'L' : 'Laotian',
        'O' : 'Other',
        'P' : 'Pacific Islander',
        'S' : 'Samoan',
        'U' : 'Hawaiian',
        'V' : 'Vietnamese',
        'W' : 'White',
        'X' : 'Unknown',
        'Z' : 'Asian Indian'
        }
#my_decoder = udf(lambda cd: vict_code[cd])
spark = SparkSession.builder \
        .appName('query-3').getOrCreate()

query3_df = spark.read.format('csv').option('header', 'true').load(export_path)
income = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(income_path)
revgeo = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(revgeo_path)

query3_df = query3_df.filter( (year('Date Rptd') == 2015) & (col('Vict Descent').isNotNull()) ) # filter out the nulls
revgeo = revgeo.withColumn('ZIPCode', revgeo['ZIPCode'].cast('integer'))
income = income.withColumn('Estimated Median Income', \
        regexp_replace('Estimated Median Income', '[$,]', '').cast('integer'))
rich = income.orderBy(desc('Estimated Median Income')).limit(3)
income = income.orderBy(asc('Estimated Median Income')).limit(3).union(rich)
revgeo.join(income, revgeo['ZIPcode'] == income['Zip Code'], 'leftsemi')
query3_df.join(revgeo, \
        (query3_df['LAT'] == revgeo['LAT']) & (query3_df['LON'] == revgeo['LON']), \
        'leftsemi')
query3_df = query3_df.withColumn('Vict Descent', udf(lambda x: vict_code[x])('Vict Descent'))
query3_df = query3_df.groupBy('Vict Descent') \
        .agg(count('Vict Descent').alias('#')) \
        .orderBy(col('#').desc())
query3_df.show(5)
