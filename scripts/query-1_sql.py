from pyspark.sql import SparkSession

export_path = 'hdfs://advdb-master:54310/user/master/exports/'

spark = SparkSession.builder \
        .appName('query-1_SQL').getOrCreate()

query1_df = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .load(export_path) \
        .createOrReplaceTempView('table1')
spark.sql("""
        SELECT *
        FROM (
            SELECT
                EXTRACT(year FROM `Date Rptd`) AS year,
                EXTRACT(month FROM `Date Rptd`) AS month,
                COUNT(*) AS crime_total,
                ROW_NUMBER() OVER (PARTITION BY (EXTRACT(year FROM `Date Rptd`)) ORDER BY COUNT(*) DESC) as `#`
            FROM table1
            GROUP BY year, month
            ORDER BY year ASC, month DESC
        )
        WHERE `#` < 4;
    """).show()
