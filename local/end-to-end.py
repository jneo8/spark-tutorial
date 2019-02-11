from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, desc
import time


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("end-to-end").getOrCreate()

    # set configuration
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Read data
    df = spark.read.csv(
        "/home/jameslin/Github-Project/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv",
        header=True,
        inferSchema=True,
    ).cache()
    df.sort("count").explain()

    ## you can make any dataframe into a table or view with one simple method call:
    df.createOrReplaceTempView("flight_data_2015")

    # DataFrames & SQL
    ## Explain
    sql_way = spark.sql("""
            SELECT DEST_COUNTRY_NAME, count(1)
            FROM flight_data_2015
            GROUP BY DEST_COUNTRY_NAME
    """)

    data_frame_way = df.groupBy("DEST_COUNTRY_NAME").count()

    sql_way.explain()
    data_frame_way.explain()

    max_count = df.select(max("count")).take(1)
    print(f"max coount: {max_count}")


    max_sql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)
    max_sql.explain()
    max_sql.show()

    (
        df
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .sort(desc("destination_total"))
        .limit(5)
        .explain()
    )
    (
        df
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .sort(desc("destination_total"))
        .limit(5)
        .show()
    )
