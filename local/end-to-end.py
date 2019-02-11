from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
import time


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("end-to-end").getOrCreate()

    # set configuration
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    df = spark.read.csv(
        "/home/jameslin/Github-Project/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv",
        header=True,
        inferSchema=True,
    ).cache()

    # Explain
    df.sort("count").explain()

    df.createOrReplaceTempView("flight_data_2015")

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
