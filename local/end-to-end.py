from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("end-to-end").getOrCreate()
    print(dir(spark))
    df = spark.read.csv(
        "/home/ubuntu/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv",
        header=True,
        inferSchema=True,
    ).cache()

    print(df.collect())
    print(df.take(3))

