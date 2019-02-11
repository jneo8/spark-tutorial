from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col
import time


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("structured-streaming").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    data_path = "/home/jameslin/Github-Project/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv"
    print(f"data_path: {data_path}")

    # Read data
    df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True,
    )

    # df.createOrReplaceTempView("retail_data")
    df.show()
    staticSchema = df.schema
    df.printSchema()
