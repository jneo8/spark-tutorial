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

    # (
    #     df
    #     .selectExpr(
    #         "CustomerId",
    #         "(UnitPrice * Quantity) as total_cost",
    #         "INvoiceDate"
    #     )
    #     .groupBy(
    #         col("CustomerId"), window(col("InvoiceDate"), "1 day")
    #     )
    #     .sum("total_cost")
    #     .show(5)
    # )

    streaming_df = spark.readStream.option("maxFilesPerTrigger", 1).csv(
        data_path,
        schema=staticSchema,
        header=True,
    )
    # print(streaming_df.isStreaming)

    purchaseByCustomerPerHour = (
        streaming_df
        .selectExpr(
            "CustomerId",
            "(UnitPrice * Quantity) as total_cost",
            "INvoiceDate"
        )
        .groupBy(
            col("CustomerId"), window(col("InvoiceDate"), "1 day")
        )
        .sum("total_cost")
    )

    sq = (
        purchaseByCustomerPerHour
        .writeStream
        .format("memory")
        .queryName("customer_purchases")
        .outputMode("complete")
        .start()
    )
    sq.awaitTermination(10)

    for i in range(10):
        time.sleep(1)
        spark.sql("""
            SELECT *
            FROM customer_purchases
            ORDER BY `sum(total_cost)` DESC
        """).show(10)

    sq.stop()
