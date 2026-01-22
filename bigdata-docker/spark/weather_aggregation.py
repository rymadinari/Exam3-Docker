from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as sum_, to_timestamp
from pyspark.sql.types import *
from hdfs import InsecureClient

def main():
    # création du dossier HDFS
    client = InsecureClient('http://namenode:50070', user='hdfs')
    client.makedirs('/user/hdfs/weather/output')

    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    schema = StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("temp_f", DoubleType()),
        StructField("high_wind_alert", BooleanType()),
        StructField("time", StringType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_transformed") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("time").isNotNull()) \
        .withColumn("event_time", to_timestamp(col("time"))) \
        .filter(col("event_time").isNotNull()) \
        .withWatermark("event_time", "20 seconds")

    agg = parsed.groupBy(
        window(col("event_time"), "10 seconds")
    ).agg(
        avg("temperature").alias("avg_temp_c"),
        sum_(col("high_wind_alert").cast("int")).alias("alert_count")
    )

    agg.writeStream \
        .format("csv") \
        .option("path", "hdfs://namenode:8020/user/hdfs/weather/output") \
        .option("checkpointLocation", "hdfs://namenode:8020/user/hdfs/weather/checkpoints") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
