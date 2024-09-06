"""
This script gets the streaming data from Kafka topic and writes it to MinIO.
"""

import sys
import warnings
import traceback
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')
checkpointDir = f"s3a://minio-bucket/checkpoints"  # Update this path based on your environment

def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
        spark = (SparkSession.builder
                 .appName("Streaming Kafka to MinIO")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.hadoop:hadoop-aws:3.2.0")
                 .getOrCreate())
        logging.info('Spark session successfully created')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
        sys.exit(1)

    return spark

def read_minio_credentials():
    # Hardcoded for testing
    accessKeyId = 'admin'
    secretAccessKey = 'password'
    logging.info('MinIO credentials are set correctly')
    return accessKeyId, secretAccessKey

def load_minio_config(spark_context: SparkContext):
    """
    Establishes the necessary configurations to access MinIO.
    """
    accessKeyId, secretAccessKey = read_minio_credentials()
    try:
        conf = spark_context._jsc.hadoopConfiguration()
        conf.set("fs.s3a.access.key", accessKeyId)
        conf.set("fs.s3a.secret.key", secretAccessKey)
        conf.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("fs.s3a.connection.ssl.enabled", "false")
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")
        sys.exit(1)

def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        df = (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "WEATHER")
            .load())
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Initial dataframe couldn't be created due to exception: {e}")
        sys.exit(1)

    return df

def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe and creates the final dataframe.
    """
    try:
        df2 = df.selectExpr("CAST(value AS STRING)")
        
        df3 = df2.withColumn("date", F.split(F.col("value"), ",")[0].cast(StringType())) \
                 .withColumn("temperature_2m", F.split(F.col("value"), ",")[1].cast(FloatType())) \
                 .withColumn("relative_humidity_2m", F.split(F.col("value"), ",")[2].cast(FloatType())) \
                 .withColumn("rain", F.split(F.col("value"), ",")[3].cast(FloatType())) \
                 .withColumn("snowfall", F.split(F.col("value"), ",")[4].cast(FloatType())) \
                 .withColumn("weather_code", F.split(F.col("value"), ",")[5].cast(FloatType())) \
                 .withColumn("surface_pressure", F.split(F.col("value"), ",")[6].cast(FloatType())) \
                 .withColumn("cloud_cover", F.split(F.col("value"), ",")[7].cast(FloatType())) \
                 .withColumn("cloud_cover_low", F.split(F.col("value"), ",")[8].cast(FloatType())) \
                 .withColumn("cloud_cover_high", F.split(F.col("value"), ",")[9].cast(FloatType())) \
                 .withColumn("wind_direction_10m", F.split(F.col("value"), ",")[10].cast(FloatType())) \
                 .withColumn("wind_direction_100m", F.split(F.col("value"), ",")[11].cast(FloatType())) \
                 .withColumn("soil_temperature_28_to_100cm", F.split(F.col("value"), ",")[12].cast(FloatType())) \
                 .drop(F.col("value"))

        df3.createOrReplaceTempView("df3")

        logging.info("Final dataframe created successfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Final dataframe couldn't be created due to exception: {e}")
        sys.exit(1)

    return df3

def start_streaming(df):
    """
    Starts the streaming to write data into MinIO.
    """
    logging.info("Streaming is being started...")
    try:
        stream_query = (df.writeStream
                            .format("csv")
                            .outputMode("append")
                            .option('header', 'true')
                            .option("checkpointLocation", checkpointDir)
                            .option("path", 's3a://minio-bucket')  # The bucket my_bucket should be created in MinIO
                            .start())

        stream_query.awaitTermination()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Streaming failed due to exception: {e}")
        sys.exit(1)

if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)
