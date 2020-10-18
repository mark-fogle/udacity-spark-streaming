import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),\
                     StructField("original_crime_type_name", StringType(), True),\
                     StructField("report_date", StringType(), True),\
                     StructField("call_date", StringType(), True),\
                     StructField("offense_date", StringType(), True),\
                     StructField("call_time", StringType(), True),\
                     StructField("call_date_time", TimestampType(), True),\
                     StructField("disposition", StringType(), True),\
                     StructField("address", StringType(), True),\
                     StructField("city", StringType(), True),\
                     StructField("state", StringType(), True),\
                     StructField("agency_id", StringType(), True),\
                     StructField("address_type", StringType(), True),\
                     StructField("common_location", StringType(), True)])

def run_spark_job(spark):

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "earliest") \
        .option("subscribe","gov.sfpd.servicecalls") \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING) as service_call")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('service_call'), schema).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table.select(service_table.original_crime_type_name, service_table.disposition, service_table.call_date_time)\
        .distinct()

    # count the number of original crime type
    agg_df = distinct_table \
            .select(distinct_table.original_crime_type_name, distinct_table.disposition, distinct_table.call_date_time) \
            .withWatermark("call_date_time", "5 minutes") \
            .groupby(psf.window(distinct_table.call_date_time,"30 minutes","1 minute"), distinct_table.original_crime_type_name) \
            .count()\
            .orderBy("count", ascending=False)

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    agg_df.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
        .option("numRows", 30)\
        .start()\
        .awaitTermination()


    radio_code_json_filepath = "../data/radio_code.json"
    radio_code_df = spark.read\
                        .json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join_query") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
