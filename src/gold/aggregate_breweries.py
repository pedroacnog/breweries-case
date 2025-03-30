import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_SILVER = os.getenv("BUCKET_SILVER")
BUCKET_GOLD = os.getenv("BUCKET_GOLD")


def init_spark():
    return SparkSession.builder \
        .appName("GoldBreweryAggregations") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .getOrCreate()


def main():
    logging.info("Starting Gold: main agg and one extra")
    spark = init_spark()

    df = spark.read.parquet(f"s3a://{BUCKET_SILVER}/partitioned")

    # Quantity of breweries per type and country
    logging.info("Generating main view: brewery count by type and country")
    breweries_by_type_country = df.groupBy("brewery_type", "country").count()
    breweries_by_type_country.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/breweries_by_type_country")

    # Top 10 states with the most active breweries
    logging.info("extra view: top 10 states with most active breweries")
    top_states = df.filter(col("active_status") == True) \
        .groupBy("state") \
        .agg(count("id").alias("total_breweries")) \
        .orderBy(desc("total_breweries")) \
        .limit(10)
    top_states.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/top_states_active")


    logging.info("Gold completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
