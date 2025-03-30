import os
import boto3
import logging
from unidecode import unidecode 
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.functions import col, lower, concat_ws, trim, udf

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_BRONZE = os.getenv("BUCKET_BRONZE")
BUCKET_SILVER = os.getenv("BUCKET_SILVER")

def get_latest_file_path(bucket, prefix, endpoint, access_key, secret_key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = response.get("Contents", [])
    if not files:
        raise FileNotFoundError("No files found in Bronze bucket")
    latest = max(files, key=lambda x: x["LastModified"])

    return f"s3a://{bucket}/{latest['Key']}"

def init_spark():
    return SparkSession.builder \
        .appName("SilverBreweryTransformation") \
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

@udf(StringType())
def normalize_ascii(text):
    if text:
        return unidecode(text)
    
    return text

def main():
    logging.info("Starting Silver transformation with advanced cleaning and formatting.")
    spark = init_spark()

    latest_file_path = get_latest_file_path(
        bucket=BUCKET_BRONZE,
        prefix="breweries_",
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    output_path = f"s3a://{BUCKET_SILVER}/partitioned"

    logging.info(f"Reading raw file from Bronze: {latest_file_path}")
    df = spark.read.json(latest_file_path)

    df_clean = df.dropDuplicates(["id"]) \
        .dropna(subset=["id", "name", "state", "country", "brewery_type"])

    # Applying cleaning and normalization to the "main" columns # 
    df_clean = df_clean \
        .withColumn("state", lower(trim(normalize_ascii(col("state"))))) \
        .withColumn("country", lower(trim(normalize_ascii(col("country"))))) \
        .withColumn("brewery_type", lower(trim(col("brewery_type")))) \
        .withColumn("city", lower(trim(normalize_ascii(col("city"))))) \
        .withColumn("name", trim(normalize_ascii(col("name"))))
    
    # Concat the columns address in just one.
    df_clean = df_clean.withColumn("address", concat_ws(", ",
        "address_1", "address_2", "address_3")).drop("address_1", "address_2", "address_3")

    # Casting lat and lon in double, for use in a BI.
    df_clean = df_clean \
        .withColumn("latitude", col("latitude").cast(DoubleType())) \
        .withColumn("longitude", col("longitude").cast(DoubleType()))

    df_clean = df_clean.withColumn("active_status", ~col("brewery_type").contains("closed"))

    # Dropping the state province, website-url and phone columns#   
    df_final = df_clean.drop("state_province", "phone", "website_url")

    #print(df_final.select("country", "state").distinct().show(200))

    logging.info("Writing to Silver path with partitioning")
    
    df_final.select(
            "id",
            "name",
            "brewery_type",
            "active_status",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "latitude",
            "longitude"
        ) \
        .write \
        .mode("overwrite") \
        .partitionBy("country", "state") \
        .parquet(output_path)

    logging.info("Silver transformation completed successfully")
    logging.info(f"Total records of bronze raw data: {df.count()}, after silver layer: {df_final.count()}")

    spark.stop()

if __name__ == "__main__":
    main()
