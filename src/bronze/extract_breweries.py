import os
import requests
import boto3
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def main():
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    BUCKET_NAME = os.getenv("BUCKET_BRONZE")
    API_URL = os.getenv("API_URL")

    breweries = []
    page = 1

    try:
        while True:
            response = requests.get(API_URL, params={"page": page}, timeout=10)

            if response.status_code != 200:
                raise Exception(f"Error accessing API on page {page}: Status {response.status_code}")

            data = response.json()
            if not data:
                break

            breweries.extend(data)
            page += 1

        file_name = f"breweries_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.json"

        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(breweries).encode("utf-8"),
            ContentType="application/json",
        )

        logging.info(f"File saved successfully: {BUCKET_NAME}/{file_name} (total records: {len(breweries)})")

    except Exception as e:
        logging.error(f"Fail on execution: {e}")
        raise 

if __name__ == "__main__":
    main()
