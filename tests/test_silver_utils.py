import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from src.silver.transform_breweries import normalize_ascii

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test_silver_utils") \
        .master("local[*]") \
        .getOrCreate()

def test_state_normalization_cases(spark):
    test_cases = [
        {"input": "São Paulo", "expected": "Sao Paulo"},
        {"input": "Kärnten", "expected": "Karnten"},
        {"input": "Québec", "expected": "Quebec"},
        {"input": "München", "expected": "Munchen"},
        {"input": "Île-de-France", "expected": "Ile-de-France"},
        {"input": "", "expected": ""},
        {"input": None, "expected": None},
    ]

    for case in test_cases:
        schema = StructType([StructField("state", StringType(), True)])
        df = spark.createDataFrame([{"state": case["input"]}], schema)
        df_result = df.withColumn("state", normalize_ascii(col("state")))
        result = df_result.collect()[0]["state"]
        assert result == case["expected"], f"Failed for input: {case['input']}"
