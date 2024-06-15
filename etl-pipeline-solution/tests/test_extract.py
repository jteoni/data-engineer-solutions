import pytest
from pyspark.sql import SparkSession
from src.extract import read_taxi_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.some.config.option", "config-value") \
        .getOrCreate()

def test_read_taxi_data(spark):
    zip_file_path = "data/data-files.zip"
    df = read_taxi_data(spark, zip_file_path)
    
    assert df.count() > 0
    assert "vendor_id" in df.columns
    assert "pickup_datetime" in df.columns
    assert "dropoff_datetime" in df.columns
    assert "trip_distance" in df.columns
    assert "fare_amount" in df.columns

