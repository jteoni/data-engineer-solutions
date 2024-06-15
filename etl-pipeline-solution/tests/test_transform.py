import pytest
from pyspark.sql import SparkSession
from src.transform import calculate_most_traveled_vendor, calculate_busiest_week

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.some.config.option", "config-value") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_data(spark):
    # Create a sample DataFrame for testing transformations
    data = [
        ("CMT", "2009-01-01 10:00:00", "2009-01-01 10:10:00", 1.5, 10.0),
        ("CMT", "2009-01-02 11:00:00", "2009-01-02 11:20:00", 2.0, 15.0),
        ("VTS", "2009-01-01 12:00:00", "2009-01-01 12:30:00", 3.0, 20.0),
    ]
    columns = ["vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount"]
    return spark.createDataFrame(data, columns)

def test_calculate_most_traveled_vendor(sample_data):
    result_df = calculate_most_traveled_vendor(sample_data)
    
    assert result_df.count() == 1
    assert result_df.first().vendor_id == "CMT"

def test_calculate_busiest_week(sample_data):
    result_df = calculate_busiest_week(sample_data)
    
    assert result_df.count() == 2  # Assuming two different weeks in sample_data
