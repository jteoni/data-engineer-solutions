import pytest
from pyspark.sql import SparkSession
from src.load import write_output

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.some.config.option", "config-value") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_data(spark):
    data = [("CMT", 2009, 100.0), ("VTS", 2010, 150.0)]
    columns = ["vendor_id", "year", "total_distance"]
    return spark.createDataFrame(data, columns)

def test_write_output(sample_data, tmpdir):
    output_path = str(tmpdir.join("test_output.csv"))
    write_output(sample_data, output_path)
    
    result_df = spark.read.option("header", "true").csv(output_path)
    assert result_df.count() == 2  # Assuming two rows are written
    assert result_df.columns == ["vendor_id", "year", "total_distance"]
