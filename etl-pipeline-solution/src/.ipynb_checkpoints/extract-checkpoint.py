from pyspark.sql import DataFrame, SparkSession

def read_taxi_data(spark: SparkSession, json_files: list) -> DataFrame:
    """
    Reads taxi data from JSON files.

    Parameters:
    - spark: SparkSession object
    - json_files: List of JSON file paths

    Returns:
    - DataFrame containing the concatenated data from all JSON files
    """
    # Assuming json_files is a list of paths to JSON files
    df_list = []
    for file_path in json_files:
        df = spark.read.json(file_path)
        df_list.append(df)
    
    # Union all DataFrames
    taxi_data = df_list[0] if len(df_list) > 0 else None
    for i in range(1, len(df_list)):
        taxi_data = taxi_data.union(df_list[i])

    return taxi_data
