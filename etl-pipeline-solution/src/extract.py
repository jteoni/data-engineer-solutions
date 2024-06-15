from pyspark.sql import SparkSession
from functools import reduce

def read_taxi_data(file_paths):
    """
    Read taxi data from multiple CSV and JSON files into a single PySpark DataFrame.
    Args:
    - file_paths (list): List of file paths to CSV and JSON files.
    Returns:
    - combined_df (DataFrame): Combined PySpark DataFrame containing all data.
    """
    spark = SparkSession.builder.appName("TaxiETL").getOrCreate()
    
    # Function to read JSON and select required fields
    def read_json_and_select(file_path):
        """
        Reads JSON file and selects only required fields for ETL.
        Args:
        - file_path (str): Path to JSON file.
        Returns:
        - selected_df (DataFrame): DataFrame with selected fields.
        """
        # Read JSON file into DataFrame
        df = spark.read.json(file_path)
        
        # Select only the necessary fields for ETL
        selected_df = df.select(
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "fare_amount"
        )
        
        return selected_df
    
    # List to store all DataFrames
    dfs = []
    
    # Loop through each file path and read the file into a DataFrame
    for file_path in file_paths:
        if file_path.endswith('.csv'):
            # Read CSV files
            dfs.append(spark.read.csv(file_path, header=True, inferSchema=True))
        elif file_path.endswith('.json'):
            # Read JSON files and select required fields
            dfs.append(read_json_and_select(file_path))
        else:
            # Raise error for unsupported file format
            raise ValueError(f"Unsupported file format for file: {file_path}")
    
    # Combine all DataFrames into a single DataFrame using union
    combined_df = reduce(lambda df1, df2: df1.union(df2), dfs)
    
    return combined_df
