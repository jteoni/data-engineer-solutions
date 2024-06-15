from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_most_traveled_vendor(taxi_data):
    """
    Calculate the most traveled vendor per year.
    
    Parameters:
    - taxi_data: Spark DataFrame containing taxi trip data
    
    Returns:
    - Spark DataFrame with columns: year, vendor_id, total_distance
    """
    # Assuming taxi_data has columns: year, vendor_id, trip_distance
    window_spec = Window.partitionBy("year").orderBy(F.desc("trip_distance"), "vendor_id")
    most_traveled = taxi_data.withColumn("total_distance", F.sum("trip_distance").over(window_spec)) \
                            .filter(F.rank().over(window_spec) == 1) \
                            .select("year", "vendor_id", "total_distance") \
                            .orderBy("year", "vendor_id")
    
    return most_traveled

def calculate_busiest_week(taxi_data):
    """
    Calculate the busiest week for taxi trips.
    
    Parameters:
    - taxi_data: Spark DataFrame containing taxi trip data
    
    Returns:
    - Spark DataFrame with columns: year, week, total_trips
    """
    # Assuming taxi_data has columns: year, week, trip_count
    busiest_week = taxi_data.groupBy("year", "week") \
                           .agg(F.sum("trip_count").alias("total_trips")) \
                           .orderBy("year", F.desc("total_trips"))
    
    return busiest_week
