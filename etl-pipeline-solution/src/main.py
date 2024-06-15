from extract import read_taxi_data
from transform import transform_data
from load import write_output

def main():
    # List of file paths (CSV and JSON)
    file_paths = [
        "data/data-payment_lookup.csv",
        "data/tdata-vendor_lookup.csv",
        "data/data-nyctaxi-trips-2009.json",
        "data/data-nyctaxi-trips-2010.json",
        "data/data-nyctaxi-trips-2011.json",
        "data/data-nyctaxi-trips-2012.json"
    ]
    
    # Output file paths
    output_path_vendor = "output/most_traveled_vendor.csv"
    output_path_week = "output/busiest_week.csv"
    
    # Read data
    taxi_data = read_taxi_data(file_paths)
    
    # Transform data
    most_traveled_vendor, busiest_week = transform_data(taxi_data)
    
    # Write output
    write_output(most_traveled_vendor, output_path_vendor)
    write_output(busiest_week, output_path_week)

if __name__ == "__main__":
    main()

