from pyspark.sql import SparkSession
import gzip
import json
import os

class DataExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_json_files(self, folder_path: str):
        folder_path = os.path.abspath(folder_path)
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        df_list = []
        for json_file in json_files:
            with open(os.path.join(folder_path, json_file), 'r') as f:
                lines = f.readlines()
                json_data = [json.loads(line) for line in lines]
                with gzip.open(os.path.join(folder_path, f"{json_file}.gz"), 'wt', encoding='utf-8') as gz_file:
                    for record in json_data:
                        gz_file.write(json.dumps(record) + '\n')
                temp_df = self.spark.read.json(os.path.join(folder_path, f"{json_file}.gz"))
                df_list.append(temp_df)
        return df_list

    def read_csv_file(self, file_path: str):
        file_path = os.path.abspath(file_path)
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def union_dataframes(self, df_list: list):
        return df_list[0].unionByName(*df_list[1:])

    def read_data(self, json_folder_path: str, payment_lookup_path: str, vendor_lookup_path: str):
        json_dfs = self.read_json_files(json_folder_path)
        taxi_data_df = self.union_dataframes(json_dfs)
        payment_lookup_df = self.read_csv_file(payment_lookup_path)
        vendor_lookup_df = self.read_csv_file(vendor_lookup_path)
        return taxi_data_df, payment_lookup_df, vendor_lookup_df
