from pyspark.sql import DataFrame

class DataLoader:
    def __init__(self):
        pass

    def save_data(self, df: DataFrame, output_path: str):
        df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
