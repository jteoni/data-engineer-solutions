import os
from pyspark.sql import SparkSession
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader

def main():
    # Inicializa a sessão Spark
    spark = SparkSession.builder.appName("NYC Taxi ETL").getOrCreate()

    # Configura o nível de log do Spark
    spark.sparkContext.setLogLevel("ERROR")

    # Cria instâncias das classes
    extractor = DataExtractor(spark)
    transformer = DataTransformer()
    loader = DataLoader()

    # Caminhos dos arquivos
    base_path = os.path.dirname(os.path.abspath(__file__))
    json_folder_path = os.path.join(base_path, "../data/")
    payment_lookup_path = os.path.join(base_path, "../data/data-payment_lookup.csv")
    vendor_lookup_path = os.path.join(base_path, "../data/data-vendor_lookup.csv")
    output_path = os.path.join(base_path, "../output/etl_output.csv")

    # ETL Pipeline
    taxi_data_df, payment_lookup_df, vendor_lookup_df = extractor.read_data(json_folder_path, payment_lookup_path, vendor_lookup_path)
    vendor_trips_per_year, weekly_trips = transformer.transform_data(taxi_data_df, payment_lookup_df, vendor_lookup_df)
    
    # Salva os resultados
    loader.save_data(vendor_trips_per_year, output_path)

    # Finaliza a sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()
