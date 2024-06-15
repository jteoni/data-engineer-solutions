from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_most_traveled_vendor(taxi_data):
    # Adiciona a coluna "year" com base na coluna "pickup_datetime"
    taxi_data = taxi_data.withColumn("year", F.year(F.col("pickup_datetime")))

    # Define a especificação da janela para calcular a distância total por ano e fornecedor
    window_spec = Window.partitionBy("year", "vendor_id").orderBy(F.desc(F.sum("trip_distance")))

    # Calcula a distância total de viagem por ano e fornecedor
    taxi_data = taxi_data.withColumn("total_distance", F.sum("trip_distance").over(window_spec))

    # Filtra para obter apenas a linha com a maior distância total por ano e fornecedor
    filtered_data = taxi_data.groupBy("year", "vendor_id") \
                             .agg(F.max("total_distance").alias("max_total_distance")) \
                             .join(taxi_data, ["year", "vendor_id", "total_distance"]) \
                             .select("year", "vendor_id", "max_total_distance", "total_distance")

    return filtered_data

def main():
    spark = SparkSession.builder.appName("NYC Taxi ETL").getOrCreate()

    # Carrega os dados de exemplo (substitua pelo seu carregamento real de dados)
    taxi_data = spark.read.json("data/data-nyctaxi-trips-*.json.gz")

    # Calcula a informação do vendor mais viajou por ano
    most_traveled_vendor = calculate_most_traveled_vendor(taxi_data)

    # Mostra o resultado ou salva conforme necessário
    most_traveled_vendor.show()

    spark.stop()

if __name__ == "__main__":
    main()
