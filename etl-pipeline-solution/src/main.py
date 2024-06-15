from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_most_traveled_vendor(taxi_data):
    window_spec = Window.partitionBy("year", "vendor_id").orderBy(F.desc("total_distance"))

    taxi_data = taxi_data.withColumn("total_distance", F.sum("trip_distance").over(window_spec))

    return taxi_data

def main():
    spark = SparkSession.builder.appName("NYC Taxi ETL").getOrCreate()

    # Carregar dados de exemplo (substitua pelo seu carregamento real de dados)
    taxi_data = spark.read.json("data/data-nyctaxi-trips-*.json.gz")

    # Calcular a distância total por ano e por fornecedor
    taxi_data = calculate_most_traveled_vendor(taxi_data)

    # Mostrar o resultado ou salvar conforme necessário
    taxi_data.show()

    spark.stop()

if __name__ == "__main__":
    main()
