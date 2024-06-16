from pyspark.sql import DataFrame
from pyspark.sql.functions import col, weekofyear, year

class DataTransformer:
    def __init__(self):
        pass

    def transform_data(self, taxi_data_df: DataFrame, payment_lookup_df: DataFrame, vendor_lookup_df: DataFrame):
        # Mapear os valores de payment_type
        taxi_data_df = taxi_data_df.join(payment_lookup_df, taxi_data_df.payment_type == payment_lookup_df.payment_type, 'left') \
                                   .drop(payment_lookup_df.payment_type)

        # 1. Qual vendor mais viajou de táxi em cada ano
        vendor_trips_per_year = taxi_data_df.groupBy('vendor_id', year('pickup_datetime').alias('Year')) \
                                            .count() \
                                            .withColumnRenamed('count', 'Trips')

        # 2. Desempate por maior percurso e ordem alfabética dos nomes
        vendor_trips_per_year = vendor_trips_per_year.join(taxi_data_df.groupBy('vendor_id', year('pickup_datetime').alias('Year')) \
                                                           .agg({'trip_distance': 'sum'}) \
                                                           .withColumnRenamed('sum(trip_distance)', 'TotalDistance'), 
                                                           on=['vendor_id', 'Year']) \
                                                     .orderBy(col('Trips').desc(), col('TotalDistance').desc(), col('vendor_id'))

        # 3. Semana de cada ano com mais viagens de táxi
        weekly_trips = taxi_data_df.groupBy(year('pickup_datetime').alias('Year'), weekofyear('pickup_datetime').alias('Week')) \
                                   .count() \
                                   .withColumnRenamed('count', 'Trips') \
                                   .orderBy(col('Trips').desc())

        return vendor_trips_per_year, weekly_trips
