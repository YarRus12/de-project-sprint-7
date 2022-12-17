import datetime as dt
import pyspark.sql.functions as F
from pyspark.sql.functions import radians
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col


def input_event_paths(base_url, events_base_path, date, depth):
    return [base_url + f'{events_base_path}' \
            + f"/date={dt.datetime.strptime(date, '%Y-%m-%d').date() - dt.timedelta(days=days)}"
            for days in range(int(depth))]


def data_of_research(base_url, events_base_path, date, depth, spark_session):
    message_paths = input_event_paths(date=date, depth=depth, base_url=base_url, events_base_path=events_base_path)
    data = spark_session.read \
        .option("basePath", f"{base_url}{events_base_path}") \
        .parquet(*message_paths)
    return data


def chosen_test_data(base_url, events_base_path, date, depth, spark_session):
    # Тестовая партиция за depth дней
    events_data = data_of_research(base_url, events_base_path, date=date, depth=depth, spark_session=spark_session) \
        .select('event', 'event_type', 'date', radians(col('lat')).alias('lat'), radians(col('lon')).alias('lon')) \
        .sample(fraction=0.01, seed=3000)
    return events_data


"""
def chosen_test_data(base_url, events_base_path, spark_session):    
    # Случайная выборка за весь период #очень долго работает
    events_messages = reactions = spark.read.parquet(base_url + events_base_path) \
        .select('event', 'event_type', 'date', radians(col('lat')).alias('lat'), radians(col('lon')).alias('lon')) \
        .sample(fraction=0.001, seed=3000)
    return events_data
"""


def create_test_partitions(base_url, events_base_path, spark):
    cities_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("lng", StringType(), True)
    ])

    cities = spark.read.option("delimiter", ";").option("header", "true").schema(cities_schema).csv(
        base_url + '/user/yarruss12/data/geo.csv') \
        .withColumn('city_long', radians(regexp_replace(F.col("lng"), ',', '.').cast(DoubleType()))) \
        .withColumn('city_lat', radians(regexp_replace(F.col("lat"), ',', '.').cast(DoubleType()))) \
        .select('id', 'city', 'city_lat', 'city_long')

    events_messages = chosen_test_data(base_url=base_url, events_base_path=events_base_path, date='2022-06-21',
                                       depth=60, spark_session=spark)
    result = events_messages.crossJoin(cities)
    # Тестовая выполка для будущих расчетов
    result.write.mode("overwrite").parquet(f"{base_url}/user/yarruss12/analytics/test")
