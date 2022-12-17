from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import download_data
import functional
import first_view
import second_view
import third_view

base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_base_path = '/user/master/data/geo/events'

spark = SparkSession.builder \
    .master("local") \
    .appName("Learning DataFrames") \
    .getOrCreate()

# ######## Подготовка данных ######## #
# Закомментировано, так как данные уже собраны
# download_data.create_test_partitions(base_url, events_base_path, spark)

events = spark.read \
    .parquet(f"{base_url}/user/yarruss12/analytics/test")

city_events = functional.distance(data=events, first_lat='lat', second_lat='city_lat', first_lon='lon', second_lon='city_long')\
            .select('event', 'event_type', 'id', 'city', 'date','lat', 'lon', 'distanse') \
            .withColumn("row_number", row_number().over(Window.partitionBy("event").orderBy("distanse"))) \
            .where("row_number=1")

# ######## Реализация шага 2 ######## #
first_task = first_view.first_view_maker(city_events)

# ######## Реализация шага 3 ######## #
second_task = second_view.second_view_maker(city_events)

# ######## Реализация шага 4 ######## #
third_task = third_view.third_view_maker(city_events)

