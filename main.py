from pyspark.sql import SparkSession
import download_data
import functional
import first_view
import second_view



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

city_events = functional.distance(data=events, first_lat='lon', second_lat='city_long', first_lon='lat',
                                  second_lon='city_lat')


# ######## Реализация шага 2 ######## #
first_task = first_view.first_view_maker(city_events)

# ######## Реализация шага 3 ######## #
second_task = second_view.second_view_maker(city_events)


# all_subsribers_close = user_sub.join(user_sub2, 'subscription_channel', 'full').distinct()\
# result .where('distanse is not null').where('distanse < 50.0').where('user != user_right')
