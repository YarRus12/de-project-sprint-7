from pyspark.sql import SparkSession
from pyspark.sql.functions import sin, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import sin, cos, asin, sinh, sqrt
import pyspark.sql.functions as F
import datetime
from pyspark.sql.functions import udf, max, expr, desc, row_number, collect_set, sum
from pyspark.sql.window import Window

base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_base_path = '/user/master/data/geo/events'

spark = SparkSession.builder \
    .master("local") \
    .appName("Learning DataFrames") \
    .getOrCreate()

# открываем тестовую выборку
events = spark.read \
    .parquet(f"{base_url}/user/yarruss12/analytics/test")

# Определяем расстояние каждого события до городов и выбираем город с минимальным расстоянием
city_events = events \
    .withColumn('1', F.pow(F.sin((F.col('city_lat') - F.col('lat')) / F.lit(2)), 2)) \
    .withColumn('2', cos(F.col('lon')) * cos(F.col('city_long'))) \
    .withColumn('3', F.pow(F.sin((F.col('city_lat') - F.col('lat')) / F.lit(2)), 2)) \
    .withColumn('4', sqrt(F.col('1') + (F.col('2') * F.col('3')))) \
    .withColumn('distanse', 2 * 6371 * (asin((F.col('4'))))) \
    .select('event', 'event_type', 'city', 'date', 'distanse') \
    .withColumn("row_number", row_number().over(Window.partitionBy("event").orderBy("distanse"))) \
    .where("row_number=1") \
    .select('event', 'event_type', 'date', 'city', 'lat', 'lon')

# Берем сообщения
messages = city_events.where("event.message_from is not Null")

# с непрерывным рядом значений есть проблема, нужно видимо сделать udf
# поэтому для начала выбраны пользователи и города, из которых указанный пользователь чаще всего отправлял сообщения за последние 27 дней

homecity = messages \
    .withColumn('max_date', expr("""date_add(date,-27) as inc_date""")) \
    .where(F.col('date') >= F.col('max_date')) \
    .select(F.col('event.message_from').alias("user_id"), 'city', 'date') \
    .groupBy("user_id", 'city').count() \
    .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
    .where("rank = 1") \
    .select('user_id', F.col('city').alias('home_city'))

act_city = messages \
    .select(F.col('event.message_from').alias("user_id"), 'city', 'date') \
    .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("date")))) \
    .where("rank = 1") \
    .select('user_id', F.col('city').alias('act_city'))

travel_count = messages \
    .where("event.message_from is not Null") \
    .select(F.col('event.message_from').alias("user_id"), 'city', 'date') \
    .groupBy("user_id", 'city', 'date').count() \
    .groupBy("user_id").agg(sum('count').alias('travel_count'))

travel_cities = messages \
    .where("event.message_from is not Null") \
    .select(F.col('event.message_from').alias("user_id"), 'city') \
    .groupBy('user_id').agg(F.collect_list("city")) \
    .select('user_id', F.col('collect_list(city)').alias('travel_array'))

# О каких полях идет речь в подсказе? в event нет полей TIME_UTC и timezone????
#    .withColumn('localtime', F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))

first_view = homecity \
    .join(act_city, 'user_id', 'inner') \
    .join(travel_count, 'user_id', 'inner') \
    .join(travel_cities, 'user_id', 'inner')

first_view.show(500, False)

