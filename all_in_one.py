from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, desc
from pyspark.sql.window import Window
import datetime as dt
import pyspark.sql.functions as F
import sys
from pyspark.sql.functions import radians
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col


base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_base_path = '/user/master/data/geo/events'

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

def input_event_paths(date, depth):
    return [
    base_url +\
    f'{events_base_path}' +\
    f"/date={dt.datetime.strptime(date, '%Y-%m-%d').date() - dt.timedelta(days=days)}"
    for days in range(int(depth))]
def contacts(date, depth, spark):
    message_paths = input_event_paths(date=date, depth=depth)
    events_messages = spark.read\
                .option("basePath", f"{base_url}{events_base_path}")\
                .parquet(*message_paths)
    return events_messages

#Тестовая партиция - 30 дней
events_messages = contacts('2022-06-21', 30, spark)\
    .select('event', 'event_type', 'date', radians(col('lat')).alias('lon'), radians(col('lon')).alias('lat'))\
    .sample(fraction=0.01, seed=3000)

"""
#Случайная выборка за весь период
events_messages = reactions = spark.read.parquet(base_url+events_base_path)\
            .select('event', 'event_type', 'date', radians(col('lat')).alias('lon'), radians(col('lon')).alias('lat'))
            .sample(fraction=0.001, seed=3000)

"""

citiesSchema = StructType([
    StructField("id", IntegerType(), True),        
    StructField("city", StringType(), True),        
    StructField("lat", StringType(), True),
    StructField("lng", StringType(), True)
])


cities = spark.read.option("delimiter", ";").option("header", "true").schema(citiesSchema).csv(base_url+'/user/yarruss12/data/geo.csv')\
    .withColumn('city_long', radians(regexp_replace(F.col("lat"), ',', '.').cast(DoubleType())))\
    .withColumn('city_lat', radians(regexp_replace(F.col("lng"), ',', '.').cast(DoubleType())))\
    .select('id','city','city_lat','city_long')
    
result = events_messages.crossJoin(cities)
# Тестовая выполка для будущих расчетов
result.write.parquet(f"{base_url}/user/yarruss12/analytics/test")


################################################################################
# Шаг 2 
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
    .withColumn('1', F.pow(F.sin((F.col('city_long') - F.col('lon')) / F.lit(2)), 2)) \
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



###############
# Шаг 3

from pyspark.sql.functions import radians
from pyspark.sql.functions import regexp_replace, col


citiesSchema = StructType([
    StructField("id", IntegerType(), True),        
    StructField("city", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lng", StringType(), True)
])


"""
Делаем выборку данных о городах (это повтор. В перспективе переделать тестовую партицию выше, который передадим сюда. для избежания повтора кода)
"""

cities = spark.read.option("delimiter", ";").option("header", "true").schema(citiesSchema).csv(base_url+'/user/yarruss12/data/geo.csv')\
    .withColumn('city_long', radians(regexp_replace(F.col("lat"), ',', '.').cast(DoubleType())))\
    .withColumn('city_lat', radians(regexp_replace(F.col("lng"), ',', '.').cast(DoubleType())))\
    .select('id','city','city_lat','city_long')


"""
Собираем данные по месяцам и неделям. это можно переделать в функции, если получится. Одни и теже операции. Две выборки
"""


messages_month = city_events\
    .join(cities, 'city')\
    .where("event_type = 'message'")\
    .select(F.month('date'), F.col('id').alias('zone_id'))\
    .groupBy('month(date)', 'zone_id').agg(F.count('*').alias('month_message'))\
    .select(F.col('month(date)').alias('month'), 'month_message', 'zone_id')
    
reaction_month = city_events\
    .join(cities, 'city')\
    .where("event_type = 'reaction'")\
    .select(F.month('date'), F.col('id').alias('zone_id'))\
    .groupBy('month(date)', 'zone_id').agg(F.count('*').alias('month_reaction'))\
    .select(F.col('month(date)').alias('month'), 'month_reaction', 'zone_id')

subscription_month = city_events\
    .join(cities, 'city')\
    .where("event_type = 'subscription'")\
    .select(F.month('date'), F.col('id').alias('zone_id'))\
    .groupBy('month(date)', 'zone_id').agg(F.count('*').alias('month_subscription'))\
    .select(F.col('month(date)').alias('month'), 'month_subscription', 'zone_id')

month = messages_month\
    .join(reaction_month, ['month','zone_id'], 'inner')\
    .join(subscription_month, ['month','zone_id'], 'inner')

month.show()


messages_week = city_events\
    .join(cities, 'city')\
    .where("event_type = 'message'")\
    .select(F.weekofyear('date'), F.col('id').alias('zone_id'))\
    .groupBy('weekofyear(date)', 'zone_id').agg(F.count('*').alias('week_message'))\
    .select(F.col('weekofyear(date)').alias('week'), 'week_message','zone_id')

reaction_week = city_events\
    .join(cities, 'city')\
    .where("event_type = 'reaction'")\
    .select(F.weekofyear('date'), F.col('id').alias('zone_id'))\
    .groupBy('weekofyear(date)', 'zone_id').agg(F.count('*').alias('week_reaction'))\
    .select(F.col('weekofyear(date)').alias('week'), 'week_reaction', 'zone_id')

subscription_week = city_events\
    .join(cities, 'city')\
    .where("event_type = 'subscription'")\
    .select(F.weekofyear('date'), F.col('id').alias('zone_id'))\
    .groupBy('weekofyear(date)', 'zone_id').agg(F.count('*').alias('week_subscription'))\
    .select(F.col('weekofyear(date)').alias('week'), 'week_subscription', 'zone_id')

week = messages_week\
    .join(reaction_week, ['week','zone_id'], 'full')\
    .join(subscription_week, ['week','zone_id'], 'full')

week.show()


# Почему первые сообщение это регистрация и нужно ли выискивать первое сообщение из всего пула данных, а не только из выборки?!
# что значит: Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя. Это про что?


""" Базу тоже можно оптимизировать. Напимер сперва база и добавление чрезе функцию полей витрины"""
base = city_events\
    .join(cities, 'city')\
    .select(F.month('date').alias('month'), F.weekofyear('date').alias('week'), F.col('id').alias('zone_id'))

base.show()


second_view = base\
    .join(month, ['month', 'zone_id'], 'inner')\
    .join(week, ['week', 'zone_id'], 'inner')\
    .select('month','week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription',\
            'month_message', 'month_reaction', 'week_subscription').distinct()
    
second_view.show(500)




###############
# Шаг 4

from pyspark.sql import SparkSession
from pyspark.sql.functions import sin, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import sin,cos,asin,sinh,sqrt
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


#открываем тестовую выборку
events = spark.read\
                .parquet(f"{base_url}/user/yarruss12/analytics/test")    


#Определяем расстояние каждого события до городов и выбираем город с минимальным расстоянием
city_events = events\
    .withColumn('1', F.pow(F.sin((F.col('city_lat') - F.col('lat')) /F.lit(2)), 2))\
    .withColumn('2', cos(F.col('city_lat'))*cos(F.col('lat')))\
    .withColumn('3', F.pow(F.sin((F.col('city_long') - F.col('lon'))/F.lit(2)), 2))\
    .withColumn('4', sqrt(F.col('1')+(F.col('2')*F.col('3'))))\
    .withColumn('distanse', 2*6371*(asin((F.col('4')))))\
    .select('event', 'event_type', 'city','date','distanse','lat', 'lon')\
    .withColumn("row_number",row_number().over(Window.partitionBy("event").orderBy("distanse")))\
    .where("row_number=1")\
    .select('event', 'event_type', 'date', 'city', 'lat', 'lon')

# Все подписки всех пользователей и их координаты
user_sub = city_events\
    .where("event_type ='subscription'")\
    .select('event.user', 'event.subscription_channel',F.col('lat').alias('user_lat'), F.col('lon').alias('user_lon'), 'city')
user_sub2 = user_sub.select(F.col('user').alias('contact'), 'subscription_channel',F.col('user_lat').alias('contact_lat'), F.col('user_lon').alias('contact_lon'))
# Все уникальные пары пользователей с одинаковыми подписками и
all_subsribers_close = user_sub.join(user_sub2, 'subscription_channel', 'full').distinct()\
                .withColumn('1', F.pow(F.sin((F.col('contact_lat') - F.col('user_lat')) /F.lit(2)), 2))\
                .withColumn('2', cos(F.col('contact_lat'))*cos(F.col('user_lat')))\
                .withColumn('3', F.pow(F.sin((F.col('contact_lon') - F.col('user_lon'))/F.lit(2)), 2))\
                .withColumn('4', sqrt(F.col('1')+(F.col('2')*F.col('3'))))\
                .withColumn('distanse', 2*6371*(asin((F.col('4')))))\
                .select('user', F.col('contact').alias('user_right'), 'distanse', 'city')\
                .where('distanse is not null').where('distanse < 50.0').where('user != user_right')

    
all_subsribers_close.show()

# Все сообщения, их авторы и получатели
out_user_contacts = city_events\
    .select(F.col('event.message_from').alias('user'), F.col('event.message_to').alias('user_right'))\
    .where("event_type ='message'")

receive_user_contacts = city_events\
    .select(F.col('event.message_to').alias('user'), F.col('event.message_from').alias('user_right'))\
    .where("event_type ='message'")

# Пары пользователелей, которые никогда не общались друг с другом
non_chatting_users = out_user_contacts.join(receive_user_contacts, 'user', 'leftanti')

non_chatting_users.show(1)

third_view = all_subsribers_close.join(non_chatting_users, ['user', 'user_right'], 'left')
third_view.show()

#user_left — первый пользователь;
#user_right — второй пользователь;
#processed_dttm — дата расчёта витрины;
#zone_id — идентификатор зоны (города);
#local_time — локальное время.


