from pyspark.sql.functions import col, expr, desc, row_number, collect_list, sum
from pyspark.sql.window import Window


def message_filter(data):
    return data.where("event.message_from is not Null")


def homecity_finder(messages):
    # с непрерывным рядом значений есть проблема, нужно видимо сделать udf
    # поэтому для начала выбраны пользователи и города, из которых указанный пользователь чаще всего отправлял сообщения
    # за последние 27 дней
    return messages \
        .withColumn('max_date', expr("""date_add(date,-27) as inc_date""")) \
        .where(col('date') >= col('max_date')) \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .groupBy("user_id", 'city').count() \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
        .where("rank = 1") \
        .select('user_id', col('city').alias('home_city'))


def actual_home_finder(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("date")))) \
        .where("rank = 1") \
        .select('user_id', col('city').alias('act_city'))


def travel_counter(messages):
    return messages \
        .where("event.message_from is not Null") \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .groupBy("user_id", 'city', 'date').count() \
        .groupBy("user_id").agg(sum('count').alias('travel_count'))


def travel_cities_list(messages):
    return messages \
        .where("event.message_from is not Null") \
        .select(col('event.message_from').alias("user_id"), 'city') \
        .groupBy('user_id').agg(collect_list("city")) \
        .select('user_id', col('collect_list(city)').alias('travel_array'))


def first_view(data):
    # О каких полях идет речь в подсказе? в event нет полей TIME_UTC и timezone????
    #    .withColumn('localtime', F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))
    messages = message_filter(data)
    home_city = homecity_finder(messages)
    actual_city = actual_home_finder(messages)
    travel_count = travel_counter(messages)
    travel_cities = travel_cities_list(messages)
    return home_city \
        .join(actual_city, 'user_id', 'inner') \
        .join(travel_count, 'user_id', 'inner') \
        .join(travel_cities, 'user_id', 'inner')

