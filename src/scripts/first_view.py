from pyspark.sql.functions import col, expr, desc, row_number, collect_list, sum, date_sub
from pyspark.sql.window import Window


def message_filter(data):
    return data.where("event.message_from is not Null")


def homecity_finder(messages):
    # с непрерывным рядом значений есть проблема, нужно видимо сделать udf
    # поэтому для начала выбраны пользователи и города, из которых указанный пользователь чаще всего отправлял сообщения
    # за последние 27 дней
    """
    return messages \
        .withColumn('max_date', expr(""date_add(date,-27) as inc_date"")) \
        .where(col('date') >= col('max_date')) \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .groupBy("user_id", 'city').count() \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
        .where("rank = 1") \
        .select('user_id', col('city').alias('home_city'))
    """
    # Правильный подход
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .withColumn('days_num', row_number().over(Window.partitionBy("user_id", "city", "group_id") \
        .orderBy(desc("date")))).where('days_num > 26').orderBy(desc("date"))


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
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .groupBy("user_id", 'group_id').count() \
        .show()
# Надо проверить


def travel_cities_list(messages):
    return messages \
        .where("event.message_from is not Null") \
        .select(col('event.message_from').alias("user_id"), 'city') \
        .groupBy('user_id').agg(collect_list("city")) \
        .select('user_id', col('collect_list(city)').alias('travel_array'))
# Надо переработать


def first_view_maker(data):
    """Время события (TIME_UTC) необходимо "добыть".
    Например, для сообщений это может быть поле message_ts в структуре event, для других событий - datetime.
    Можно заранее определить одно поле, которое будет собираться из указанных в зависимости от типа события.
Таймзону можно определить исходя из города, в котором произошло событие, например, как Australia/название_города.
Только обрати внимание, что не для всех городов есть стандартная таймзона, поэтому рекомендую разметить таймзоны
в справочнике городов (самый надёжный способ) или искать ближайший среди тех, для которых есть стандартная таймзона
(но на самом деле они могут оказаться в разных таймзонах). Но твоё решение может быть более элегантным."""
    messages = message_filter(data)
    home_city = homecity_finder(messages)
    actual_city = actual_home_finder(messages)
    travel_count = travel_counter(messages)
    travel_cities = travel_cities_list(messages)
    return home_city \
        .join(actual_city, 'user_id', 'inner') \
        .join(travel_count, 'user_id', 'inner') \
        .join(travel_cities, 'user_id', 'inner')
