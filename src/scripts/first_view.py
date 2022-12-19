from pyspark.sql.functions import col, expr, desc, row_number, collect_list, sum, date_sub, to_date, date_add, lag
from pyspark.sql.window import Window


# user_id 21395

def message_filter(data):
    return data.where("event.message_from is not Null")


def homecity_finder(messages):
    # с непрерывным рядом значений есть проблема, нужно видимо сделать udf
    # поэтому для начала выбраны пользователи и города, из которых указанный пользователь чаще всего отправлял сообщения
    # за последние 27 дней

    # Правильный подход только days_num нужно заменить на 26 и протестировать на большой выборке
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))).orderBy('user_id') \
        .withColumn('days_num', row_number().over(Window.partitionBy("user_id", "city", "group_id") \
                                                  .orderBy(desc("date")))).where('days_num > 2')


def actual_home_finder(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("date")))) \
        .where("rank = 1") \
        .select('user_id', col('city').alias('act_city'))


def travel_counter(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .groupBy("user_id", 'group_id').count() \
        .select('user_id', col('count').alias('travel_count'))


def travel_cities_list(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_add(to_date(col("date"), "yyyy-MM-dd"), -col('rank'))) \
        .groupBy("user_id", 'group_id').agg(collect_list('city'))


# Надо переработать
def travel_cities_list(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .groupBy("user_id", 'group_id').agg(collect_list('city'))


def travel_cities_list_log_test(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .withColumn('lag', lag('group_id', 1).over(Window.partitionBy("user_id").orderBy(desc("group_id"))))


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
