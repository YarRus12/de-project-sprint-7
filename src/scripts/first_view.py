from pyspark.sql.functions import col, expr, desc, row_number, collect_list, date_format, udf, date_sub, \
    min, lag, size, max, from_utc_timestamp, lead
from pyspark.sql.window import Window
import functional


def message_filter(data):
    return data.where("event.message_from is not Null")


def homecity_finder(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))).orderBy('user_id') \
        .withColumn('privious_city', lag('city', 1).over(Window.partitionBy("user_id").orderBy("group_id"))) \
        .where('city != privious_city or privious_city is null')\
        .withColumn('privious_date', lag('date', 1).over(Window.partitionBy('user_id').orderBy('group_id')))\
        .withColumn('lived_here_days', expr("case when privious_date is not null then datediff(date, privious_date)\
                                      else datediff(current_date(), date) end"))\
        .where('lived_here_days>26')\
        .withColumn('rank_days', row_number().over(Window.partitionBy("user_id").orderBy(desc("date")))) \
        .where('rank_days=1').select('user_id', col('city').alias('home_city'))


def actual_home_finder(messages):
    timezone = udf(functional.time_zone)
    return messages \
        .where('event.message_ts is not NULL') \
        .select(col('event.message_from').alias("user_id"), 'city', 'date', 'event.message_ts') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id").orderBy(desc("date")))) \
        .where("rank = 1") \
        .withColumn('utc', timezone(col('city'))) \
        .withColumn('local_datetime', from_utc_timestamp(col('message_ts'), col('utc'))) \
        .withColumn('local_time', date_format(col('local_datetime'), 'HH:mm:ss')) \
        .select('user_id', col('city').alias('act_city'), 'local_time')


def travel_cities(messages):
    return messages \
        .select(col('event.message_from').alias("user_id"), 'city', 'date') \
        .withColumn('rank', row_number().over(Window.partitionBy("user_id", "city").orderBy(desc("date")))) \
        .withColumn('group_id', date_sub(col('date'), col('rank'))) \
        .withColumn('lag', lag('city', 1).over(Window.partitionBy("user_id").orderBy(desc("group_id")))) \
        .withColumn('all_city', expr("case when city != lag or lag is null then city end")) \
        .where('all_city is not NULL').groupBy("user_id").agg(collect_list('all_city')) \
        .select('user_id', col('collect_list(all_city)').alias('travel_array'),
                size(col('collect_list(all_city)')).alias('travel_count'))


def first_view_maker(data):
    messages = message_filter(data)
    home_city = homecity_finder(messages)
    actual_city = actual_home_finder(messages)
    visited_cities = travel_cities(messages)
    return home_city \
        .join(actual_city, 'user_id', 'inner') \
        .join(visited_cities, 'user_id', 'inner')
