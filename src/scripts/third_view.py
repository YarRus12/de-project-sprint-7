from pyspark.sql.functions import current_date, col, udf, from_utc_timestamp, date_format
from src.scripts import functional


def all_subscribers_area(data):
    user_sub = data \
        .where("event_type ='subscription'") \
        .select(col('event.user').alias('user_left'), 'event.subscription_channel', col('lat').alias('user_lat'),
                col('lon').alias('user_lon'), 'id', 'city')
    user_sub2 = user_sub.select(col('user_left').alias('user_right'), 'subscription_channel',
                                col('user_lat').alias('contact_lat'), col('user_lon').alias('contact_lon'),
                                col('id').alias('r_u_id'), col('city').alias('c_city'))

    all_subsribers = user_sub.join(user_sub2, 'subscription_channel', 'inner').where('user_left < user_right').distinct()
    all_subsribers_near = functional.distance(data=all_subsribers, first_lat='user_lat', second_lat='contact_lat',
                                              first_lon='user_lon', second_lon='contact_lon').where('distanse is not null').where(
        'distanse < 1.0').select('user_left', 'user_right', 'id', 'city')
    return all_subsribers_near


def non_chatting_users(data):
    # сперва мы выбираем всех отправителей и даем им имя user_left. Всех получаетелей мы именуем user_right
    out_user_contacts = data \
        .select(col('event.message_from').alias('user_left'), col('event.message_to').alias('user_right')) \
        .where("event_type ='message'")
    # затем выбираем всех ПОЛУЧАТЕЛЕЙ и даем им все тоже имя user_left. Отправителей же именуем user_right
    receive_user_contacts = data \
        .select(col('event.message_to').alias('user_left'), col('event.message_from').alias('user_right')) \
        .where("event_type ='message'")
    return out_user_contacts.union(receive_user_contacts).distinct()
    # Ранее out_user_contacts.join(receive_user_contacts, 'user_left', 'leftanti')


def third_view_maker(data):
    timezone = udf(functional.time_zone)
    subsribers = all_subscribers_area(data)
    no_message_users = non_chatting_users(data)
    return subsribers.join(no_message_users, ['user_left', 'user_right'], 'leftanti') \
        .withColumn("processed_dttm", current_date())\
        .withColumn('utc', timezone(col('city'))) \
        .withColumn('local_datetime', from_utc_timestamp(col('processed_dttm'), col('utc'))) \
        .withColumn('local_time', date_format(col('local_datetime'), 'HH:mm:ss')) \
        .select('user_left', 'user_right', 'processed_dttm', 'id', 'local_time')
    # Ранее left
