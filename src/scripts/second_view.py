from pyspark.sql.functions import col, count, trunc, date_trunc


def month_data(data, old_registration):
    messages_month_registration = data \
        .where("event_type = 'message'") \
        .select('user_id', 'zone_id', 'date') \
        .join(old_registration, 'user_id', 'inner') \
        .withColumn('month', trunc(col('date'), "Month")) \
        .groupBy('month', 'zone_id').agg(count('*').alias('month_user')) \
        .select('month', 'month_user', 'zone_id')

    messages_month = data \
        .where("event_type = 'message'") \
        .withColumn('month', trunc(col('date'), "Month")) \
        .groupBy('month', 'zone_id').agg(count('*').alias('month_message')) \
        .select('month', 'month_message', 'zone_id')

    reaction_month = data \
        .where("event_type = 'reaction'") \
        .withColumn('month', trunc(col('date'), "Month")) \
        .groupBy('month', 'zone_id').agg(count('*').alias('month_reaction')) \
        .select('month', 'month_reaction', 'zone_id')

    subscription_month = data \
        .where("event_type = 'subscription'") \
        .withColumn('month', trunc(col('date'), "Month")) \
        .groupBy('month', 'zone_id').agg(count('*').alias('month_subscription')) \
        .select('month', 'month_subscription', 'zone_id')

    return messages_month \
        .join(messages_month_registration, ['month', 'zone_id'], 'full') \
        .join(reaction_month, ['month', 'zone_id'], 'full') \
        .join(subscription_month, ['month', 'zone_id'], 'full')


def week_data(data, old_registration):
    messages_week_registration = data \
        .where("event_type = 'message'") \
        .select('user_id', 'zone_id', 'date') \
        .join(old_registration, 'user_id', 'inner') \
        .withColumn('week', date_trunc("week", col('date'))) \
        .groupBy('week', 'zone_id').agg(count('*').alias('week_user')) \
        .select('week', 'week_user', 'zone_id')

    messages_week = data \
        .where("event_type = 'message'") \
        .withColumn('week', date_trunc("week", col('date'))) \
        .groupBy('week', 'zone_id').agg(count('*').alias('week_message')) \
        .select('week', 'week_message', 'zone_id')

    reaction_week = data \
        .where("event_type = 'reaction'") \
        .withColumn('week', date_trunc("week", col('date'))) \
        .groupBy('week', 'zone_id').agg(count('*').alias('week_reaction')) \
        .select('week', 'week_reaction', 'zone_id')

    subscription_week = data \
        .where("event_type = 'subscription'") \
        .withColumn('week', date_trunc("week", col('date'))) \
        .groupBy('week', 'zone_id').agg(count('*').alias('week_subscription')) \
        .select('week', 'week_subscription', 'zone_id')

    return messages_week \
        .join(messages_week_registration, ['week', 'zone_id'], 'full') \
        .join(reaction_week, ['week', 'zone_id'], 'full') \
        .join(subscription_week, ['week', 'zone_id'], 'full')


def second_view_maker(all_data, old_registration):
    base = all_data \
        .select(trunc(col('date'), "Month").alias('month'), date_trunc("week", col('date')).alias('week'),
                col('id').alias('zone_id')).distinct()
    data = all_data.select(col('event.message_from').alias('user_id'), 'date', col('id').alias('zone_id'), 'event_type')
    month_info = month_data(data=data, old_registration=old_registration)
    week_info = week_data(data=data, old_registration=old_registration)
    return base \
        .join(month_info, ['month', 'zone_id'], 'inner') \
        .join(week_info, ['week', 'zone_id'], 'inner') \
        .select('month', 'week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user',
                'month_message', 'month_reaction', 'month_subscription', 'month_user')
