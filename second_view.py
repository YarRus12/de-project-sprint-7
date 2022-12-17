from pyspark.sql.functions import month, col, weekofyear, count

# Почему первые сообщение это регистрация и нужно ли выискивать первое сообщение из всего пула данных, а не только из выборки?!
# что значит: Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя. Это про что?

def month_data(all_data):
    data = all_data.select(month('date'), col('id').alias('zone_id'), 'event_type')
    messages_month = data \
        .where("event_type = 'message'") \
        .groupBy('month(date)', 'zone_id').agg(count('*').alias('month_message')) \
        .select(col('month(date)').alias('month'), 'month_message', 'zone_id')
    reaction_month = data \
        .where("event_type = 'reaction'") \
        .groupBy('month(date)', 'zone_id').agg(count('*').alias('month_reaction')) \
        .select(col('month(date)').alias('month'), 'month_reaction', 'zone_id')
    subscription_month = data \
        .where("event_type = 'subscription'") \
        .groupBy('month(date)', 'zone_id').agg(count('*').alias('month_subscription')) \
        .select(col('month(date)').alias('month'), 'month_subscription', 'zone_id')
    return messages_month \
        .join(reaction_month, ['month', 'zone_id'], 'inner') \
        .join(subscription_month, ['month', 'zone_id'], 'inner')


def week_data(all_data):
    data = all_data.select(weekofyear('date'), col('id').alias('zone_id'), 'event_type')

    messages_week = data \
        .where("event_type = 'message'") \
        .groupBy('weekofyear(date)', 'zone_id').agg(count('*').alias('week_message')) \
        .select(col('weekofyear(date)').alias('week'), 'week_message', 'zone_id')

    reaction_week = data \
        .where("event_type = 'reaction'") \
        .groupBy('weekofyear(date)', 'zone_id').agg(count('*').alias('week_reaction')) \
        .select(col('weekofyear(date)').alias('week'), 'week_reaction', 'zone_id')

    subscription_week = data \
        .where("event_type = 'subscription'") \
        .groupBy('weekofyear(date)', 'zone_id').agg(count('*').alias('week_subscription')) \
        .select(col('weekofyear(date)').alias('week'), 'week_subscription', 'zone_id')

    return messages_week \
        .join(reaction_week, ['week', 'zone_id'], 'full') \
        .join(subscription_week, ['week', 'zone_id'], 'full')


def second_view_maker(all_data):
    base = all_data \
        .select(month('date').alias('month'), weekofyear('date').alias('week'), col('id').alias('zone_id'))
    month_info = month_data(all_data=all_data)
    week_info = week_data(all_data=all_data)
    return base\
        .join(month_info, ['month', 'zone_id'], 'inner') \
        .join(week_info, ['week', 'zone_id'], 'inner') \
        .select('month', 'week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription',
            'month_message', 'month_reaction', 'week_subscription').distinct()