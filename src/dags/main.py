from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from src.scripts import download_data, first_view, functional, second_view, third_view
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_base_path = '/user/master/data/geo/events'

spark = SparkSession.builder \
    .master("local") \
    .appName("Learning DataFrames") \
    .getOrCreate()

city_events = []

def create_test_partition(url, events_path, spark_session):
    """ Подготовка данных """
    download_data.create_test_partitions(base_url=url, events_base_path=events_path, spark=spark_session)


def take_df(spark_session):
    """Базовая выборка для построения витрин"""
    events = spark_session.read \
        .parquet(f"{base_url}/user/yarruss12/analytics/test100")
    city_events_data = functional.distance(data=events, first_lat='lat', second_lat='city_lat', first_lon='lon',
                                           second_lon='city_long') \
        .select('event', 'event_type', 'id', 'city', 'date', 'lat', 'lon', 'distanse') \
        .withColumn("row_number", row_number().over(Window.partitionBy("event").orderBy("distanse"))) \
        .where("row_number=1")
    return city_events_data


def first_task(city_events_data):
    """Реализация 1 витрины"""
    first_view.first_view_maker(city_events_data)\
        .write.mode("overwrite").parquet(f"{base_url}/user/yarruss12/analytics/project/1_view")


def second_task(city_events_data):
    """Реализация 2 витрины"""
    from pyspark.sql.functions import min, col
    # Дополнительная обработка регистрации, в проме я бы отнес это на отдельную джобу и выполнял бы по рассписанию,
    # Но здесь это делать наверное нет нужды
    old_users = spark.read \
        .parquet(f"{base_url}/user/yarruss12/analytics/allregistration").select('user_id')
    new_users = city_events_data.select(col('event.message_from').alias('user_id'), 'date') \
        .join(old_users, 'user_id', 'leftanti') \
        .groupBy('user_id').agg(min('date'))
    spark.catalog.refreshTable("old_users") # Не совсем верно
    new_users.write.mode("append").parquet(f"{base_url}/user/yarruss12/analytics/allregistration")
    # данные о датах регистрации пользователей
    old_registration = spark.read \
        .parquet(f"{base_url}/user/yarruss12/analytics/allregistration").select('user_id')
    second_view.second_view_maker(city_events_data, old_registration)\
        .write.mode("overwrite").parquet(f"{base_url}/user/yarruss12/analytics/project/2_view")


def third_task(city_events_data):
    """Реализация 2 витрины"""
    third_view.third_view_maker(city_events_data)\
        .write.mode("overwrite").parquet(f"{base_url}/user/yarruss12/analytics/project/3_view")


dag = DAG(
    schedule_interval=None,
    dag_id='project_7_spark',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['first_view, second_view, third_view'],
    is_paused_upon_creation=True)
create_test_partition = PythonOperator(task_id='create_test_partition',
                                       python_callable=create_test_partition,
                                       op_kwargs={'base_url': 'base_url', 'events_base_path': 'events_base_path',
                                                  'spark_session': 'spark'},
                                       dag=dag)
take_df = PythonOperator(task_id='take_df',
                         python_callable=take_df,
                         op_kwargs={'spark_session': 'spark'},
                         dag=dag)
first_task = PythonOperator(task_id='first_task',
                         python_callable=first_task,
                         op_kwargs={'city_events': 'city_events'},
                         dag=dag)
second_task = PythonOperator(task_id='second_task',
                         python_callable=second_task,
                         op_kwargs={'city_events': 'city_events'},
                         dag=dag)
third_task = PythonOperator(task_id='third_task',
                         python_callable=third_task,
                         op_kwargs={'city_events': 'city_events'},
                         dag=dag)

create_test_partition >> take_df >> [first_task >> second_task >> third_task]