from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, desc
from pyspark.sql.window import Window
import datetime as dt
import pyspark.sql.functions as F
import sys


base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_base_path = '/user/master/data/geo/events'

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

reactions = spark.read.parquet(base_url+events_base_path)
reactions.printSchema()
reactions.show(20, False)