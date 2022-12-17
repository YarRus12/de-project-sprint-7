def distance(data, first_lat, second_lat, first_lon, second_lon):
    from pyspark.sql.functions import sin, cos, asin, sqrt, col, pow, lit, row_number
    from pyspark.sql.window import Window
    result = data.withColumn('1', pow(sin((col(second_lat) - col(first_lat)) / lit(2)), 2)) \
        .withColumn('2', cos(col(first_lat)) * cos(col(second_lat))) \
        .withColumn('3', pow(sin((col(second_lon) - col(first_lon)) / lit(2)), 2)) \
        .withColumn('4', sqrt(col('1') + (col('2') * col('3')))) \
        .withColumn('distanse', 2 * 6371 * (asin((col('4')))))
    return result