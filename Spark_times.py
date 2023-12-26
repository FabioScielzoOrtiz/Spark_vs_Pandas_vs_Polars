import findspark
findspark.init()
from pyspark.sql import SparkSession
# Create a SparkSession with memory configurations
spark = SparkSession.builder.appName('Intro to Spark') \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .master("local[*]").getOrCreate()
# Access the SparkContext from the SparkSession
sc = spark.sparkContext
import pandas as pd
import numpy as np
import time
import pickle
from pyspark.sql.functions import col, round, expr, from_unixtime, unix_timestamp, date_format
from pyspark.sql.types import IntegerType

#####################################################################################

def spark_tasks(url_data, rdd) :

    # Data cleaning: Spark DF
    start_time = time.time()
    taxis_df = spark.read.format("csv").option("inferSchema", "true").option("timestampFormat","yyyy-MM-dd HH:mm:ss").option("header", "true").option("mode", "DROPMALFORMED").load(url_data)
    end_time = time.time()  
    data_reading_spark_df_time.append(end_time - start_time )
 
   
    start_time = time.time()
    taxis_df = taxis_df.withColumn("trip_duration_hours", round(expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600"), 3))
    taxis_df = taxis_df.filter(taxis_df['trip_duration_hours'] > 0)
    for col_name in ['total_amount', 'fare_amount'] :
        taxis_df = taxis_df.filter(taxis_df[col_name] > 0)
    for col_name in ['tip_amount', 'mta_tax'] :
        taxis_df = taxis_df.filter(taxis_df[col_name] >= 0)       
    for col_name in ['total_amount', 'tolls_amount'] :
        quantiles = taxis_df.select(col_name).summary("25%", "75%").collect()
        Q25 = float(quantiles[0][col_name])
        Q75 = float(quantiles[1][col_name])
        IQR = Q75 - Q25
        upper_bound = Q75 + 2.5*IQR
        taxis_df = taxis_df.filter(taxis_df[col_name] <= upper_bound)
    end_time = time.time()

    data_cleaning_spark_df_time.append(end_time - start_time )

    #############################################

    # Task1: Spark DF

    taxis_df_copy = taxis_df.alias('taxis_df_copy')

    start_time = time.time()
    taxis_df_copy = (taxis_df_copy
                      .withColumn("trip_distance_km", round(col("trip_distance") * 1.609344, 3))
                      .withColumn("trip_speed_km_h", round(col("trip_distance_km") / col("trip_duration_hours"), 3))
                      .withColumn("hour_time", date_format(from_unixtime(unix_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")), "HH"))
                      .withColumn("hour_time", col("hour_time").cast(IntegerType()))  # Cast here to avoid another DataFrame transformation later
                      )
    hours_speed_df = (taxis_df_copy.groupBy("hour_time").mean('trip_speed_km_h'))
    hours_speed_df_pd = hours_speed_df.toPandas()
    end_time = time.time()
    task1_spark_df_time.append(end_time - start_time)

    #############################################

    # Task1: Spark RDD

    if rdd == True :

        taxis_df_copy = taxis_df.alias('taxis_df_copy')

        def calculate_elapsed_hours(start_time, end_time):
           elapsed_time = (end_time - start_time).total_seconds() / 3600
           return np.round(elapsed_time,3)

        start_time = time.time()
        trip_distance_rdd = taxis_df_copy.select('trip_distance').rdd.map(lambda x: x[0])
        trip_distance_km_rdd = trip_distance_rdd.map(lambda x : np.round(x*1.609344, 3))
        tpep_dropoff_datetime_rdd = taxis_df_copy.select('tpep_dropoff_datetime').rdd.map(lambda x: x[0])
        tpep_pickup_datetime_rdd = taxis_df_copy.select('tpep_pickup_datetime').rdd.map(lambda x: x[0])
        datetime_rdd = taxis_df_copy.select('tpep_pickup_datetime', 'tpep_dropoff_datetime').rdd.map(lambda x: (x[0],x[1]))
        trip_duration_rdd = datetime_rdd.map(lambda x : calculate_elapsed_hours(x[0], x[1]))
        trip_distance_time_rdd = trip_distance_km_rdd.zip(trip_duration_rdd)
        trip_speed_rdd = trip_distance_time_rdd.map(lambda x : np.round(x[0] / x[1], 3))
        trip_hour_rdd = tpep_pickup_datetime_rdd.map(lambda x : x.hour)
        key_hour_rdd = trip_hour_rdd.map(lambda x : (x,1))
        trip_hour_sum_hour_rdd = key_hour_rdd.reduceByKey(lambda x,y : x + y)
        trip_hour_speed_rdd = trip_hour_rdd.zip(trip_speed_rdd)
        trip_hour_sum_speed_rdd = trip_hour_speed_rdd.reduceByKey(lambda x,y : np.round(x + y, 3)) 
        avg_speed_hour_rdd = trip_hour_sum_hour_rdd.zip(trip_hour_sum_speed_rdd).map(lambda x : (np.round(x[1][1] / x[0][1], 3))) 
        hours_new_order_rdd = trip_hour_sum_hour_rdd.map(lambda x : x[0])
        avg_speed_hour_list = avg_speed_hour_rdd.collect()
        hours_new_order_list = hours_new_order_rdd.collect()
        end_time = time.time()

        task1_spark_rdd_time.append(end_time - start_time)

    #############################################

    # Task1: Spark SQL

    taxis_df_copy = taxis_df.alias('taxis_df_copy')

    start_time = time.time()
    taxis_df_copy.createOrReplaceTempView('taxis_df_copy')
    combined_query = """
    SELECT 
        HOUR(tpep_pickup_datetime) as hour_time, 
        AVG(ROUND((trip_distance * 1.609344) / trip_duration_hours, 3)) as avg_trip_speed_km_h 
    FROM 
        taxis_df_copy 
    GROUP BY 
        HOUR(tpep_pickup_datetime)
    ORDER BY
        hour_time
    """
    hours_speed_df = spark.sql(combined_query)
    hours_speed_df_pd = hours_speed_df.toPandas()
    end_time = time.time()

    task1_spark_sql_time.append(end_time - start_time)

#####################################################################################

times_df_dict = dict()

for data in ['taxis.csv', 'taxis_15M.csv', 'taxis_62M.csv', 'taxis_124M.csv',  'taxis_186M.csv'] :
  
    print(data)  
    url_data = f'/home/jovyan/code/Taxis-Project/Data/{data}'

    times_dict = dict()
    data_reading_spark_df_time = list()
    data_cleaning_spark_df_time = list()
    task1_spark_df_time = list()
    task1_spark_rdd_time = list()
    task1_spark_sql_time = list()

    if data in ['taxis.csv', 'taxis_15M.csv'] :

        for i in range(0, 3):

            spark_tasks(url_data, rdd=True)

        times_dict['data_reading_spark_df_time'] = data_reading_spark_df_time
        times_dict['data_cleaning_spark_df_time'] = data_cleaning_spark_df_time  
        times_dict['task1_spark_df_time'] = task1_spark_df_time  
        times_dict['task1_spark_rdd_time'] = task1_spark_rdd_time  
        times_dict['task1_spark_sql_time'] = task1_spark_sql_time 
        times_df = pd.DataFrame(times_dict)
        times_df_dict[data] = times_df

    else :

        for i in range(0, 3):

            spark_tasks(url_data, rdd=False)

        times_dict['data_reading_spark_df_time'] = data_reading_spark_df_time
        times_dict['data_cleaning_spark_df_time'] = data_cleaning_spark_df_time  
        times_dict['task1_spark_df_time'] = task1_spark_df_time  
        times_dict['task1_spark_sql_time'] = task1_spark_sql_time  
        times_df = pd.DataFrame(times_dict)
        times_df_dict[data] = times_df

#####################################################################################

# save a dictionary as a pickle
with open('Spark_times_df_dict.pickle', 'wb') as file:
    # Use pickle's dump function to write the dict to the opened file.
    pickle.dump(times_df_dict, file)
