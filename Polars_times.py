
import pandas as pd
import polars as pl
from polars import col
import numpy as np
import time
import pickle

#####################################################################################

def polars_tasks(url_data) :

    start_time = time.time()
    taxis_df = pl.read_csv(url_data)
    end_time = time.time()
    data_reading_polars_time.append(end_time - start_time)


    start_time = time.time()
    taxis_df = taxis_df.with_columns([pl.col("tpep_pickup_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
                                      pl.col("tpep_dropoff_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                                     ])
    taxis_df = taxis_df.with_columns((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).alias("trip_duration_hours"))
    taxis_df = taxis_df.with_columns((pl.col("trip_duration_hours").cast(pl.Float64) / 1_000_000 / 3600).round(3))
    taxis_df = taxis_df.filter(col("trip_duration_hours") > 0)
    for col_name in ['total_amount', 'fare_amount'] :
        taxis_df = taxis_df.filter(col(col_name) > 0)
    for col_name in ['tip_amount', 'mta_tax'] :
        taxis_df = taxis_df.filter(col(col_name) >= 0)
    for col_name in ['total_amount', 'tolls_amount'] :
        Q1 = taxis_df.select(pl.col(col_name).quantile(0.25)) 
        Q3 = taxis_df.select(pl.col(col_name).quantile(0.75)) 
        IQR = Q3 - Q1
        upper_bound = Q3 + 2.5 * IQR
        taxis_df = taxis_df.filter(pl.col(col_name) <= upper_bound)
    end_time = time.time()

    data_cleaning_polars_time.append(end_time - start_time)

    #############################################

    # Task1: Polars

    start_time = time.time()
    taxis_df = taxis_df.with_columns((col("trip_distance") * 1.609344).alias("trip_distance_km"))
    taxis_df = taxis_df.with_columns((col("trip_distance_km") / col("trip_duration_hours")).alias("trip_speed_km_h"))
    taxis_df = taxis_df.with_columns(col("tpep_pickup_datetime").dt.hour().alias("hour_time"))
    hours_speed_df = taxis_df.group_by("hour_time").agg(pl.col("trip_speed_km_h").mean().alias("avg_speed")).sort("hour_time") 
    end_time = time.time()

    task1_polars_time.append(end_time - start_time)  

#####################################################################################

times_df_dict = dict()

for data in ['taxis.csv', 'taxis_15M.csv', 'taxis_62M.csv', 'taxis_124M.csv',  'taxis_186M.csv'] :

    print(data)
    url_data = f'./Data/{data}'

    times_dict = dict()
    data_reading_polars_time = list()
    data_cleaning_polars_time = list()
    task1_polars_time = list()

    for i in range(0, 3):

        polars_tasks(url_data)

    times_dict['data_reading_polars_time'] = data_reading_polars_time  
    times_dict['data_cleaning_pandas_time'] = data_cleaning_polars_time  
    times_dict['task1_polars_time'] = task1_polars_time  
    times_df = pd.DataFrame(times_dict)
    times_df_dict[data] = times_df

#####################################################################################

# save a dictionary as a pickle
with open('Polars_times_df_dict.pickle', 'wb') as file:
    # Use pickle's dump function to write the dict to the opened file.
    pickle.dump(times_df_dict, file)
