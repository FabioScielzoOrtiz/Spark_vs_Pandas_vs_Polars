
import pandas as pd
import numpy as np
import time
import pickle

#####################################################################################

def pandas_tasks(url_data) :

    start_time = time.time()
    taxis_df_pd = pd.read_csv(url_data)
    end_time = time.time()
    data_reading_pandas_time.append(end_time - start_time)


    start_time = time.time()
    taxis_df_pd['tpep_pickup_datetime'] = pd.to_datetime(taxis_df_pd['tpep_pickup_datetime'])
    taxis_df_pd['tpep_dropoff_datetime'] = pd.to_datetime(taxis_df_pd['tpep_dropoff_datetime'])
    taxis_df_pd['trip_duration_hours'] = ((taxis_df_pd['tpep_dropoff_datetime'] - taxis_df_pd['tpep_pickup_datetime']).dt.total_seconds() / 3600).round(3)
    taxis_df_pd = taxis_df_pd.loc[taxis_df_pd['trip_duration_hours'] > 0, :]
    for col in ['total_amount', 'fare_amount'] :
        taxis_df_pd = taxis_df_pd.loc[taxis_df_pd[col] > 0, :]
    for col in ['tip_amount', 'mta_tax'] :
        taxis_df_pd = taxis_df_pd.loc[taxis_df_pd[col] >= 0, :]
    for col in ['total_amount', 'tolls_amount'] :
        Q75 = taxis_df_pd[col].quantile(0.75) 
        Q25 = taxis_df_pd[col].quantile(0.25) 
        IQR = Q75 - Q25
        upper_bound = Q75 + 2.5*IQR
        taxis_df_pd = taxis_df_pd.loc[taxis_df_pd[col] <= upper_bound, :]
    end_time = time.time()

    data_cleaning_pandas_time.append(end_time - start_time)

    #############################################

    # Task1: Pandas

    start_time = time.time()
    taxis_df_pd['trip_distance_km'] = (taxis_df_pd["trip_distance"]*1.609344).round(3)
    taxis_df_pd['trip_speed_km_h'] = (taxis_df_pd["trip_distance_km"] / taxis_df_pd['trip_duration_hours']).round(3)
    taxis_df_pd['hour_time'] =  taxis_df_pd['tpep_pickup_datetime'].dt.hour
    hours_speed_df_pd = taxis_df_pd.groupby(by='hour_time')['trip_speed_km_h'].mean()
    end_time = time.time()

    task1_pandas_time.append(end_time - start_time)  

#####################################################################################

times_df_dict = dict()

for data in ['taxis.csv', 'taxis_15M.csv', 'taxis_62M.csv', 'taxis_124M.csv',  'taxis_186M.csv'] :

    print(data)
    url_data = f'./Data/{data}'

    times_dict = dict()
    data_reading_pandas_time = list()
    data_cleaning_pandas_time = list()
    task1_pandas_time = list()

    for i in range(0, 3):

        pandas_tasks(url_data)

    times_dict['data_reading_pandas_time'] = data_reading_pandas_time  
    times_dict['data_cleaning_pandas_time'] = data_cleaning_pandas_time  
    times_dict['task1_pandas_time'] = task1_pandas_time  
    times_df = pd.DataFrame(times_dict)
    times_df_dict[data] = times_df

#####################################################################################

# save a dictionary as a pickle
with open('Pandas_times_df_dict.pickle', 'wb') as file:
    # Use pickle's dump function to write the dict to the opened file.
    pickle.dump(times_df_dict, file)
