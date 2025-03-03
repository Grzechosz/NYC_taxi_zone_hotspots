import helper_functions as hf
import cleaning as cll
import pandas as pd
from sqlalchemy import create_engine

yellow_taxi_columns_to_drop = ['VendorID', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
       'DOLocationID', 'payment_type', 'fare_amount', 'extra',"trip_duration",
       'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
       'total_amount', 'congestion_surcharge', 'Airport_fee']

green_columns_to_drop = ['VendorID','lpep_dropoff_datetime',
       'store_and_fwd_flag', 'RatecodeID', 'DOLocationID',"trip_duration",
       'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
       'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
       'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge']

fhv_columns_to_drop = ['dispatching_base_num', 'dropOff_datetime',
       'DOlocationID', 'SR_Flag', 'Affiliated_base_number']

hv_fhv_columns_to_drop = ['hvfhs_license_num', 'dispatching_base_num', 'originating_base_num',
        'on_scene_datetime', 'request_datetime',
       'dropoff_datetime', 'DOLocationID', 'trip_miles',
       'trip_time', 'base_passenger_fare', 'tolls', 'bcf', 'sales_tax',
       'congestion_surcharge', 'airport_fee', 'tips', 'driver_pay',
       'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag',
       'wav_request_flag', 'wav_match_flag']

month = 12

print("Loading data")
green_taxi_data = hf.read_taxi_files('data_files/green_taxi_data/green_tripdata_2024-', month)
yellow_taxi_data = hf.read_taxi_files('data_files/yellow_taxi_data/yellow_tripdata_2024-', month)
fhv_taxi_data = hf.read_taxi_files('data_files/fhv_data/fhv_tripdata_2024-', month)
hv_fhv_taxi_data = hf.read_taxi_files('data_files/hv_fhv_data/fhvhv_tripdata_2024-', month)


print("Cleaning data")
green_cleaned = cll.clean_green_taxi(green_taxi_data)
yellow_cleaned = cll.clean_yellow_taxi(yellow_taxi_data)
fhv_cleaned = cll.clean_fhv(fhv_taxi_data)
hv_fhv_cleaned = cll.cleanhv_hv_fhv(hv_fhv_taxi_data)

green_taxi_data = green_cleaned
yellow_taxi_data = yellow_cleaned
fhv_taxi_data = fhv_cleaned
hv_fhv_taxi_data = hv_fhv_cleaned

print("Dropping unused columns")
green_taxi_data = green_taxi_data.drop(green_columns_to_drop, axis=1)
yellow_taxi_data = yellow_taxi_data.drop(yellow_taxi_columns_to_drop, axis=1)
fhv_taxi_data = fhv_taxi_data.drop(fhv_columns_to_drop, axis=1)
hv_fhv_taxi_data = hv_fhv_taxi_data.drop(hv_fhv_columns_to_drop, axis=1)

green_taxi_data = green_taxi_data.rename(columns={"lpep_pickup_datetime" : "pickup_datetime"})
yellow_taxi_data = yellow_taxi_data.rename(columns={"tpep_pickup_datetime" : "pickup_datetime"})
fhv_taxi_data = fhv_taxi_data.rename(columns={"PUlocationID" : "PULocationID"})


cleaned_dfs = hf.drop_rows_with_nans_in_PULocationID([green_taxi_data, yellow_taxi_data,fhv_taxi_data, hv_fhv_taxi_data])

print("Merging data")
concaternated_data = pd.concat(cleaned_dfs, ignore_index=True)

print("New columns")
concaternated_data["hour"] = concaternated_data["pickup_datetime"].dt.hour
concaternated_data["day_of_week"] = concaternated_data["pickup_datetime"].dt.dayofweek
concaternated_data["month"] = concaternated_data["pickup_datetime"].dt.month
concaternated_data["day"] = concaternated_data["pickup_datetime"].dt.day

df_grouped = concaternated_data.groupby(['month','day', 'day_of_week', 'hour', 'PULocationID']).size().reset_index(name='ride_count')

data_for_database = df_grouped.drop(['month', 'day'], axis=1)

engine = create_engine('mysql+pymysql://root:BURAK@127.0.0.1:3306/nyc_taxi', echo=False)
print("Writing to db")
data_for_database.to_sql(name="rides", con=engine, if_exists = 'append', index=False)

engine.dispose()