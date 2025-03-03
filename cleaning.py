import pandas as pd
import helper_functions as hf

def clean_yellow_taxi(data):
    # checking corect dates
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"])
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"])
    data = data[data["tpep_pickup_datetime"] < data["tpep_dropoff_datetime"]]

    # passenger count lower than 0
    data = data[data["passenger_count"] > 0]

    # non existing rides and too long
    data = data[(data["trip_distance"] > 0) & (data["trip_distance"] <= 60)]

    # trip duration
    data["trip_duration"] = (data["tpep_dropoff_datetime"] - data["tpep_pickup_datetime"]).dt.total_seconds()
    data = data[(data["trip_duration"] > 30) & (data["trip_duration"] < 86400)]  # 30 sek < trip_duration < 24h

    # total amount lower than zero
    data = data[data["total_amount"] >= 0]

    # only real taxi zones
    data = data[(data["PULocationID"].between(1, 263)) & (data["DOLocationID"].between(1, 263))]
    
    return data

def clean_green_taxi(data):
    # checking corect dates
    data["lpep_pickup_datetime"] = pd.to_datetime(data["lpep_pickup_datetime"])
    data["lpep_dropoff_datetime"] = pd.to_datetime(data["lpep_dropoff_datetime"])
    data = data[data["lpep_pickup_datetime"] < data["lpep_dropoff_datetime"]]

    # passenger count lower than 0
    data = data[data["passenger_count"] > 0]

    # non existing rides and too long
    data = data[(data["trip_distance"] > 0) & (data["trip_distance"] <= 60)]

    # trip duration
    data["trip_duration"] = (data["lpep_dropoff_datetime"] - data["lpep_pickup_datetime"]).dt.total_seconds()
    data = data[(data["trip_duration"] > 30) & (data["trip_duration"] < 86400)]  # 30 sek < trip_duration < 24h

    # total amount lower than zero
    data = data[data["total_amount"] >= 0]

    # only real taxi zones
    data = data[(data["PULocationID"].between(1, 263)) & (data["DOLocationID"].between(1, 263))]
    
    return data


def clean_fhv(data):
    # checking corect dates
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"])
    data["dropOff_datetime"] = pd.to_datetime(data["dropOff_datetime"])
    data = data[data["pickup_datetime"] < data["dropOff_datetime"]]

    # trip duration
    data["trip_duration"] = (data["dropOff_datetime"] - data["pickup_datetime"]).dt.total_seconds()
    data = data[(data["trip_duration"] > 30) & (data["trip_duration"] < 86400)]  # 30 sek < trip_duration < 24h

    # only real taxi zones
    data = data[(data["PUlocationID"].between(1, 263)) & (data["DOlocationID"].between(1, 263))]
    
    return data

def cleanhv_hv_fhv(data):
    # checking corect dates
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"])
    data["dropoff_datetime"] = pd.to_datetime(data["dropoff_datetime"])
    data = data[data["pickup_datetime"] < data["dropoff_datetime"]]

    # trip duration
    data = data[(data["trip_time"] > 30) & (data["trip_time"] < 86400)]  # 30 sek < trip_duration < 24h

    # only real taxi zones
    data = data[(data["PULocationID"].between(1, 263)) & (data["DOLocationID"].between(1, 263))] 

    # non existing rides and too long
    data = data[(data["trip_miles"] > 0) & (data["trip_miles"] <= 60)]
    
    # total amount lower than zero
    data = data[data["base_passenger_fare"] >= 0]
    return data