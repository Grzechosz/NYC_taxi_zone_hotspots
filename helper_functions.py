import pandas as pd
import pyarrow.parquet as pq

def read_taxi_files(path, month_number):
    return pq.read_table(path+str(month_number).zfill(2)+'.parquet').to_pandas()

def drop_rows_with_nans_in_PULocationID(dataframes):
    cleaned_list = []
    
    for df in dataframes:
        if df['PULocationID'].isna().any():
            df = df.dropna(subset=['PULocationID'])
            cleaned_list.append(df)
        else:
            cleaned_list.append(df)
    return cleaned_list