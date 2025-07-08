import os
import pandas as pd
from multiprocessing import Pool, Process



def read_csv_and_return_df(file_path):
    """
    Function to read a CSV file and return a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def combine_dataframes(dfs):
    """
    Function to combine a list of DataFrames into a single DataFrame.
    """
    try:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    except Exception as e:
        print(f"Error combining DataFrames: {e}")
        return None

def split_data_by_revenue(data):
    over_60_usd = data[data['total_amount'] > 60]
    less_60_usd = data[data['total_amount'] <= 60]
    return over_60_usd,less_60_usd

def get_amount_by_date(data):
    grp = data.groupby(by='tpep_pickup_datetime').sum('total_amount')
    return grp
