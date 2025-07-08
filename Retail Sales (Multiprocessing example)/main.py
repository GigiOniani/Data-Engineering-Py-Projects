import os
import pandas as pd
from multiprocessing import Pool, Process
from utils.utils import *


if __name__ == "__main__":
    # Folder path
    folder_path = 'C:\\Users\\User\\Desktop\\NYC_Taxi'

    # List to store DataFrames
    dfs = []

    # List to store file paths
    file_paths = []

    # Loop through each file in the folder and construct file paths
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_paths.append(os.path.join(folder_path, file_name))

    # Use multiprocessing to read CSV files into DataFrames
    with Pool() as pool:
        dfs = pool.map(read_csv_and_return_df, file_paths)

    # Remove None values from the list (in case of errors during reading)
    dfs = [df for df in dfs if df is not None]

    # Combine DataFrames
    combined_df = combine_dataframes(dfs)

    # Display the combined DataFrame
    print(combined_df)

    process1 = Process(target=split_data_by_revenue, args=(combined_df,))
    process2 = Process(target=get_amount_by_date,args=(combined_df,) )

    process1.start()
    process2.start()

    process1.join()
    process2.join()

    over_60, below_60 = split_data_by_revenue(combined_df)
    get_amount_by_date(combined_df)
