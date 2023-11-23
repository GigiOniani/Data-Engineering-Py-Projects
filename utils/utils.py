import numpy as np
import pandas as pd
import os
from collections import Counter

folder_path = "C:\\Users\\User\\Desktop\\Assignment 1"
output_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data'
partitioning_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\uid_post_partitioned'
lost_records = "./lost_records.txt"



filename_list = []

friends ='C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\friends_table.csv'
posts = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\posts_table.csv'
reactions = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\reactions_table.csv'
users = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\user_table.csv'

def check_folder_exists(folder_path):
    if os.path.exists(folder_path):
        print(f'{folder_path} -- folder exists!')
    else:
        print(' folder doesnt exist!')


def check_folder_size(folder_name):
    file_stats = os.stat(folder_name)
    if file_stats == 0:
        return False
    else:
        print(f'{folder_name} -- folder size is {file_stats.st_size}MB')


def read_csv_files(folder_name):
    try:
        for filename in os.listdir(folder_name):
            filename_list.append(filename)
            df = pd.read_csv(f'{folder_path}\\{filename}')
            yield df
        print('files read successfully!')

    except IOError as err:
        raise IOError(f'something wrong happened! {err}')


def transfer_files_to_datadir(folder_path, output_directory):
    output_dir = output_directory
    os.makedirs(output_directory, exist_ok=True)

    for i,file in enumerate(read_csv_files(folder_path)):
        output_file = os.path.join(output_dir, f'{filename_list[i]}')
        file.to_csv(output_file, index=False)
        print(f'DataFrame {filename_list[i]} saved to: {output_file}')


def read_file_log_lost(filename: str):
    try:
        with open(filename, "r") as f:
            for idx, record in enumerate(f):
                yield idx, record
    except Exception as err:
        print(f"Unable to open {filename} reason: {err}")
        log_lost_records(idx)

def log_lost_records(idx) -> None:
    with open(lost_records, "a") as f:
        f.write(f"{idx}\n")

def list_top_ten_name(filename_users,top_n):
    record = read_file_log_lost(filename_users)
    names = []
    for values in record:
        value = list(values)
        surname,name,age,subscription = value[1].strip().split(',')
        names.append(name)
    count_names = Counter(names)

    for name, count in count_names.most_common(top_n):
        print(f"{name} has occured: {count} times")


def partition_by_user_post(filename_posts,partitioning_directory):
    record = read_file_log_lost(filename_posts)
    os.makedirs(partitioning_directory, exist_ok=True)
    for rows in record:
        user_id, post_id, post_type, post_date = rows[1].strip().split(',')
        file_name = f"{post_type}.txt"
        file_path = os.path.join(partitioning_directory, file_name)

        with open(file_path,'a') as file:
            file.writelines(f'{user_id},{post_type},{post_id},{post_date}\n')


def list_five_people_post_reacts(posts,reactions):
    posts_table = pd.read_csv(posts)
    reactions_table = pd.read_csv(reactions)
    unique_id = pd.concat([posts_table['User'],reactions_table['User']])
    uid = unique_id.drop_duplicates()
    df = pd.DataFrame(uid,columns=['User'])
    df1 = pd.merge(df['User'],posts_table[['User','ID']],left_on='User',right_on='User',how = 'outer')
    df1.rename(columns={'ID':'post_id1'}, inplace =True )
    reactions_table.rename(columns={'post_id' : 'react'},inplace=True)
    df_all = pd.merge(df1[['User','post_id1']],reactions_table[['User','react']],left_on = 'User',right_on='User',how = 'outer')
    df_all.dropna(inplace = True)
    df_all.drop_duplicates(inplace=True)
    # print(df_all[df_all['User']==7])
    grp = df_all.groupby(by='User').agg({'post_id1': 'nunique','react':'nunique'})

    print(grp.sort_values(by=['post_id1','react'],ascending=False).head())

