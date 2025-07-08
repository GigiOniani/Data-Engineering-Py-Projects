import numpy as np
import pandas as pd
import os
from collections import Counter
from datetime import datetime
import time
import networkx as nx
import matplotlib.pyplot as plt

folder_path = "C:\\Users\\User\\Desktop\\Assignment 1"
output_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data'
partitioning_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\uid_post_partitioned'
lost_records = "./lost_records.txt"

filename_list = []

def check_folder_exists(folder_path : str):
    """
    :param folder_path: data source (in our case it is folder)
    :return: notification of folder status
    """
    if os.path.exists(folder_path):
        print(f'{folder_path} -- folder exists!')
    else:
        print(' folder doesnt exist!')


def check_folder_size(folder_name : str):
    """

    :param folder_name: data source (in our case it is folder)
    :return: boolean value
    """
    file_stats = os.stat(folder_name)
    if file_stats == 0:
        return False
    else:
        print(f'{folder_name} -- folder size is {file_stats.st_size}MB')

def read_csv_files(folder_name : str):
    """

    :param folder_name: data source (in our case it is folder)
    :return: generator, that reads file
    """
    try:
        for filename in os.listdir(folder_name):
            filename_list.append(filename)
            df = pd.read_csv(f'{folder_path}\\{filename}')
            yield df
        print('files read successfully!')

    except IOError as err:
        raise IOError(f'something wrong happened! {err}')

def convert_timestamp(timestamp: str):
    """
    :param timestamp: object, that needs to be trasnformed to time format
    :return: datetime object
    """
    obj = datetime.fromtimestamp(timestamp)
    return obj.strftime("%Y/%m/%d, %H:%M:%S")


def transfer_files_to_datadir(folder_path : str, output_directory : str):
    """
    :param folder_path: data source (in our case it is folder)
    :param output_directory: place, where we save our files
    :return: csv files to given path
    """
    output_dir = output_directory
    os.makedirs(output_directory, exist_ok=True)
    files = []
    try:
        for i,file in enumerate(read_csv_files(folder_path)):
            output_file = os.path.join(output_dir, f'{filename_list[i]}')
            files.append(file)

        friends = pd.DataFrame(files[1])
        posts = pd.DataFrame(files[2])
        reactions = pd.DataFrame(files[3])
        users = pd.DataFrame(files[4])

        users['Subscription Date'] = users['Subscription Date'].apply(lambda x: convert_timestamp(x))
        reactions['Reaction Date'] = reactions['Reaction Date'].apply(lambda x: convert_timestamp(x))
        posts['Post Date'] = posts['Post Date'].apply(lambda x: convert_timestamp(x))

        users.to_csv('C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\users_table.csv',index=False)
        friends.to_csv('C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\friends_table.csv', index=False)
        posts.to_csv('C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\posts_table.csv', index=False)
        reactions.to_csv('C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\reactions_table.csv', index=False)

    except IOError as err:
        raise IOError(f'something went wrong see {err}')

def read_file_log_lost(filename: str):
    """
    :param filename: filename source
    :return: list of records of read filename
    """
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
    """
    :param filename_users users data table source
    :param top_n: how many users user need to see.
    :return: list of people names, who occur many times in users table
    """
    record = read_file_log_lost(filename_users)
    names = []
    for values in record:
        value = list(values)
        surname,name,age,subscription,*_ = value[1].strip().split(',')
        names.append(name)
    count_names = Counter(names)

    for name, count in count_names.most_common(top_n):
        print(f"{name} has occured: {count} times")


def partition_by_user_post(filename_posts,partitioning_directory):
    """
    :param filename_posts: posts_table_source
    :param partitioning_directory: file path, where our partitioned data should be stored
    :return: file, that contains partitioned files by post types. each post type must contain user_id,post_type_name,post_id and post_date
    """
    record = read_file_log_lost(filename_posts)
    os.makedirs(partitioning_directory, exist_ok=True)
    next(record)
    for rows in record:
        user_id, post_id, post_type, post_date,post_h_m_s = rows[1].strip().split(',')
        file_name = f"{post_type}.txt"
        file_path = os.path.join(partitioning_directory, file_name)

        with open(file_path,'a') as file:
            file.writelines('user_id')
            file.writelines(f'{user_id},{post_type},{post_id},{post_date},{post_h_m_s}\n')

def time_decorator(func):
    def wrapper():
        start_time = time.time()
        print(f"Main function started at {time.ctime(start_time)}")
        func()
        end_time = time.time()
        print(f"Main function ended at {time.ctime(end_time)}")
        print(f"Total execution time: {end_time - start_time:.5f} seconds")
    return wrapper

def list_five_people_post_reacts(posts,reactions):
    """
    :param posts: post_table
    :param reactions: reactions_table
    :return: top five most_reacted people
    """
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


def plot_user_subgraph(user_ids, csv_file, user_1_col, user_2_col):
    """
    :param user_ids: unique user_id
    :param csv_file: file which must be visualized
    :param user_1_col: user1_id
    :param user_2_col: user2_id
    :return: networking plot
    """
    # Load your data from the CSV file
    df = pd.read_csv(csv_file)

    # Create a directed graph from the DataFrame
    G = nx.from_pandas_edgelist(df, user_1_col, user_2_col, create_using=nx.DiGraph())

    # Create a single plot object
    plt.figure()

    for user_id in user_ids:
        # Check if the user ID exists in either user_1 or user_2 columns
        if user_id in df[user_1_col].values or user_id in df[user_2_col].values:
            # Create a subgraph starting from the user and its neighbors
            neighbors_of_user = set(df[df[user_1_col] == user_id][user_2_col]) | \
                                set(df[df[user_2_col] == user_id][user_1_col])
            neighbors_of_user.add(user_id)
            subgraph = G.subgraph(neighbors_of_user)

            # Plot the network graph with a shell layout
            pos = nx.shell_layout(subgraph)
            nx.draw(subgraph, pos, with_labels=True, font_size=18, node_size=1200, font_color="white")

    # Display the single plot containing all subgraphs
    plt.title(f"Subgraphs for User IDs {user_ids}")
    plt.show()

