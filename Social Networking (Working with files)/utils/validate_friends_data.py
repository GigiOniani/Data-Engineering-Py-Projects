import numpy as np
import pandas as pd
import os
from collections import Counter
from datetime import datetime

folder_path = "C:\\Users\\User\\Desktop\\Assignment 1"
output_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data'
os.makedirs(output_directory,exist_ok=True)
pd.set_option('display.max_columns',10)

def convert_timestamp(timestamp):
    obj = datetime.fromtimestamp(timestamp)
    return obj.strftime("%Y/%m/%d, %H:%M:%S")

def transform_friends_data():
    friends = pd.read_csv('C:\\Users\\User\\Desktop\\Assignment 1\\friends_table.csv')
    posts = pd.read_csv(f'C:\\Users\\User\\Desktop\\Assignment 1\\posts_table.csv')
    reactions = pd.read_csv(f'C:\\Users\\User\\Desktop\\Assignment 1\\reactions_table.csv')
    users = pd.read_csv("C:\\Users\\User\\Desktop\\Assignment 1\\user_table.csv")

    users['User'] = users.index+1
    users['Subscription Date'] = users['Subscription Date'].apply(lambda x: convert_timestamp(x))
    reactions['Reaction Date'] = reactions['Reaction Date'].apply(lambda x: convert_timestamp(x))
    posts['Post Date'] = posts['Post Date'].apply(lambda x: convert_timestamp(x))

    user_and_posts_df = pd.merge(users[['User','Subscription Date']], posts[['User','Post Date']], on='User', how='left')

    full_df = pd.merge(user_and_posts_df, reactions[['User','Reaction Date']], on='User', how='left')

    full_df['Reaction Date'] = pd.to_datetime(full_df['Reaction Date'])
    full_df['Subscription Date'] = pd.to_datetime(full_df['Subscription Date'])
    full_df['Post Date'] = pd.to_datetime(full_df['Post Date'])
    full_df['Trs_status'] = full_df.apply(lambda x: x['Subscription Date'] < x['Reaction Date'] and x['Subscription Date'] <x['Post Date'], axis=1)

    user_status = full_df[['User', 'Subscription Date', 'Trs_status']].drop_duplicates()

    friendships1 = pd.merge(friends[['Friend 1', 'Friend 2']], user_status[['User','Trs_status','Subscription Date']],left_on =['Friend 1'], right_on='User',how='left')

    final_friends = pd.merge(friendships1[['Friend 1','Trs_status','Subscription Date', 'Friend 2']],user_status[['User','Trs_status','Subscription Date']], left_on=['Friend 2'], right_on = 'User', how ='left')
    final_friends['Friendship_request_date'] = final_friends.apply(lambda x: min(x['Subscription Date_x'], x['Subscription Date_y']) - np.random.randint(1, 30) * np.timedelta64(1, 'D'), axis=1)
    friends_table_finalized = final_friends[(final_friends['Trs_status_x'] == True) & (final_friends['Trs_status_y'] == True)]
    friends_table_finalized['Request status'] = np.random.randint(0, 2, size= len(friends_table_finalized))
    friends_table_finalized_not_valid = final_friends[(final_friends['Trs_status_x'] == False) & (final_friends['Trs_status_y'] == False)]
    return friends_table_finalized, friends_table_finalized_not_valid

def generate_friends_data(output_path):
    friends_table_finalized, friends_table_finalized_not_valid = transform_friends_data()
    os.makedirs(output_path,exist_ok=True)
    output_file_path = os.path.join(output_path, f'friends_table_final.csv')
    friends_table_finalized.to_csv(output_file_path,index= True)
    output_file_path_not_valid= os.path.join(output_path, f'friends_table_not_valid.csv')
    friends_table_finalized_not_valid.to_csv(output_file_path_not_valid,index=True)

generate_friends_data(output_directory)