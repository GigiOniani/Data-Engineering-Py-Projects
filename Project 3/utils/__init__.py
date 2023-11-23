import numpy as np
import pandas as pd
import os
from collections import Counter
from datetime import datetime
import time

from utils.generate_friends_data import convert_timestamp, transform_friends_data, generate_friends_data
from utils.utils import check_folder_exists, check_folder_size, read_csv_files, transfer_files_to_datadir , read_file_log_lost, list_top_ten_name, partition_by_user_post, list_five_people_post_reacts

friends = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\friends_table.csv'
posts = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\posts_table.csv'
reactions = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\reactions_table.csv'
users = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\user_table.csv'

folder_path = 'C:\\Users\\User\\Desktop\\Assignment 1'
output_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data'
partitioning_directory = 'C:\\Users\\User\\PycharmProjects\\Exercising\\Assignment1\\data\\uid_post_partitioned'