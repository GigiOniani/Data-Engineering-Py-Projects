from utils import convert_timestamp, transform_friends_data, generate_friends_data
from utils import check_folder_exists, check_folder_size ,read_csv_files,transfer_files_to_datadir , read_file_log_lost, list_top_ten_name, partition_by_user_post, list_five_people_post_reacts
from utils import friends,posts,reactions,users
from utils import time, folder_path , output_directory, partitioning_directory

def main():

    start_time = time.time()

    check_folder_exists(folder_path)
    check_folder_size(folder_path)
    read_csv_files(folder_path)
    read_file_log_lost(friends)

    transfer_files_to_datadir(folder_path, output_directory)
    partition_by_user_post(posts, partitioning_directory)

    list_top_ten_name(users,10)
    list_five_people_post_reacts(posts,reactions)

    end_time = time.time()

    print(f'time taken {round(end_time - start_time,2)}')



if __name__ == '__main__':
    main()

