from utils import convert_timestamp, transform_friends_data, generate_friends_data
from utils import check_folder_exists, check_folder_size ,read_csv_files,transfer_files_to_datadir , read_file_log_lost, list_top_ten_name, partition_by_user_post, list_five_people_post_reacts
from utils import friends,posts,reactions,users, friends_valid, time_decorator,time, folder_path , output_directory, partitioning_directory, plot_user_subgraph

@time_decorator
def main():
    check_folder_exists(folder_path)
    check_folder_size(folder_path)
    read_csv_files(folder_path)
    read_file_log_lost(friends)
    read_file_log_lost(posts)
    read_file_log_lost(reactions)
    read_file_log_lost(users)
    transfer_files_to_datadir(folder_path, output_directory)
    partition_by_user_post(posts, partitioning_directory)
    top_n_names = 10
    list_top_ten_name(users,top_n_names)
    list_five_people_post_reacts(posts,reactions)
    users_to_be_plotted = [11,13]
    plot_user_subgraph(users_to_be_plotted,friends_valid,'Friend 1', 'Friend 2')


if __name__ == '__main__':
    main()

