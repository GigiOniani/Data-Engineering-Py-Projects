import os
from utils.generate_data import create_users, create_transactions, uids_all
from utils.utils import check_file_exists, check_file_size, access_file , get_transaction_products_by_user_id, get_purchase_details, get_transaction_number_for_products

def main():
    users_file = 'data/users_list'
    trs_file = 'data/transactions'
    create_users(users_file)
    create_transactions(trs_file,uids_all)
    check_file_exists(users_file)
    check_file_exists(trs_file)
    check_file_size(users_file)
    check_file_size(trs_file)
    content = access_file(trs_file)
    get_transaction_products_by_user_id(content, 469164 )
    get_purchase_details(469164)
    get_transaction_number_for_products(trs_file)

if __name__ == '__main__':
    main()

