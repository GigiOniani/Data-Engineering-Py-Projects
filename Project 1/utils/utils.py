import os

def check_file_exists(filename):
    """
    this function checks if file does exist in directory

    :param filename:
    :return:
    """
    return os.path.exists(filename)


def check_file_size(filename):
    """
    #this function is Checking file size, if size is zero it returns false
    :param filename:
    :return:
    """
    file_stats = os.stat(filename)
    if file_stats.st_size == 0:
        return False
    return True


def access_file(filename):
    """
    #Opening the file, Always using try/except methods on any file
    :param filename:
    :return:
    """
    try:
        with open(filename,'r') as f:
            content = f.readlines()
            return content
    except FileNotFoundError as error1:
        raise FileNotFoundError(f'File not found check{error1}')
    except IOError as error2:
        raise IOError(f'IO Error check {error2}')
    finally:
        print(f'{filename} file has read successfully!')

def get_transaction_products_by_user_id(content, user_id):
    """
    gives function allows use  to check each user purchased products by providing user_id
    :param user_id:
    :return:
    """
    prod_list = []
    for rows in access_file('data/transactions'):
        product_id,uid,product_name,transaction_id,transaction_date,store_id = rows.split(',')

        if uid == str(user_id):
            prod_list.append(product_name)
    return prod_list

def get_purchase_details(user_id):
    """
    this function tells detail information about customer's purchase
    :param user_id: 
    :return: 
    """
    user_data = {}
    with open('data/users_list','r') as file:
        next(file)
        rows = file.readlines()
        for row in rows:
            uid, name, age = row.strip().split(',')
            user_data['id'] = uid

    with open('data/transactions' , 'r') as f1:
        next(f1)
        rowss = f1.readlines()
        for row in rowss:
            product_id,users_id,product_name,transaction_id,transaction_date,store_id= row.strip().split(',')
            if users_id == str(user_id):
                print(f'Customer N{user_id} - {name}, has bought {product_name} on date:{transaction_date}')

def get_transaction_number_for_products(filename):
    """
    This function allows to see what number of transactions occured on specific products
    :param filename: 
    :return: 
    """
    from collections import defaultdict
    product_counts = defaultdict(int)
    with open('data/transactions' , 'r') as f1:
        next(f1)
        rowss = f1.readlines()
        for row in rowss:
            product_id,user_id,product_name,transaction_id,transaction_date,store_id = row.strip().split(',')
            product_counts[product_name] += 1

    for product,count in product_counts.items():
        print(f'{product}: {count} has transactions')



