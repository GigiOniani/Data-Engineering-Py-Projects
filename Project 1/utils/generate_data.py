import os
import random
from faker import Faker
import binascii

fake = Faker()
uids_all = []

user_list_entry =100  #1_000_000
transaction_list_entry=100 #10_000_000

def create_users(filename):
    try:
        with open(filename,'w') as file:
            file.writelines('uid,name, age\n')
            for i in range(user_list_entry):
                uid = fake.unique.random_int(min=1 , max=1500000)
                name = fake.name()
                age = random.randint(20,98)
                file.writelines(f"""{uid},{name},{age}\n""")
                uids_all.append(uid)

    except Exception:
        raise Exception('please check function!')


def create_transactions(filename,uid_list):
    try:
        with open(filename,'w') as file:
            file.writelines('product_id,user_id,product_name,transaction_id,transaction_date,store_id\n')
            for i in range(transaction_list_entry):
                product_id = binascii.b2a_hex(os.urandom(15))
                user_id= random.choice(uid_list)
                product_name = f"""Intel Core I{random.randint(1,100)}-{random.randint(1,12)}K Overclocked Pr:{fake.city()}"""
                transaction_id = binascii.b2a_hex(os.urandom(15))
                transaction_date = fake.date()
                store_id = random.randint(1,200)
                file.writelines(f"""{product_id},{user_id},{product_name},{transaction_id},{transaction_date},{store_id}\n""")

    except Exception:
        raise Exception('please check function!')

