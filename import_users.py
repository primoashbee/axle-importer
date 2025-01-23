import csv
import os
from  helpers import *
from multiprocessing import Pool
import uuid


def process_row(row):
    if getRelatedId('users','migration_source_id',row['userID']) != None:
        print(f"Skipping.. {row['userID']} - doing update")
        return update_user(row)
    
    userObject = {
        'first_name': row['firstName'],
        'last_name': row['lastName'],
        'name': row['firstName'] + ' ' + row['lastName'],
        'personal_phone_number': add_dashes(row['phone']),
        'email': row['email'],
        'password': '$2y$12$KWeeoypM9oYWJfiqqp9PMeNEToMzsaZry3iqFlhHqYipOS5rVWisO',
        'migration_source_id': row['userID'],
    }


    insert_query = """
        INSERT INTO users (first_name, last_name, name, personal_phone_number, email, password, migration_source_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        returning id
    """

    cursor.execute(insert_query, (
        userObject['first_name'], 
        userObject['last_name'], 
        userObject['name'], 
        userObject['personal_phone_number'], 
        userObject['email'], 
        userObject['password'], 
        userObject['migration_source_id']
    ))
    conn.commit();
    user_id = cursor.fetchone()[0]    

    match = row['role']
    if match == 'admin':
        role_id = getRelatedId('roles','name','admin')
    elif match == 'owner':
        role_id = getRelatedId('roles','name','user')
    elif match == 'fin_mgr':
        role_id = getRelatedId('roles','name','finance_manager')
    elif match == 'gen_sale_mgr':
        role_id = getRelatedId('roles','name','general_sale_manager')
    elif match == 'bdc_mgr':
        role_id = getRelatedId('roles','name','bdc_manager')
    elif match == 'sale_mgr':
        role_id = getRelatedId('roles','name','sales_manager')
    elif match == 'bdc_rep':
        role_id = getRelatedId('roles','name','bdc_rep')
    elif match == 'sale_rep':
        role_id = getRelatedId('roles','name','sales_rep')

    if(role_id == None):
        print(f'Role {row["role"]} does not exist')
        return False
    
    role_query = """
        INSERT INTO model_has_roles (role_id, model_type, model_id)
        VALUES (%s, %s, %s)
    """
    cursor.execute(role_query, (role_id, 'Domain\\Users\\Models\\User', user_id))
    conn.commit()

    return True

def update_user(row):
    userObject = {
        'first_name': row['firstName'],
        'last_name': row['lastName'],
        'name': row['firstName'] + ' ' + row['lastName'],
        'personal_phone_number': add_dashes(row['phone']),
        'email': row['email'],
        'password': '$2y$12$KWeeoypM9oYWJfiqqp9PMeNEToMzsaZry3iqFlhHqYipOS5rVWisO',
        'migration_source_id': row['userID'],
    }
    print(userObject['email']);
    print(userObject['personal_phone_number']);

    update_query = """
        UPDATE users SET 
        first_name = %s,
        last_name = %s,
        migration_source_id = %s, 
        personal_phone_number = %s WHERE 
        email = %s
    """

    cursor.execute(update_query, (
        userObject['first_name'], 
        userObject['last_name'], 
        userObject['migration_source_id'], 
        userObject['personal_phone_number'], 
        userObject['email']
    ))
    conn.commit();

    user_id = getRelatedId('users','migration_source_id',row['userID'])
    # exists = getRelatedId('roles','model_id',user_id)
    query = "SELECT * FROM model_has_roles WHERE model_id = %s"
    cursor.execute(query, (user_id,))
    
    result = cursor.fetchone()
    exists = result[0] if result else None
    role_id = None
    match = row['role']
    if match == 'admin':
        role_id = getRelatedId('roles','name','admin')
    elif match == 'owner':
        role_id = getRelatedId('roles','name','user')
    elif match == 'fin_mgr':
        role_id = getRelatedId('roles','name','finance_manager')
    elif match == 'gen_sale_mgr':
        role_id = getRelatedId('roles','name','general_sale_manager')
    elif match == 'bdc_mgr':
        role_id = getRelatedId('roles','name','bdc_manager')
    elif match == 'sale_mgr':
        role_id = getRelatedId('roles','name','sales_manager')
    elif match == 'bdc_rep':
        role_id = getRelatedId('roles','name','bdc_rep')
    elif match == 'sale_rep':
        role_id = getRelatedId('roles','name','sales_rep')

    if(role_id == None):
        print(f'Role {row["role"]} does not exist')
        return False
    
    if exists == None:
        role_query = """
            INSERT INTO model_has_roles (role_id, model_type, model_id)
            VALUES (%s, %s, %s)
        """
        cursor.execute(role_query, (role_id, 'Domain\\Users\\Models\\User', user_id))
        conn.commit()

    return True

def get_source_csv():
    return "files/users.csv"

def read_csv():
    source_csv = get_source_csv()
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        with Pool(processes=10) as pool:
            result =pool.map(process_row, reader)
            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()