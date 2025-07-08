import csv
import os
from  helpers import *
from multiprocessing import Pool
import uuid
from tqdm import tqdm
from datetime import datetime


def process_row(row):

    # Lead has customer ID
    # events can have customer and lead id
    # but right now we only migrate customer
    # need a lead_customer_migration_table


    query = """
    SELECT * FROM lead_customer_linking_migration where lead_migration_id = %s and customer_migration_id = %s
    """
    cursor.execute(query, (row['leadID'], row['customerID']))
    linked = cursor.fetchone()
    if linked is not None:
        return False
    

    query = """
    INSERT INTO lead_customer_linking_migration (lead_migration_id, customer_migration_id, lead_migration_phone, lead_migration_email, customer_migration_phone, customer_migration_email)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING id
    """

    cursor.execute(query, (row['leadID'], row['customerID'], add_dashes(row['lead_phone']), row['lead_email'] , add_dashes(row['customer_phone']), row['customer_email']))
    conn.commit()
    result = cursor.fetchone()
    return bool(result[0])


    
def get_source_csv():
    return "files/customer_lead_migration_id_view.csv"

def read_csv():
    source_csv = get_source_csv()
    i = 0
    truncate()
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        with Pool(processes=10) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()
            
        succeeded = len([item for item in result if item == True])
        update_linking()
        print(f'{succeeded} of {len(result)} rows processed successfully')
            # result =pool.map(process_row, reader)
            
            # succeeded = len([item for item in result if item == True])
            # print(f'{succeeded} of {len(result)} imported')
    # createdAt = '2018-03-29 14:17:05'
    # print(validate_date(createdAt))
        
def truncate():
    query = """
    TRUNCATE TABLE lead_customer_linking_migration;
    """
    cursor.execute(query)
    conn.commit()
    print("Table truncated successfully")
def update_linking():
    query ="""
    UPDATE lead_customer_linking_migration ll
    SET
        lead_id = l.id,
        lead_email = l.email,
        lead_phone = l.phone_number
    FROM leads l
    WHERE l.migration_source_id = ll.lead_migration_id;

    """
    cursor.execute(query)
    conn.commit()
    query ="""
    UPDATE lead_customer_linking_migration ll
    SET
        customer_id = c.id,
        customer_email = c.email,
        customer_phone = ci.mobile_phone
    FROM customers c
    LEFT JOIN customer_contact_information ci ON ci.customer_id = c.id
    WHERE c.migration_source_id = ll.customer_migration_id;


    """
    cursor.execute(query)
    conn.commit()
    print("Linking updated successfully")
if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()