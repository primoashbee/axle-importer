import csv
import os
from  helpers import *
from multiprocessing import Pool
import uuid
from tqdm import tqdm

from import_customers_v2 import process_customer, formatCustomerObject


def process_row(row):
    dealId = getRelatedId('deals', 'migration_source_id', row['dealID'])
    if dealId is not None:
        cosignerId = getRelatedId('customers', 'migration_source_id', row['coBuyerID'])
        if cosignerId is None:
            customerObject = formatCustomerObject(row)
            if(customerObject is False):
                return False
            process_customer(customerObject, customerObject['crm_customer_id'])
            cosignerId = getRelatedId('customers', 'migration_source_id', row['coBuyerID'])
    else: 
        return False
    update_customer_to_co_buyer(cosignerId)
    update_deal_co_buyer(dealId, cosignerId)
    return True

def get_source_csv():
    return "files/cosigners_view.csv"

def update_customer_to_co_buyer(cosignerId):
    update_query = """
    UPDATE customers SET 
    is_co_buyer = TRUE
    WHERE 
    id = %s
    """
    cursor.execute(update_query, (cosignerId,))
    conn.commit()
    return True

def update_deal_co_buyer(dealId, cosignerId):
    update_query = """
    UPDATE deals SET 
    co_buyer_id = %s
    WHERE 
    id = %s
    """
    cursor.execute(update_query, (cosignerId, dealId))
    conn.commit()
    return True

def read_csv():
    source_csv = get_source_csv()
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))

        with Pool(processes=10) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()