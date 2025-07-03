import csv
import os
from  helpers import *
from multiprocessing import Pool
from tqdm import tqdm
import logging
import functools

def process_row(row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    
    # $insertDeals[] = [
    #     'customer_id' : $customer?->id,
    #     'co_buyer_id' : null, // tbe fetch
    #     'vehicle_id' : $vehicle?->id,
    #     'salesperson_id' : $salesperson?->id,
    #     'finance_manager_id' : $financeManager?->id,
    #     'sales_manager_id' : $salesManager?->id,
    #     'status' : $row['status'],
    #     'retail_value' : $row['retail_value'],
    #     'calculated_gross' : $row['calculated_gross'],
    #     'sales_price' : $row['sales_price'],
    #     'accessories_cost' : $row['accessories_cost'],
    #     'deal_date' : $row['deal_date'],
    #     'rebates' : $row['rebates'],
    #     'provider_id' : $dealScanId,
    #     'payload' : json_encode($row),
    #     'migration_source_id' : $row['migration_source_id'],
    # ];
    
    # if(getRelatedId('deal_payments','migration_source_id',row['financingID']) != None):
    #     print(row['financingID'], 'already exists')
    #     return False
    if(row['dealID'] is None):
        return False
    dealId = getRelatedId('deals','migration_source_id',row['dealID'])
    if(dealId is None):
        return False

    
    dealPayment = {
        'deal_id' :  dealId,
        'down_payment' : row['downPayment'] or 0,
        'term' : row['installments'] ,
        'rate' : row['interestRate'] or 0,
        'payment' : row['monthlyPayment'],
        'amount_financed': row['amountFinanced'] or 0,
        'created_at': row['createdAt'],
        'updated_at': row['updatedAt'],
        'migration_source_id' : row['financingID'],
    }


    insert_query = """
        INSERT INTO deal_payments (deal_id, down_payment, term, rate, payment, amount_financed, created_at, updated_at, migration_source_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    values = (
        dealPayment['deal_id'],
        dealPayment['down_payment'],
        dealPayment['term'],
        dealPayment['rate'],
        dealPayment['payment'],
        dealPayment['amount_financed'],
        dealPayment['created_at'],
        dealPayment['updated_at'],
        dealPayment['migration_source_id']
    )

    cursor.execute(insert_query, values)
    conn.commit()
    result =  cursor.fetchone()
    return True

def get_source_csv():
    return "files/financings.csv"

def read_csv():
    source_csv = get_source_csv()
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        with Pool(processes=20) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()
            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')    
    read_csv()