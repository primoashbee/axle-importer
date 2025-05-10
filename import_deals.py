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

    # if no customer id, return false
    if(row['buyerID'] is None):
        return False
    
    # get customer id
    
 
    
    if(getRelatedId('deals','migration_source_id',row['dealID']) != None):
        return update_row(row)

    customerId = getRelatedId('customers','migration_source_id', row['buyerID'])
    salesRepId = getRelatedId('users','migration_source_id',row['saleRepID']) or None
    financeManagerId = getRelatedId('users','migration_source_id',row['financeManagerID']) or None
    salesManagerId = getRelatedId('users','migration_source_id',row['saleManagerID']) or None
    vehicleId = getRelatedId('vehicles','migration_source_id',row['vehicleID']) or None
    
    if(customerId is None):
        return False
    deal = {
        'customer_id' : customerId,
        'co_buyer_id' : None, # co buyer customer id
        'vehicle_id' : vehicleId,
        'salesperson_id' : salesManagerId,
        'finance_manager_id' : financeManagerId,    
        'sales_manager_id' : salesManagerId,
        'status' : row['status'],
        'retail_value' : row['retailValue'],
        'sales_price' : row['salePrice'],
        'accessories_cost' : row['accessories'],
        'deal_date' : row['createdAt'],
        'rebates' : row['amount'],
        'provider_id' : 2, # Fixed for deal scan
        'payload' : json.dumps(row),
        'migration_source_id' : row['dealID'],
        'created_at': row['createdAt'],
        'updated_at': row['updatedAt']
    }

    insert_query = """
        INSERT INTO deals (customer_id, co_buyer_id, vehicle_id, salesperson_id, finance_manager_id, sales_manager_id, status, retail_value, sales_price, accessories_cost, deal_date, rebates, provider_id, payload, migration_source_id, created_at, updated_at)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        deal['customer_id'],
        deal['co_buyer_id'],
        deal['vehicle_id'],
        deal['salesperson_id'],
        deal['finance_manager_id'],
        deal['sales_manager_id'],
        deal['status'],
        deal['retail_value'],
        deal['sales_price'],
        deal['accessories_cost'],
        deal['deal_date'],
        deal['rebates'],
        deal['provider_id'],
        deal['payload'],
        deal['migration_source_id'],
        deal['created_at'],
        deal['updated_at']
    )

    cursor.execute(insert_query, values)
    conn.commit()
    
    return True


def update_row(row):

    customerId = getRelatedId('customers','migration_source_id', row['buyerID'])
    salesRepId = getRelatedId('users','migration_source_id',row['saleRepID']) or None
    financeManagerId = getRelatedId('users','migration_source_id',row['financeManagerID']) or None
    salesManagerId = getRelatedId('users','migration_source_id',row['saleManagerID']) or None
    vehicleId = getRelatedId('vehicles','migration_source_id',row['vehicleID']) or None
    
    deal = {
        'customer_id' : customerId,
        'co_buyer_id' : None, # co buyer customer id
        'vehicle_id' : vehicleId,
        'salesperson_id' : salesManagerId,
        'finance_manager_id' : financeManagerId,    
        'sales_manager_id' : salesManagerId,
        'status' : row['status'],
        'retail_value' : row['retailValue'],
        'sales_price' : row['salePrice'],
        'accessories_cost' : row['accessories'],
        'deal_date' : row['createdAt'],
        'rebates' : row['amount'],
        'provider_id' : 2, # Fixed for deal scan
        'payload' : json.dumps(row),
        'migration_source_id' : row['dealID'],
        'created_at': row['createdAt'],
        'updated_at': row['updatedAt'],
        'migration_source_id': row['dealID']
    }

    update_query = """
        UPDATE deals SET customer_id = %s, co_buyer_id = %s, vehicle_id = %s, salesperson_id = %s, finance_manager_id = %s, sales_manager_id = %s, status = %s, retail_value = %s, sales_price = %s, accessories_cost = %s, deal_date = %s, rebates = %s, provider_id = %s, payload = %s, migration_source_id = %s, created_at = %s, updated_at = %s
        WHERE migration_source_id = %s
    """
    values = (
        deal['customer_id'],
        deal['co_buyer_id'],
        deal['vehicle_id'],
        deal['salesperson_id'],
        deal['finance_manager_id'],
        deal['sales_manager_id'],
        deal['status'],
        deal['retail_value'],
        deal['sales_price'],
        deal['accessories_cost'],
        deal['deal_date'],
        deal['rebates'],
        deal['provider_id'],
        deal['payload'],
        deal['migration_source_id'],
        deal['created_at'],
        deal['updated_at'],
        deal['migration_source_id']
    )

    cursor.execute(update_query, values)
    conn.commit()
    return True
def get_source_csv():
    return "files/deal_export_view.csv"

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