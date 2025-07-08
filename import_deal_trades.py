import csv
import os
from  helpers import *
from multiprocessing import Pool
from tqdm import tqdm
import logging
import functools
from import_vehicles import create_vehicle
from helpers_vehicle import get_or_create_make, get_or_create_type, get_or_create_model
def process_row(row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    if(row['dealID'] is None or row['dealID'] == ''):
        log(f"Skipping row with dealID {row['dealID']} due to missing or invalid data.")
        return False
    deal_id = getRelatedId('deals', 'migration_source_id', row['dealID'])
    if(deal_id is None):
        log(f"Deal not found for ID: {row['dealID']}. Trade {row["tradeID"]}")
        return False
    vehicle_id = getRelatedId('vehicles', 'vin', row['VIN'])
    if vehicle_id is None:
        data = formatVehicleData(row)
        vehicle_id = create_vehicle(data)

    deal_trade = {
        "deal_id": deal_id,
        "vehicle_id": vehicle_id,
        "migration_source_id": row['tradeID'],
        "vin": row['VIN'],
        "actual_cash_value": row['actualCashValue'],
        "payoff_amount": row['payOffAmount'] or 0,
        "trade_allowance": row['tradeAllowance'],
        "created_at": row['createdAt'],
        "updated_at": row['updatedAt'],
        "migration_source_id": row['tradeID'],
        "provider_response":json.dumps(row)
    }

    
    # deal_trade_vehicle_id = getRelatedId('deal_trade_vehicles', 'migration_source_id', row['tradeID'])
    # if(deal_trade_vehicle_id is not None):
    return create_trade_vehicle(deal_trade)
    
    return False

def create_trade_vehicle(deal_trade):
    insert_query = """
    INSERT INTO deal_trade_vehicles (deal_id, vehicle_id, trade_allowance, actual_cash_value, payoff_amount, created_at, updated_at, migration_source_id, provider_response)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """

    cursor.execute(insert_query, (
        deal_trade['deal_id'],
        deal_trade['vehicle_id'],
        deal_trade['trade_allowance'],
        deal_trade['actual_cash_value'],
        deal_trade['payoff_amount'],
        deal_trade['created_at'],
        deal_trade['updated_at'],
        deal_trade['migration_source_id'],
        deal_trade['provider_response']
    ))
    conn.commit()
    deal_trade_id = cursor.fetchone()[0]
    return bool(deal_trade_id)

def formatVehicleData(row):

    created_at = row['createdAt']
    updated_at = row['updatedAt']
    make_id = get_or_create_make(row['make'],created_at,updated_at)
    model_id = get_or_create_model(row['model'], make_id,created_at,updated_at)
    type_id = get_or_create_type('other',created_at,updated_at)
    data = {
        "model_id": model_id,
        "make_id": make_id,
        "type_id": type_id,
        "vin": row["VIN"],
        "stock_no": None,
        "invoice": None,
        "net_cost": None,
        "hard_pack": None,
        "pack": None,
        "retail_value": None,
        "year":row["year"],
        "state":None,
        "color": row["color"],
        "body_style":row["bodyStyle"],
        "migration_source_id": None,
        "created_at": created_at,
        "updated_at": updated_at,
        "provider_response": json.dumps(row)
    }

    return data

def get_source_csv():
    return "files/trades.csv"

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