import csv
import os
from  helpers import *
from multiprocessing import Pool
from tqdm import tqdm
import logging
import functools

def process_row(row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    
    vehicleId = getRelatedId('vehicles','migration_source_id', row['vehicleID'])
    if(vehicleId is None):  
        return False

    # map classification
    typeId = getRelatedId('vehicle_types','slug', row['classification'])
    update_query = """
        UPDATE vehicles set type_id = %s WHERE migration_source_id = %s
    """
    values = (
        typeId,
        row['vehicleID']
    )
    cursor.execute(update_query, values)
    conn.commit()
    
    return True

def get_source_csv():
    return "files/vehicles.csv"

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