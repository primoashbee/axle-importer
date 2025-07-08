import csv
import os
from  helpers_vehicle import *
from multiprocessing import Pool
from tqdm import tqdm

def process_row(row):
    
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}


    query = """
    UPDATE vehicles
    SET mileage = %s
    WHERE migration_source_id = %s

    """
    cursor.execute(query, (
        row['mileage'] if row['mileage'] else None,
        row['vehicleID']
    ))
    conn.commit()

    # query = """
    #     UPDATE vehicle_additional_information 
    #     SET 
    #     color = %s,
    #     trim = %s,
    #     hard_pack = %s,
    #     invoice = %s,
    #     net_cost = %s,
    #     pack = %s,
    #     package = %s,
    #     retail_value = %s
    #     where vehicle_id IN (
    #         SELECT id from vehicles WHERE vin = %s
    #     )
    # """
    # cursor.execute(query, (
    #     row['color'],
    #     row['trimLevel'],
    #     row['hardPack'] if row['hardPack'] else 0,
    #     row['invoice'] if row['invoice'] else 0,
    #     row['netCost'] if  row['netCost'] else 0,
    #     row['pack'] if row['pack'] else 0,
    #     row['package'] if row['package'] else None,
    #     row['retailValue'] if row['retailValue'] else 0,    
    #     row['VIN']
    # ))
    # conn.commit()

   
    return cursor.rowcount > 0
def read_csv():
    # source_csv = "files/vehicles.csv"
    source_csv = "files/deal_vehicles_view.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        # for row in reader:
        #     process_row(row)
        #     i = i + 1
        #     if i > 10:
        #         break
        with Pool(processes=20) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap_unordered(process_row, reader):
                    result.append(res)
                    pbar.update()

            # result =pool.map(process_row, reader)            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')
            





if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    if(mode == "prod"):
        proceed = input("You are about to connect to the prod database. Do you want to proceed? (y/n): ").strip().lower()
        if proceed != "y":
            print("Operation aborted by the user.")
            exit()
    read_csv()
    print('Done')


