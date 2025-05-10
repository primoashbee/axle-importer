import csv
import os
from  helpers_vehicle import *
from import_vehicles import *
from multiprocessing import Pool
from tqdm import tqdm


def process_row(row):
    
    if(row['leadID'] == None or row['leadID'] == "NULL"):
        print("skip")
        return False
    
    lead_id = getRelatedId("leads","migration_source_id", row['leadID'])

    if(lead_id == None ):
        print(f"No Lead found")
        return False
    
    vehicle_id = get_vehicle_by_vin(row.get('VIN'))
    if vehicle_id is None:
        vehicle_id = create_vehicle_from_lead_vehicle_row(row)

    if(vehicle_id == None):
       return False
    
    created_at = time_es_to_utc(row['updatedAt']).strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_es_to_utc(row['updatedAt']).strftime('%Y-%m-%d %H:%M:%S')

    lead_vehicle_data = {
        "lead_id": lead_id,
        "vehicle_id": vehicle_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "event_title": "lead-vehicle-created",
        "event":{
            "vehicle_id": vehicle_id,
            "lead_id": lead_id,
            "created_at": created_at,
            "updated_at": updated_at
        }
    }
    lead_vehicle_id = create_lead_vehicle(lead_vehicle_data)
    print(lead_vehicle_id)
    return lead_vehicle_id > 0
def create_vehicle_from_lead_vehicle_row(row):

    created_at = time_es_to_utc(row['updatedAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_es_to_utc(row['updatedAt'])

    make_id = get_or_create_make(row['make'],created_at,updated_at)
    model_id = get_or_create_model(row['model'], make_id,created_at,updated_at)
    type_id = get_or_create_type(row.get('classification'),created_at,updated_at)
    data = {
        "model_id": model_id,
        "make_id": make_id,
        "type_id": type_id,
        "vin": row.get("VIN"),
        "stock_no": row.get("stockNumber"),
        "invoice": row.get("invoice"),
        "net_cost": row.get("netCost"),
        "hard_pack": row.get("hardPack"),
        "pack": row.get("pack"),
        "retail_value": row.get("retailValue"),
        "year":row.get("year"),
        "state":row.get("state"),
        "color": row.get("color"),
        "body_style": row.get("bodyStyle"),
        "migration_source_id": None,
        "created_at": created_at,
        "updated_at": updated_at
    }
    # print(data)
    return create_vehicle(data)

def read_csv():
    source_csv = "may 2025 exports/leadinterests_may_2025_view.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        # for row in reader:
        #     res = process_row(row)
        #     if(res): 
        #         i = i + 1
        #     if i > 5:
        #         break
        with Pool(processes=20) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()        
            # result =pool.map(process_row, reader)            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')



def create_lead_vehicle(data):
    sql = """
    INSERT INTO lead_vehicles(
        lead_id,
        vehicle_id,
        created_at,
        updated_at
    )
    VALUES(
        %s,
        %s,
        %s,
        %s
    )
    RETURNING ID
    """
    cursor.execute(sql, [data['lead_id'], data['vehicle_id'],data['created_at'],data['updated_at']])
    id = cursor.fetchone()
    data["event"]["id"] = id[0]
    # print(f"data {data}")
    createEventLog("lead", data['lead_id'], data['event_title'], data['event'], data['created_at'])

    return id[0]

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    print('Done')

