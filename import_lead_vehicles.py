import csv
import os
from  helpers_vehicle import *
from multiprocessing import Pool



def process_row(row):
    if(row['leadID'] == None):
        return False
    lead_id = getRelatedId("leads","id", row['leadID'])
    vehicle_id = get_vehicle_id(row)
    if(lead_id == None or vehicle_id == None):
        print(f"no data found")
        return False
    created_at = time_es_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_es_to_utc(row['updatedAt']).strftime('%Y-%m-%d %H:%M:%S')

    create_lead_vehicle({
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
    })

    return True

def read_csv():
    source_csv = "files/leadinterests.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        # for row in reader:
        #     res = process_row(row)
        #     if(res):
        #         i = i + 1
        #     if i > 5:
        #         break
        with Pool(processes=10) as pool:
            result =pool.map(process_row, reader)            
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
    print(f"data {data}")
    createEventLog("lead", data['lead_id'], data['event_title'], data['event'], data['created_at'])

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    print('Done')

