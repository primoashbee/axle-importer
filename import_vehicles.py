import csv
import os
from  helpers_vehicle import *
from multiprocessing import Pool
from tqdm import tqdm

def process_row(row):
    # row = {key: (None if value == 'NULL' else value) for key, value in row.items()}

    mig_id = getRelatedId('vehicles','migration_source_id',row['vehicleID'])
    # if (mig_id != None):
    #     # print(f"Vehicle with migration source id {row["vehicleID"]} exists. Skipping.")
    #     return False

    created_at = time_es_to_utc(row['createdAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_es_to_utc(row['updatedAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    make_id = get_or_create_make(row['make'],created_at,updated_at)
        # Check if model is not null or empty
    # if not row['model']:
    #     print(f"Model is missing for vehicle ID {row['vehicleID']}. Skipping.")
    #     return False
    model_id = get_or_create_model(row['model'], make_id,created_at,updated_at)
    type_id = get_or_create_type(row['classification'],created_at,updated_at)

    data = {
        "model_id": model_id,
        "make_id": make_id,
        "type_id": type_id,
        "vin": row["VIN"],
        "stock_no": row["stockNumber"],
        "invoice": blank_to_none(row["invoice"]),
        "net_cost": blank_to_none(row["netCost"]),
        "hard_pack": blank_to_none(row["hardPack"]),
        "pack": blank_to_none(row["pack"]),
        "retail_value": blank_to_none(row["retailValue"]),
        "year":row["year"],
        "state":row["state"],
        "color": row["color"],
        "body_style":row["bodyStyle"],
        "migration_source_id": row['vehicleID'],
        "created_at": created_at,
        "updated_at": updated_at
    }
    create_vehicle(data)
    return True
def read_csv():
    source_csv = "files/vehicles.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        # for row in reader:
        #     process_row(row)
        #     i = i + 1
        #     if i > 10:
        #         break
        with Pool(processes=1) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()

            # result =pool.map(process_row, reader)            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')
            



def create_vehicle(data):
    data = {key: None if value == "NULL" else value for key, value in data.items()}
    sql = """
        WITH v AS (
            INSERT INTO vehicles (
                vin,
                year,
                make_id,
                model_id,
                created_at,
                updated_at,
                migration_source_id
            )
            VALUES(
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            RETURNING id, created_at, updated_at
        )
        INSERT INTO vehicle_additional_information(
            vehicle_id,
            created_at,
            updated_at,
            stock_no,
            state,
            color,
            body_style,
            hard_pack,
            invoice,
            net_cost,
            pack,
            retail_value
        )
        SELECT v.id, v.created_at, v.updated_at, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM v
        RETURNING id
    """
    params =[
            data["vin"],
            data["year"],
            data["make_id"],
            data["model_id"],
            data["created_at"],
            data["updated_at"],
            data["migration_source_id"],
            data["stock_no"],
            data["state"],
            data["color"],
            data["body_style"],
            data["hard_pack"],
            data["invoice"],
            data["net_cost"],
            data["pack"],
            data["retail_value"]
        ]
    try:
        # query = cursor.mogrify(sql, params).decode("utf-8")  # Decode the query to a string
        # print("Executing SQL Query:\n" + query)  # Beautify the output with a label and formatting
        # log_file = "sql_queries.log"
        # with open(log_file, "a") as f:
            # f.write(query + "\n")
        cursor.execute(sql, params)
        conn.commit()
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print("No vehicle record returned.")
            return None        
        # return vehicle_id
    except Exception as e:
        # print("error ah")
        # print(cursor.mogrify(sql, params))
        raise e




if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    print('Done')


