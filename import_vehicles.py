import csv
import os
from  helpers_vehicle import *
from multiprocessing import Pool
from tqdm import tqdm

def process_row(row):
    # row = {key: (None if value == 'NULL' else value) for key, value in row.items()}

    # if(row['vehicleID'] != '17176'):
    #     return False
    mig_id = getRelatedId('vehicles','migration_source_id',row['vehicleID'])
    created_at = time_pacific_to_utc(row['createdAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_pacific_to_utc(row['updatedAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    if (mig_id != None):
        make_id = get_or_create_make(row['make'],created_at,updated_at)
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
            "provider_response": json.dumps(row),
            "created_at": created_at,
            "updated_at": updated_at
        }

        return update_vehicle(data)
        return False


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
        "provider_response": json.dumps(row),
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
    # source_csv = "files/vehicles.csv"
    source_csv = "may 2025 exports/vehicles.csv"
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
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()

            # result =pool.map(process_row, reader)            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')
            



def create_vehicle(data):
    data = {key: None if value == "NULL" else value for key, value in data.items()}
    # sql = """
    #     WITH v AS (
    #         INSERT INTO vehicles (
    #             vin,
    #             year,
    #             make_id,
    #             model_id,
    #             created_at,
    #             updated_at,
    #             migration_source_id
    #         )
    #         VALUES(
    #             %s,
    #             %s,
    #             %s,
    #             %s,
    #             %s,
    #             %s,
    #             %s
    #         )
    #         RETURNING id, created_at, updated_at
    #     )
    #     INSERT INTO vehicle_additional_information(
    #         vehicle_id,
    #         created_at,
    #         updated_at,
    #         stock_no,
    #         state,
    #         color,
    #         body_style,
    #         hard_pack,
    #         invoice,
    #         net_cost,
    #         pack,
    #         retail_value
    #     )
    #     SELECT v.id, v.created_at, v.updated_at, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM v
    #     RETURNING id
    # """
    sql = """
        WITH v AS (
            INSERT INTO vehicles (
                vin,
                year,
                make_id,
                model_id,
                created_at,
                updated_at,
                provider_response,
                migration_source_id
            )
            VALUES (
                %s,
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
        INSERT INTO vehicle_additional_information (
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
        RETURNING vehicle_id
    """
    params =[
            data["vin"],
            data["year"],
            data["make_id"],
            data["model_id"],
            data["created_at"],
            data["updated_at"],
            data["provider_response"],
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


def update_vehicle(data):
    data = {key: None if value == "NULL" else value for key, value in data.items()}
    sql = """
        WITH updated_vehicle AS (
            UPDATE vehicles
            SET
                vin = %s,
                year = %s,
                make_id = %s,
                model_id = %s,
                provider_response = %s,
                updated_at = NOW()
            WHERE migration_source_id = %s
            RETURNING id, updated_at
        )
        UPDATE vehicle_additional_information
        SET
            stock_no = %s,
            state = %s,
            color = %s,
            body_style = %s,
            hard_pack = %s,
            invoice = %s,
            net_cost = %s,
            pack = %s,
            retail_value = %s,
            updated_at = (SELECT updated_at FROM updated_vehicle)
        WHERE vehicle_id = (SELECT id FROM updated_vehicle)
        RETURNING vehicle_id
    """

    params = [
        data["vin"],               # 1
        data["year"],              # 2
        data["make_id"],           # 3
        data["model_id"],          # 4
        data["provider_response"],  # 5
        data["migration_source_id"],  # 5
        data["stock_no"],          # 6
        data["state"],             # 7
        data["color"],             # 8
        data["body_style"],        # 9
        data["hard_pack"],         # 10
        data["invoice"],           # 11
        data["net_cost"],          # 12
        data["pack"],              # 13
        data["retail_value"]       # 14
    ]


    try:
        cursor.execute(sql, params)
        conn.commit()

        result = cursor.fetchone()
        if result:
            vehicle_id = result[0]
            # print(f"Updated vehicle with ID: {vehicle_id}")
            return True
        else:
            print("No vehicle record updated.")
            return None
    except Exception as e:
        conn.rollback()
        print(f"Error updating vehicle: {e}")
        return False    
    


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


