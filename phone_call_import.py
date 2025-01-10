import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from helpers import createEventLog, getRelatedId, add_dashes, cursor, conn

def import_data_to_pg(source_csv, db_config):
    """
    Import data from a CSV file and insert it into a PostgreSQL database.
    """
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            caller_user_id = getRelatedId("users", "migration_source_id", row["userID"], cursor)
            caller_user_id = caller_user_id or row["userID"]
            loggableType = "lead";
            loggableId = None;

            data = {
                "lead_id": None,
                "customer_id": None,
                "caller_user_id": caller_user_id,
                "from_number": "+13017533306",
                "to_number": add_dashes(row["phone"]),
                "provider_response": json.dumps({}),
                "provider_response_id": 111111,
                "direction": "outbound",
                "status": row["status"],
                "end_call_status": row["description"],
                "created_at": datetime.strptime(row["createdAt"], "%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.strptime(row["createdAt"], "%Y-%m-%d %H:%M:%S"),
                "rowData": row,
                "migration_source_id": row["id"]
            }

            if row["attendant_type"] == "lead":
                data["lead_id"] = getRelatedId("leads", "migration_source_id", row["attendant_id"], cursor)
                loggableType = "lead"
                loggableId = data["lead_id"]

            if row["attendant_type"] == "customer":
                data["customer_id"] = getRelatedId("customers", "migration_source_id", row["attendant_id"], cursor)
                loggableType = "customer"
                loggableId = data["customer_id"]

            getPhoneCallId(row["id"], data)

            caller_id = getRelatedId("users", "migration_source_id", row["userID"], cursor)
            logData = {
                'call_status' : row["status"],
                'caller_name' : row["dispatcher_name"],
                'caller_id' : caller_id,
                'disposition': row['description'],
                'disposition_text': row['description'],
            }
            
            if(loggableId == None):
                print(row)
                return;
            
            createEventLog(loggableType, loggableId, 'phone-call', logData, data["created_at"])
            print(f"Log data created for {loggableType} with ID {loggableId}.")


  
    conn.close()

    print("Data successfully imported into PostgreSQL.")


def getPhoneCallId(migration_source_id, data):
    query = "SELECT id FROM phone_number_call_logs WHERE provider_response_id = %s"
    cursor.execute(query, (migration_source_id,))
    result = cursor.fetchone()
    return result[0] if result else createPhoneCall(data)

def createPhoneCall(data):
    # Define the insert query
    insert_query = """
    INSERT INTO phone_number_call_logs (
        lead_id, customer_id, caller_user_id, "from", "to",
        provider_response, provider_response_id, direction,
        status, end_call_status, created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s
    )
    """

    # Pass dictionary values as a tuple
    cursor.execute(insert_query, (
        data["lead_id"],
        data["customer_id"],
        data["caller_user_id"],
        data["from_number"],
        data["to_number"],
        json.dumps(data["provider_response"]),
        data["migration_source_id"],
        data["direction"],
        data["status"],
        data["end_call_status"],
        data["created_at"],
        data["updated_at"],
    ))

    conn.commit()
    return cursor.lastrowid
# Example usage
source_csv = "files/phone_call.csv"  # Path to your source CSV file
db_config = {
    "dbname": "prod_v3_backup",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": 5432,
}
import_data_to_pg(source_csv, db_config)
