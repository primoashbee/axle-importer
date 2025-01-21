import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from helpers import createEventLog, getRelatedId, add_dashes, cursor, conn, validate_date, get_username, get_attendant
import uuid
import os
from multiprocessing import Pool

def process_row(row):
    return create_phone_call(row)

# def getPhoneCallId(migration_source_id, data):
#     query = "SELECT id FROM phone_number_call_logs WHERE provider_response_id = %s"
#     cursor.execute(query, (migration_source_id,))
#     result = cursor.fetchone()
#     return result[0] if result else createPhoneCall(data)

# def createPhoneCall(data):
#     # Define the insert query
#     insert_query = """
#     INSERT INTO phone_number_call_logs (
#         lead_id, customer_id, caller_user_id, "from", "to",
#         provider_response, provider_response_id, direction,
#         status, end_call_status, created_at, updated_at
#     ) VALUES (
#         %s, %s, %s, %s, %s,
#         %s, %s, %s, %s, %s,
#         %s, %s
#     )
#     returning id
#     """

#     # Pass dictionary values as a tuple
#     cursor.execute(insert_query, (
#         data["lead_id"],
#         data["customer_id"],
#         data["caller_user_id"],
#         data["from_number"],
#         data["to_number"],
#         json.dumps(data["provider_response"]),
#         data["migration_source_id"],
#         data["direction"],
#         data["status"],
#         data["end_call_status"],
#         data["created_at"],
#         data["updated_at"],
#     ))

#     conn.commit()
#     return cursor.lastrowid
# # # Example usage
# # source_csv = "files/phone_call.csv"  # Path to your source CSV file
# # db_config = {
# #     "dbname": "prod_v3_backup",
# #     "user": "postgres",
# #     "password": "root",
# #     "host": "localhost",
# #     "port": 5432,
# # }
# # import_data_to_pg(source_csv, db_config)
# # # End of example usage

def create_phone_call(row):
    migId = getRelatedId("phone_number_call_logs", "migration_source_id", row['id'])
    # if(migId == None):
    #     print(f"Task with migration source id {row["id"]} exists. Skipping.")
    #     return False
    
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    
    callerUserId =  getRelatedId("users", "migration_source_id", row['hostID'])
    callerFrom = "+13017533306"
    callerTo = add_dashes(row['phone'])
    createdAt = validate_date(row['createdAt']) or None
    callerName = get_username(callerUserId)
    disposition = getEndCallStatus(row['description'])
    phoneCallObject = {
        "caller_user_id" : callerUserId,
        "from" : callerFrom,
        "to" :callerTo,
        "lead_id": attendant['lead_id'],
        "customer_id": attendant['customer_id'],
        "provider_response": json.dumps(row),
        "provider_response_id":  str(uuid.uuid4()),
        "direction": "outbound",
        "status": "completed",
        "start" : None,
        "end" : None,
        "end_call_status": disposition,
        "migration_source_id": row['id'],
        "created_at": createdAt,
        "updated_at": createdAt,
        "event" : 'phone-call',
        "eventData" : {
            "call_status" : "completed",
            "caller_name" : callerName,
            "caller_id" : callerUserId,
            "disposition": disposition
        }
    }

    insert_query = """
        INSERT INTO phone_number_call_logs (
            caller_user_id, 
            lead_id, 
            customer_id, 
            "from", 
            "to", 
            provider_response, 
            provider_response_id, 
            direction, 
            status, 
            start, 
            "end", 
            created_at, 
            updated_at, 
            end_call_status, 
            migration_source_id
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) returning id
    """

    cursor.execute(insert_query, (
        phoneCallObject['caller_user_id'],
        phoneCallObject['lead_id'],
        phoneCallObject['customer_id'],
        phoneCallObject['from'],
        phoneCallObject['to'],
        phoneCallObject['provider_response'],
        phoneCallObject['provider_response_id'],
        phoneCallObject['direction'],
        phoneCallObject['status'],
        phoneCallObject['start'],
        phoneCallObject['end'],
        phoneCallObject['created_at'],
        phoneCallObject['updated_at'],
        phoneCallObject['end_call_status'],
        phoneCallObject['migration_source_id'],
    ))
    conn.commit()
    createEventLog(attendant['attendant_type'], attendant['attendant_id'], phoneCallObject['event'], phoneCallObject['eventData'], createdAt)

def getEndCallStatus(callStatus):
    """
    No Answer - Left voicemail
    No Answer - Hung up
    Answered - Schedule Appointment
    Answered - More

    const ANSWERED_SCHEDULE_APPOINTMENT = 'answered_scheduled_appointment';
    const ANSWERED_MORE = 'answered_more';
    const NO_ANSWER = 'no_answer_hung_up';
    const NO_ANSWER_LEFT_VOICEMAIL = 'no_answer_left_voicemail';
    const CALL_NOT_PLACED = 'call_wasnt_placed';
    """
    match callStatus:
        case "No Answer - Left voicemail":
            return "no_answer_left_voicemail"
    
        case "No Answer - Hung up":
            return "no_answer_hung_up"

        case "Answered - Schedule Appointment":
            return "answered_scheduled_appointment"
        
        case "Answered - More":
            return "answered_more"
    


def read_csv():
    source_csv = "files/phone_call.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        with Pool(processes=10) as pool:
            result =pool.map(process_row, reader)
            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
        