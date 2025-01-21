import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from helpers import createEventLog, cursor, conn, getRelatedId, add_dashes
import os
from multiprocessing import Pool

def execute():
    source_csv = "files/leads.csv"
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
            is_internet = True if row['sourceType'] == 'Internet' else False
            leadSourceId = getLeadSourceId(row['sourceName'], is_internet);
            leadStatusID = getLeadStatus(row['status']);

            if(leadSourceId == None or leadStatusID == None):
                print(f"Skipping.. {row['leadID']}")
                return False;
            assigneeId = None
            if row['agentID']:
                assigneeId = getUserId(row['agentID'], row)

            leadData = {
                "first_name": row['firstName'],
                "last_name": row['lastName'],
                "phone_number": add_dashes(row["phone"]),
                "email": row['email'],
                "lead_source_id": leadSourceId,
                "lead_status_id": leadStatusID,
                "middle_intial": row['middleInitial'],
                "migration_source_id": row['leadID'],
                "assignee_id": assigneeId,
                "created_at": row["createdAt"],
                "updated_at": row["updatedAt"],
                "rowData": row
            }
            
            getLeadId(row['leadID'], leadData)


    print("Done")
    # conn.commit()
    # cursor.close()
    # conn.close()
        
def process_row(row):
    
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    migId = getRelatedId("phone_number_call_logs", "migration_source_id", row['leadID'])
    if(migId):
        print(f"Skipping.. {row['leadID']}")
        return False
    is_internet = True if row['sourceType'] == 'Internet' else False
    
    leadSourceId = getLeadSourceId(row['sourceName'], is_internet);
    leadStatusID = getLeadStatus(row['status']);

    if(leadSourceId == None or leadStatusID == None):
        print(f"Skipping.. {row['leadID']}")
        return False;
    assigneeId = None
    if row['agentID']:
        assigneeId = getUserId(row['agentID'], row)

    leadData = {
        "first_name": row['firstName'],
        "last_name": row['lastName'],
        "phone_number": add_dashes(row["phone"]),
        "email": row['email'],
        "lead_source_id": leadSourceId,
        "lead_status_id": leadStatusID,
        "middle_intial": row['middleInitial'],
        "migration_source_id": row['leadID'],
        "assignee_id": assigneeId,
        "created_at": row["createdAt"],
        "updated_at": row["updatedAt"],
        "rowData": row
    }
    
    getLeadId(row['leadID'], leadData)
    print(f"Imported {row['leadID']}")
    return True
def getLeadSourceId(source, is_internet):
    query = "SELECT id FROM lead_sources WHERE name = %s"
    cursor.execute(query, (source,))
    result = cursor.fetchone()
    return result[0] if result else createLeadSource(source,is_internet)

def createLeadSource(source, is_internet):
    insert_query = """
    INSERT INTO lead_sources (
        name, website, is_internet
    ) VALUES (
        %s, %s, %s
    ) RETURNING id
    """
    cursor.execute(insert_query, (
        source, source, is_internet
    ))
    insert_id = cursor.fetchone()[0]
    conn.commit()

    return insert_id

def getLeadStatus(status):
    query = "SELECT id FROM lead_statuses WHERE lower(name) = %s"
    cursor.execute(query, (status.lower(),))
    result = cursor.fetchone()

    return result[0] if result else createLeadStatus(status)

def createLeadStatus(status):
    
    insert_query = "INSERT INTO lead_statuses name VALUES (%s)"
    cursor.execute(insert_query, ((status),))  # Note the comma after `status` to make it a tuple
    conn.commit()
    return cursor.lastrowid

def getUserId(migration_source_id, data):
    query = "SELECT id FROM users WHERE migration_source_id = %s"
    cursor.execute(query, (migration_source_id,))
    result = cursor.fetchone()
    return result[0] if result else createUser(data)

def createUser(data):
    insert_query = """
    INSERT INTO users (
        name, email, password, first_name, last_name, migration_source_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s
    )
    """
    cursor.execute(insert_query, (
        data["agent_name"], data["agent_email"], "$2y$12$KWeeoypM9oYWJfiqqp9PMeNEToMzsaZry3iqFlhHqYipOS5rVWisO",
        data["agent_firstName"], data["agent_lastName"], data["agentID"]
    ))

    conn.commit()
    return cursor.lastrowid

def createLead(data):
    insert_query = """
    INSERT INTO leads (
        first_name, last_name, phone_number, email, 
        provider_payload, lead_source_id, status_id, assignee_id, 
        middle_initial, recent_activity, migration_source_id, created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    """

    cursor.execute(insert_query, (
        data["first_name"], data["last_name"], data["phone_number"], data["email"],
        json.dumps(data["rowData"]), data["lead_source_id"], data["lead_status_id"], data["assignee_id"],
        data["middle_intial"], json.dumps({}), data["migration_source_id"], data["created_at"], data["updated_at"]
    ))
    conn.commit()
    return cursor.lastrowid


    
def getLeadId(migration_source_id, data):
    query = "SELECT id FROM leads WHERE migration_source_id = %s"
    cursor.execute(query, (migration_source_id,))
    result = cursor.fetchone()
    return result[0] if result else createLead(data)


def read_csv():
    source_csv = "files/leads.csv"
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
        


# execute();