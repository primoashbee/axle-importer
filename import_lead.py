import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from helpers import createEventLog, cursor, conn, getRelatedId, add_dashes


def execute():
    source_csv = "files/lead_view_no_info.csv"
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
            row = {
                    'parentID': '914', 'leadID': '914', 'firstName': 'Test', 'lastName': 'ref:_00D30WKx._50033yNwr9:ref', 
                    'middleInitial': 'L', 'name': 'TEST L REF:_00D30WKX._50033YNWR9:REF', 'phone': '8885290281', 
                    'email': 'testleads@cars.com', 'streetAddress': None, 'city': None, 'province': None, 
                    'postalCode': None, 'address': '', 'sourceName': 'Cars.com', 'sourceType': 'Internet', 
                    'check_for_duplicate': '0', 'latestInterestID': None, 'vin': None, 'stockNumber': None, 
                    'year': None, 'make': None, 'model': None, 'trim': None, 'condition': None, 'type': None, 
                    'listing': '', 'status': 'new', 'state': 'active', 'soldDate': None, 'latestSubmission': '2016-07-12 17:00:14', 
                    'createdAt': '2016-07-12 17:00:14', 'updatedAt': '2017-01-09 10:10:09', 'creatorID': '42', 
                    'agentID': None, 'agent_firstName': None, 'agent_lastName': None, 'agent_email': None, 
                    'agent_name': None, 'agent_role': None, 'customerID': None, 'lastContact': None, 'nextContact': None, 
                    'Latitude': None, 'Longitude': None, 'lastActionTaken': None, 'lastActionResult': None, 
                    'campaignCheck': None, 'campaignCheckTime': None, 'originId': None, 'mergedByID': None, 
                    'mergedAt': None, 'teamID': None, 'dealershipID': '2'
                }
            print(row)
            exit()
            is_internet = True if row['sourceType'] == 'Internet' else False
            leadSourceId = getLeadSourceId(row['sourceName'], is_internet);
            leadStatusID = getLeadStatus(row['status']);

            if(leadSourceId == None or leadStatusID == None):
                exit();
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
    )
    """
    cursor.execute(insert_query, (
        source, source, is_internet
    ))
    conn.commit()
    return cursor.lastrowid

def getLeadStatus(status):
    query = "SELECT id FROM lead_statuses WHERE lower(name) = %s"
    cursor.execute(query, (status.lower(),))
    result = cursor.fetchone()

    return result[0] if result else createLeadStatus(status)

def createLeadStatus(status):
    
    insert_query = "INSERT INTO lead_statuses name VALUES (%s)"
    print(cursor.mogrify(insert_query, (status,)).decode('utf-8'))
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

execute();