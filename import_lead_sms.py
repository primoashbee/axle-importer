import csv
import os
from  helpers import *
from multiprocessing import Pool

sms_from = "+13017533306"
sms_status = "sent"
sms_system_generated = False

def process_row(row):
    # print(row)

    if getRelatedId('lead_sms_logs','migration_source_id',row['messageID']) != None:
        return False

    created_at = time_pacific_to_utc(row["createdAt"]).strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_pacific_to_utc(row["createdAt"]).strftime('%Y-%m-%d %H:%M:%S')
    user_id = getRelatedId('users','migration_source_id',row['userID'])
    lead_id = getRelatedId('leads','migration_source_id',row['leadID'])
    if(user_id == None or lead_id == None):
        return False

    lead_details = get_lead_details(lead_id)

    data = {
        "id": row["messageID"],
        "to": row["to"],
        "created_at": created_at,
        "updated_at": updated_at,
        "content": row["body"],
        "user_id": user_id,
        "lead_id": lead_id,
        "event": "sms-sent",
        "event_data": {
            "from_name":"Claudia Portillo",
            "from_id": user_id,
            "to_id": lead_id,
            "to_name": row['lead_name'],
            "to_email": lead_details['data'][lead_details['fields'].index('email')],
            "message": row["body"]
        }
    }

    create_lead_sms(data)
    return True


def read_csv():
    source_csv = "files/lead_sent_text_view.csv"
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        # i = 0
        # for row in reader:
        #     res = process_row(row)
        #     if(res):
        #         i = i + 1
        #     if i > 0:
        #         break
        with Pool(processes=10) as pool:
            result =pool.map(process_row, reader)            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')

def create_lead_sms(data):
    sql = """
    WITH pl AS (
        INSERT INTO phone_number_sms_logs(
            "from",
            "to",
            content,
            is_inbound,
            status,
            sent_by,
            is_system_generated,
            created_at, 
            updated_at
        )
        VALUES( 
            %s,
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
    INSERT INTO lead_sms_logs (
        lead_id,
        phone_number_sms_log_id,
        migration_source_id,
        created_at,
        updated_at
    )
    SELECT 
        %s,
        pl.id,
        %s,
        pl.created_at,
        pl.updated_at
    FROM pl
    RETURNING id
    """

    params = [
        sms_from,
        data['to'],
        data['content'],
        False,
        sms_status,
        data["user_id"],
        sms_system_generated,
        data['created_at'],
        data['updated_at'],
        data['lead_id'],
        data['id']

    ]
    
    cursor.execute(sql, params)
    conn.commit()
    createEventLog("lead", data['lead_id'],data['event'],data['event_data'],data['created_at'])




if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    print('Done')