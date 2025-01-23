import csv
import os
from  helpers import *
from multiprocessing import Pool
import sys
import uuid
import json

csv.field_size_limit(sys.maxsize)

def process_row(row):
    
    row_type = row['type']
    if (row_type == 'mail' and getRelatedId('email_logs','migration_source_id',row['messageID']) != None):
        print(f"Skipping.. {row['messageID']}")
        return False
    recipient = row['recipient']
    if( recipient == 'lead' and row_type == 'sms'):
        return False
    
    if( recipient == 'lead' and row_type == 'mail'):
        return create_lead_email(row)
    
    if( recipient == 'customer' and row_type == 'mail'):
        return create_customer_email(row)
    
    if( recipient == 'customer' and row_type == 'text'):
        return create_customer_sms(row)
        



def create_lead_email(row):
    attendant = get_attendant(row['recipient'], row['recipientID'])
    sentBy = get_user_id_by_email(row['from'])
    if sentBy == None:
        return False
    if attendant['lead_id'] == None:
        return False
    
    emailLogObject = {
        "user_id": sentBy,
        "subject": row['subject'],
        "body": row["body"],
        "bound_type": 'OUTBOUND',
        "created_at": row["createdAt"],
        "updated_at": row["updatedAt"],
        "lead_id": attendant['lead_id'],
        "migration_source_id": row['messageID'],

    }
    insert_query = """
    INSERT INTO email_logs (
        user_id, 
        subject, 
        body, 
        bound_type, 
        created_at, 
        updated_at,
        migration_source_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s
    )
    
    returning id
    """


    # insert_query = """
    # WITH pl as (
    #     INSERT INTO email_logs (
    #         user_id, 
    #         subject, 
    #         body, 
    #         bound_type, 
    #         created_at, 
    #         updated_at
    #     ) VALUES (
    #         %s, %s, %s, %s, %s, %s
    #     )
        
    #     returning id, created_at, updated_at
    # )

    # INSERT INTO lead_email_logs (
    #     email_log_id,
    #     lead_id,
    #     created_at,
    #     updated_at
    # ) VALUES (
    #     (SELECT id FROM pl),
    #     %s,
    #     (SELECT created_at FROM pl),
    #     (SELECT updated_at FROM pl)
    # )

    # SELECT 
    #     %s,
    #     pl.id
    #     %s,
    #     pl.created_at,
    #     pl.updated_at
    # FROM pl
    # returning id
    # """
    

    cursor.execute(insert_query, (
        emailLogObject['user_id'],
        emailLogObject['subject'],
        emailLogObject['body'],
        emailLogObject['bound_type'],
        emailLogObject['created_at'],
        emailLogObject['updated_at'],
        emailLogObject['migration_source_id'],
    ))
    conn.commit()
    emailLogId = cursor.fetchone()[0]

    leadEmailObject = {
        "email_log_id": emailLogId,
        "lead_id": attendant['lead_id'],
        "created_at": emailLogObject['created_at'],
        "updated_at": emailLogObject['updated_at'],
    }

    insert_query = """
    INSERT INTO lead_email_logs (
        email_log_id,
        lead_id,
        created_at,
        updated_at
    ) VALUES (
        %s,
        %s,
        %s,
        %s
    )
    """

    cursor.execute(insert_query, (
        leadEmailObject['email_log_id'],
        leadEmailObject['lead_id'],
        leadEmailObject['created_at'],
        leadEmailObject['updated_at'],
    ))
    conn.commit()

    logData = {
        "from_id": sentBy,
        "from_name": get_username(sentBy),
        "to_id": attendant['lead_id'],
        "to": row['to'],
        "subject": row['subject'],
        "body": row['body'],
        "log_type": "Lead"
    }

    createEventLog(attendant['attendant_type'],attendant['attendant_id'], 'mail_sent', logData, emailLogObject['created_at'])
    print(f"Imported {row['messageID']}")
    return True
    # emailLogRecipient = {
    #     "email_log_id": data["email_log_id"],
    #     "email": data["email"],
    #     "name": data["name"],
    # }

    # leadEmailLogObject = {
    #     "from_id": data["from_id"],
    #     "from_name": data["from_name"],
    #     "to_id": data["to_id"],
    #     "to_name": data["to_name"],
    #     "subject": data["subject"],
    #     "body": data["body"],
    #     "log_type": data["log_type"],
    # }

    # logData = {
    #     "event": "mail_sent",
    #     "event_data": {
    #         "from_id": data["from_id"],
    #         "from_name": data["from_name"],
    #         "to_id": data["to_id"],
    #         "to": data["to_name"],
    #         "subject": data["subject"],
    #         "body": data["body"],
    #         "log_type": "Lead"
    #     }
    # }

    # createEventLog(data['attendant']['attendant_type'],data['attendant']['attendant_id'], data['event_title'], data['event'], data['created_at'])
def create_customer_email(row):
    attendant = get_attendant(row['recipient'], row['recipientID'])
    sentBy = get_user_id_by_email(row['from'])
    if sentBy == None:
        return False
    if attendant['customer_id'] == None:
        return False
    
    emailLogObject = {
        "user_id": sentBy,
        "subject": row['subject'],
        "body": row["body"],
        "bound_type": 'OUTBOUND',
        "created_at": row["createdAt"],
        "updated_at": row["updatedAt"],
        "lead_id": attendant['lead_id'],
        "migration_source_id": row['messageID'],

    }
    insert_query = """
    INSERT INTO email_logs (
        user_id, 
        subject, 
        body, 
        bound_type, 
        created_at, 
        updated_at,
        migration_source_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s
    )
    
    returning id
    """


    # insert_query = """
    # WITH pl as (
    #     INSERT INTO email_logs (
    #         user_id, 
    #         subject, 
    #         body, 
    #         bound_type, 
    #         created_at, 
    #         updated_at
    #     ) VALUES (
    #         %s, %s, %s, %s, %s, %s
    #     )
        
    #     returning id, created_at, updated_at
    # )

    # INSERT INTO lead_email_logs (
    #     email_log_id,
    #     lead_id,
    #     created_at,
    #     updated_at
    # ) VALUES (
    #     (SELECT id FROM pl),
    #     %s,
    #     (SELECT created_at FROM pl),
    #     (SELECT updated_at FROM pl)
    # )

    # SELECT 
    #     %s,
    #     pl.id
    #     %s,
    #     pl.created_at,
    #     pl.updated_at
    # FROM pl
    # returning id
    # """
    

    cursor.execute(insert_query, (
        emailLogObject['user_id'],
        emailLogObject['subject'],
        emailLogObject['body'],
        emailLogObject['bound_type'],
        emailLogObject['created_at'],
        emailLogObject['updated_at'],
        emailLogObject['migration_source_id'],
    ))
    conn.commit()
    emailLogId = cursor.fetchone()[0]

    customerEmailObject = {
        "email_log_id": emailLogId,
        "customer_id": attendant['customer_id'],
        "created_at": emailLogObject['created_at'],
        "updated_at": emailLogObject['updated_at'],
    }

    insert_query = """
    INSERT INTO customer_email_logs (
        email_log_id,
        customer_id,
        created_at,
        updated_at
    ) VALUES (
        %s,
        %s,
        %s,
        %s
    )
    """

    cursor.execute(insert_query, (
        customerEmailObject['email_log_id'],
        customerEmailObject['customer_id'],
        customerEmailObject['created_at'],
        customerEmailObject['updated_at'],
    ))
    conn.commit()

    logData = {
        "from_id": sentBy,
        "from_name": get_username(sentBy),
        "to_id": attendant['lead_id'],
        "to": row['to'],
        "subject": row['subject'],
        "body": row['body'],
        "log_type": "Customer"
    }

    createEventLog(attendant['attendant_type'],attendant['attendant_id'], 'mail_sent', logData, emailLogObject['created_at'])
    print(f"Imported {row['messageID']}")
    return True
    # emailLogRecipient = {
    #     "email_log_id": data["email_log_id"],
    #     "email": data["email"],
    #     "name": data["name"],
    # }

    # leadEmailLogObject = {
    #     "from_id": data["from_id"],
    #     "from_name": data["from_name"],
    #     "to_id": data["to_id"],
    #     "to_name": data["to_name"],
    #     "subject": data["subject"],
    #     "body": data["body"],
    #     "log_type": data["log_type"],
    # }

    # logData = {
    #     "event": "mail_sent",
    #     "event_data": {
    #         "from_id": data["from_id"],
    #         "from_name": data["from_name"],
    #         "to_id": data["to_id"],
    #         "to": data["to_name"],
    #         "subject": data["subject"],
    #         "body": data["body"],
    #         "log_type": "Lead"
    #     }
    # }

    # createEventLog(data['attendant']['attendant_type'],data['attendant']['attendant_id'], data['event_title'], data['event'], data['created_at'])

def create_customer_sms(row):
    sentBy = get_user_id_by_email(row['from'])

    if(sentBy == None):
        sentBy = getRelatedId('users','personal_phone_number',row['from']) or 2


    phoneNumberSmsLogObject = { 
        "from": add_dashes(row['from']),
        "to": add_dashes(row['to']),
        "content": row['body'],
        "is_inbound": False,
        "provider_message_id": str(uuid.uuid4()),
        "provider_response": json.dumps(row),
        "occured_at": row['createdAt'],
        "created_at": row['createdAt'],
        "updated_at": row['updatedAt'],
        "status" : row['status'],
        "sent_by": sentBy
    }

    print(phoneNumberSmsLogObject)

def read_csv():
    source_csv = "files/messages.csv"
    source_csv = "files/messages-customers.csv"
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

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    print('Done')