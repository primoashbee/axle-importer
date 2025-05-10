import csv
import os
from  helpers import *
from multiprocessing import Pool
import sys
import uuid
import json
import logging
from tqdm import tqdm


csv.field_size_limit(sys.maxsize)

def process_row(row):
    
    row_type = row['type']
    # if (row_type == 'mail' and getRelatedId('email_logs','migration_source_id',row['messageID']) != None):
    # if (row_type == 'mail'):
    #     print(f"Skipping.. {row['messageID']}")
    #     return False
    
    # if (row_type == 'text' and getRelatedId('phone_number_sms_logs','migration_source_id',row['messageID']) != None):
    # if (row_type == 'text'):
    #     print(f"Skipping.. {row['messageID']}")
    #     return False
    
    recipient = row['recipient']
    
    # Done
    if( recipient == 'lead' and row_type == 'text'):
        return create_lead_sms(row)
    
    # Done
    if( recipient == 'lead' and row_type == 'mail'):
        return create_lead_email(row)
    
    if( recipient == 'customer' and row_type == 'mail'):
        return create_customer_email(row)
    
    if( recipient == 'customer' and row_type == 'text'):
        return create_customer_sms(row)
        

def create_lead_sms(row):
    sentBy = get_user_id_by_email(row['from'])
    userName = get_username(sentBy)
    leadId = getRelatedId('leads','migration_source_id',row['recipientID'])

    if(sentBy == None):
        sentBy = getRelatedId('users','personal_phone_number',row['from']) or 2
    if(leadId == None):
        return False

    migId = getRelatedId('phone_number_sms_logs','migration_source_id',row['messageID'])

    
    if(migId is not None):
        return True
        update_query="""
            UPDATE phone_number_sms_logs SET
            "from" = %s,
            "to" = %s
            where migration_source_id = %s
        """

        cursor.execute(update_query, (
            add_dashes(row['from']),
            add_dashes(row['to']),
            row['messageID']
        ))

        
        conn.commit()
        # print(f"Updated {row['messageID']}")
        return True
    
    phoneNumberSmsLogObject = { 
        "from": add_dashes(row['from']),
        "to": add_dashes(row['to']),
        "content": row['body'].replace('\x00', '').encode('utf-8').decode('utf-8'),
        "is_inbound": False,
        "provider_message_id": str(uuid.uuid4()),
        "provider_response": json.dumps(row),
        "occurred_at": time_es_to_utc(row['createdAt']),
        "created_at": time_es_to_utc(row['createdAt']),
        "updated_at": time_es_to_utc(row['updatedAt']),
        "status" : row['status'],
        "sent_by": sentBy,
        "migration_source_id" : row['messageID']
    }

    insert_query = """
    INSERT INTO phone_number_sms_logs (
        "from",
        "to",
        content,
        is_inbound,
        provider_message_id,
        provider_response,
        occurred_at,
        created_at,
        updated_at,
        status,
        sent_by,
        migration_source_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) returning id
    """
    values = (
        phoneNumberSmsLogObject['from'],
        phoneNumberSmsLogObject['to'],
        phoneNumberSmsLogObject['content'],
        phoneNumberSmsLogObject['is_inbound'],
        phoneNumberSmsLogObject['provider_message_id'],
        phoneNumberSmsLogObject['provider_response'],
        phoneNumberSmsLogObject['occurred_at'],
        phoneNumberSmsLogObject['created_at'],
        phoneNumberSmsLogObject['updated_at'],
        phoneNumberSmsLogObject['status'],
        phoneNumberSmsLogObject['sent_by'],
        phoneNumberSmsLogObject['migration_source_id']
    )

    cursor.execute(insert_query, values)

    conn.commit()
    # print(f"Imported {row['messageID']}")
    # phoneNumberSmsLogId = cursor.fetchone()[0]
    # lead_info = (leadId) or 'N/A'
    to_name = get_lead_name(leadId) or 'N/A'
    to_email = get_lead_email(leadId) or 'N/A'
    logData = {
        'from_name' : userName,
        'from_id' : sentBy,
        'to_id' : leadId,
        'to_name' : to_name,
        'to_email': to_email,
        'message': row['body'].replace('\x00', '').encode('utf-8').decode('utf-8')
    }

    createEventLog('customer', leadId, 'sms_sent', logData, phoneNumberSmsLogObject['created_at'])
    return True


def create_lead_email(row):
    attendant = get_attendant(row['recipient'], row['recipientID'])
    sentBy = get_user_id_by_email(row['from'])
    if sentBy == None:
        return False
    if attendant['lead_id'] == None:
        return False
    
    try:
        emailLogObject = {
            "user_id": sentBy,
            "subject": row['subject'],
            "body": row["body"],
            "bound_type": 'OUTBOUND',
            "created_at": time_es_to_utc(row["createdAt"]),
            "updated_at": time_es_to_utc(row["updatedAt"]),
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
            "body": row['body'].replace('\x00', '').encode('utf-8').decode('utf-8'),
            "log_type": "Lead"
        }

        createEventLog(attendant['attendant_type'],attendant['attendant_id'], 'mail_sent', logData, emailLogObject['created_at'])
        
        return True
    except Exception as e:
        print(f"Error: {e}")
        log_file = "app.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="{e}",
        )
        return False
def create_customer_email(row):
    attendant = get_attendant(row['recipient'], row['recipientID'])
    sentBy = get_user_id_by_email(row['from'])
    if sentBy == None:
        return False
    if attendant['customer_id'] == None:
        return False
    

    try:
        emailLogObject = {
            "user_id": sentBy,
            "subject": row['subject'],
            "body": row["body"],
            "bound_type": 'OUTBOUND',
            "created_at": time_es_to_utc(row["createdAt"]),
            "updated_at": time_es_to_utc(row["updatedAt"]),
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
            "body": row['body'].replace('\x00', '').encode('utf-8').decode('utf-8'),
            "log_type": "Customer"
        }

        createEventLog(attendant['attendant_type'],attendant['attendant_id'], 'mail_sent', logData, emailLogObject['created_at'])
        return True
    except Exception as e:
        print(f"Error: {e}")
        log_file = "app.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="{e}",
        )
        return False

def create_customer_sms(row):
    sentBy = get_user_id_by_email(row['from'])
    userName = get_username(sentBy)
    customerId = getRelatedId('customers','migration_source_id',row['recipientID'])

    if(sentBy == None):
        sentBy = getRelatedId('users','personal_phone_number',row['from']) or 2
    if(customerId == None):
        return False
    
    
    migId = getRelatedId('phone_number_sms_logs','migration_source_id',row['messageID'])
    
    if(migId != None):
        # if( '@' in row['to']):
        #     print(row['to'])
        # from_ = add_dashes(row['from'], "user"),
        # to_   = add_dashes(row['to']),
        # message_ = row['messageID']

        
        # update_query="""
        #     UPDATE phone_number_sms_logs SET
        #     "from" = %s,
        #     "to" = %s
        #     where migration_source_id = %s
        # """

        # cursor.execute(update_query, (
        #     add_dashes(row['from']),
        #     add_dashes(row['to']),
        #     row['messageID']
        # ))

        
        # conn.commit()
        # print(f"Updated {row['messageID']}")
        return True

    phoneNumberSmsLogObject = { 
        "from": add_dashes(row['from']),
        "to": add_dashes(row['to']),
        "content": row['body'].replace('\x00', '').encode('utf-8').decode('utf-8'),
        "is_inbound": False,
        "provider_message_id": str(uuid.uuid4()),
        "provider_response": json.dumps(row),
        "occurred_at": time_es_to_utc(row['createdAt']),
        "created_at": time_es_to_utc(row['createdAt']),
        "updated_at": time_es_to_utc(row['updatedAt']),
        "status" : row['status'],
        "sent_by": sentBy,
        "migration_source_id": row['messageID']
    }

    insert_query = """
    INSERT INTO phone_number_sms_logs (
        "from",
        "to",
        content,
        is_inbound,
        provider_message_id,
        provider_response,
        occurred_at,
        created_at,
        updated_at,
        status,
        sent_by,
        migration_source_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) returning id
    """
    try:
        values = (
            phoneNumberSmsLogObject['from'],
            phoneNumberSmsLogObject['to'],
            phoneNumberSmsLogObject['content'],
            phoneNumberSmsLogObject['is_inbound'],
            phoneNumberSmsLogObject['provider_message_id'],
            phoneNumberSmsLogObject['provider_response'],
            phoneNumberSmsLogObject['occurred_at'],
            phoneNumberSmsLogObject['created_at'],
            phoneNumberSmsLogObject['updated_at'],
            phoneNumberSmsLogObject['status'],
            phoneNumberSmsLogObject['sent_by'],
            phoneNumberSmsLogObject['migration_source_id']
        )

        cursor.execute(insert_query, values)

        conn.commit()
        # print(f"Imported {row['messageID']}")
        phoneNumberSmsLogId = cursor.fetchone()[0]
        
        logData = {
            'from_name' : userName,
            'from_id' : sentBy,
            'to_id' : customerId,
            'to_name' : get_customer_name(customerId),
            'to_email': get_customer_email(customerId),
            'message': row['body'].replace('\x00', '').encode('utf-8').decode('utf-8')
        }

        createEventLog('customer',customerId, 'sms_sent', logData, phoneNumberSmsLogObject['created_at'])
        return True
    except Exception as e:
        actual_query = insert_query % tuple(map(lambda x: f"'{x}'" if isinstance(x, str) else x, values))
        log_file = "app.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="{e}",
        )
        print("Actual SQL Query:", actual_query)
        print(f"Error: {e}")
        # print(f"Row causing error: {phoneNumberSmsLogObject}. Error: {row['messageID']}")

        return False

def read_csv():
    source_csv = "may 2025 exports/messages_may_2025_view.csv"
    # source_csv = "files/lead_mail_view.csv"
    # source_csv = "files/lead_sms_view.csv"
    # source_csv = "files/messages-customers.csv"
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        # reader = csv.DictReader(file)
        reader = list(csv.DictReader(file))  # Convert reader to list to get its length

        # i = 0
        # for row in reader:
        #     res = process_row(row)
        #     if(res):
        #         i = i + 1
        #     if i > 0:
        #         break
        with Pool(processes=20) as pool:
            # Use tqdm to create a progress bar
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()
            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} rows processed successfully')

                # result =pool.map(process_row, reader)            
                # succeeded = len([item for item in result if item == True])
                # print(f'{succeeded} of {len(result)} imported')

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    # print(add_dashes("+12404050161"))
    # sentBy = get_user_id_by_email(add_dashes('+13017124918'))
    # if(sentBy == None):
    #     sentBy = getRelatedId('users','personal_phone_number',add_dashes('+13017124918')) or 2
    #     sentBy = getRelatedId('users','id',add_dashes('+13017124918'))
    # print(sentBy)
    # print(add_dashes('mjeffries@hagerstownford.com'));
    read_csv()