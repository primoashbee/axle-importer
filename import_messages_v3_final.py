import os
import csv
from multiprocessing import Pool
from tqdm import tqdm
import json
import sys
from helpers import add_dashes, cache_users, get_user_by_email, log, cursor, conn, createEventLog, getRelatedId, get_lead_info_by_migration_source_id, get_customer_info_by_migration_source_id, check_if_user_exists
csv.field_size_limit(sys.maxsize)
import time
user_cache = {}

def process_row(row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    if row.get('type') is None or row.get('recipient') is None:
        return False    
    type = row.get('type', '').lower()

    if type == 'mail':
        return process_mail(row)
    elif type == 'text':
        return process_sms(row)
    else:
        print(f"Unknown type '{type}'")
        return False
    return False

def process_mail(row):
    recipient_type = row.get('recipient', '').lower()
    recipient_id = row.get('recipientID', None)

    if recipient_type not in ['lead','customer']:
        return False
    
    mail_from = row.get('from').lower()
    mail_to = row.get('to')
    is_inbound = '@hagerstownford.com' not in mail_from.lower()
    sent_by = 2
    if(is_inbound is False):
        user = get_user_by_email(mail_from)
        sent_by = user.get('id') if user else 2

    # create email log
    # link email log to lead or customer
    emailLogObject = {
            "user_id": sent_by,
            "sender_provider_id": row.get('message_uuid', ''),
            "subject": row.get('subject', ''), 
            "body": row.get('body', '').replace('\x00', ''),
            "bound_type": 'INBOUND' if is_inbound else 'OUTBOUND',
            "created_at": row["createdAt"],
            "updated_at": row["updatedAt"],
            "migration_source_id": row['messageID'],
    }

    from_name = None
    axle_user_name = extract_axle_user_name(row, is_inbound) 
    logged_id = None
    if recipient_type == 'lead':
        lead_info = get_lead_info_by_migration_source_id(recipient_id)
        if lead_info is None:
            log(f"Lead info not found for recipient ID: {recipient_id}")
            return False
        lead_id = lead_info.get('id')
        from_id = lead_id if is_inbound else 2
        from_name = lead_info.get('full_name') if is_inbound else axle_user_name
        to_name = axle_user_name if is_inbound else lead_info.get('full_name')
        to_id = sent_by if is_inbound else lead_id
        email_log_id = create_email_log(emailLogObject)

        if lead_id:
            leadEmailObject = {
                "email_log_id": email_log_id,
                "lead_id": lead_id,
                "created_at": emailLogObject['created_at'],
                "updated_at": emailLogObject['updated_at'],
            }
            query = """
            INSERT INTO lead_email_logs (
                email_log_id, lead_id, created_at, updated_at
            ) VALUES (
                %(email_log_id)s, %(lead_id)s, %(created_at)s, %(updated_at)s
            )
            RETURNING id
            """
            cursor.execute(query, leadEmailObject)
            conn.commit()
            result = cursor.fetchone()
            eventLogData = {
                "from_id": from_id,
                "from_name": from_name,
                "to_id": to_id,
                "to": to_name,
                "to_email": mail_to if is_inbound else mail_from,
                "subject": row.get('subject', ''),
                "body" : row.get('body', '').replace('\x00', ''),
            }
            createEventLog('lead', lead_id, 'email_received' if is_inbound else 'email_sent', eventLogData, row['createdAt'])
            if(result):
                logged_id = result[0]

    if recipient_type == 'customer':
        customer_info = get_customer_info_by_migration_source_id(recipient_id)
        if customer_info is None:
            log(f"Customer info not found for recipient ID: {recipient_id}")
            return False
        customer_id = customer_info.get('id')
        from_id = customer_id if is_inbound else 2
        to_id = sent_by if is_inbound else customer_id
        from_name = customer_info.get('full_name') if is_inbound else axle_user_name
        to_name = axle_user_name if is_inbound else customer_info.get('full_name')
        to_id = sent_by if is_inbound else customer_id
        email_log_id = create_email_log(emailLogObject)

        if customer_id:
            customerEmailObject = {
                "email_log_id": email_log_id,
                "customer_id": customer_id,
                "created_at": emailLogObject['created_at'],
                "updated_at": emailLogObject['updated_at'],
            }
            query = """
            INSERT INTO customer_email_logs (
                email_log_id, customer_id, created_at, updated_at
            ) VALUES (
                %(email_log_id)s, %(customer_id)s, %(created_at)s, %(updated_at)s
            )
            RETURNING id
            """
            cursor.execute(query, customerEmailObject)
            conn.commit()
            result = cursor.fetchone()

            eventLogData = {
                "from_id": from_id,
                "from_name": from_name,
                "to_id": to_id,
                "to": to_name,
                "to_email": mail_to if is_inbound else mail_from,
                "subject": row.get('subject', ''),
                "body" : row.get('body', '').replace('\x00', ''),
            }
            createEventLog('customer', customer_id, 'email_received' if is_inbound else 'email_sent', eventLogData, row['createdAt'])
            if(result):
                logged_id = result[0]
    return bool(logged_id)

def create_email_log(emailLogObject):
    """
    Insert email log into the database using cursor.execute with parameterized values.
    """
    query = """
        INSERT INTO email_logs (
            user_id, sender_provider_id, subject, body, bound_type, 
            created_at, updated_at, migration_source_id
        ) VALUES (
            %(user_id)s, %(sender_provider_id)s, %(subject)s, %(body)s, %(bound_type)s, 
            %(created_at)s, %(updated_at)s, %(migration_source_id)s
        )
        RETURNING id
    """
    try: 
        cursor.execute(query, emailLogObject)
        conn.commit()
        result = cursor.fetchone()
        if result:
            return result[0]  # Return the ID of the inserted email log
        return False
    except Exception as e:
        log(f"Error inserting email log: {e}")
        log(f"Data: {emailLogObject}")
   

def check_if_mail_inbound(mail_from):
    if mail_from is None:
        return False
    return check_if_user_exists(mail_from) 
    


def extract_axle_user_name(row, is_inbound):
    axle_user_name_raw = row.get('to') if is_inbound else row.get('from')
    array = axle_user_name_raw.split('|')
    axle_user_name = array[0]
    if(len(array) > 1):
        axle_user_name_raw = array[1]
    if('@' in axle_user_name):
        result = get_user_by_email(axle_user_name)
        axle_user_name = result.get('full_name', axle_user_name) if result is not None else 'Axle CRM'

    return axle_user_name


def process_sms(row):

    notifier = add_dashes('+13017533306')  # Replace with your notifier phone number
    recipient_type = row.get('recipient', '').lower()
    recipient_id = row.get('recipientID', None)
    
    if recipient_type not in ['lead','customer']:
        return False
    
    sms_from = row.get('from')
    sms_to = row.get('to')
    
    is_inbound = False if '@' in sms_from else True 
    from_id = None
    lead_id = None
    customer_id = None
    user_data = get_user_by_email(sms_from)
    from_name = None
    to_name = None
    sent_by = None if is_inbound else (user_data.get('id') if user_data else None)
    sent_by_name = None if is_inbound else (user_data.get('id') if user_data else None)
    axle_user_name = extract_axle_user_name(row, is_inbound)    
    if( recipient_type == 'lead' ):
        # lead_id = getRelatedId('leads','migration_source_id', recipient_id)
        lead_info = get_lead_info_by_migration_source_id(recipient_id)
        if(lead_info is None):
            log(f"Lead info not found for recipient ID: {recipient_id}")
            return False
        log(f"Lead Info: {lead_info}")
        lead_id = lead_info.get('id')
        
        from_id = lead_id if is_inbound else 2
        from_name = lead_info.get('full_name') if is_inbound else axle_user_name
        to_name = axle_user_name if is_inbound else lead_info.get('full_name')
        to_id = sent_by if is_inbound else lead_id

    if( recipient_type == 'customer' ):
        # customer_id = getRelatedId('customers','migration_source_id', recipient_id)
        customer_info = get_customer_info_by_migration_source_id(recipient_id)
        if(customer_info is None):
            log(f"Customer info not found for recipient ID: {recipient_id}")
            return False
        customer_id= customer_info.get('id')
        from_id = customer_id if is_inbound else 2
        to_id = sent_by if is_inbound else customer_id
        from_name = customer_info.get('full_name') if is_inbound else axle_user_name
        to_name = axle_user_name if is_inbound else customer_info.get('full_name')
        to_id = sent_by if is_inbound else customer_id
    
    from_value = add_dashes(sms_from) if is_inbound else notifier
    to_value = notifier if is_inbound else add_dashes(sms_to)

    
    
        
    verb = 'sms_received' if is_inbound else 'sms_sent'
    phoneNumberSmsLog = {
        "from": from_value,
        "to": to_value, 
        "content": row.get('body', '').replace('\x00', ''),
        "is_inbound": is_inbound,
        "provider_response": json.dumps(row),
        "provider_message_id": row.get('message_uuid', ''),
        "occurred_at": row.get('createdAt', ''),
        "created_at": row.get('createdAt', ''),
        "updated_at": row.get('createdAt', ''),
        "status": row.get('status', ''),
        "sent_by": sent_by,
        "migration_source_id": row.get('messageID', ''),
        "lead_id": lead_id,
        "customer_id": customer_id
        # id,
    }

    id = create_sms_log(phoneNumberSmsLog)
    eventLogData = {
        "from_name": from_name,
        "from_id": from_id,
        "to_id": to_id,
        "to_name": to_name,
        "to_email": None,
        "message" : row.get('body', '').replace('\x00', ''),
    }
    if(customer_id is not None):
        log(f"Event ID: {createEventLog('customer', customer_id, verb, eventLogData, phoneNumberSmsLog['created_at'])}")
    if( lead_id is not None):
        log(f"Event ID: {createEventLog('lead', lead_id, verb, eventLogData, phoneNumberSmsLog['created_at'])}")
    return bool(id)


def create_sms_log(phoneNumberSmsLog):
    """
    Insert SMS log into the database using cursor.execute with parameterized values.
    """
    query = """
        INSERT INTO phone_number_sms_logs (
            "from", "to", content, is_inbound, provider_response, 
            provider_message_id, occurred_at, created_at, updated_at, 
            status, sent_by, migration_source_id, lead_id, customer_id
        ) VALUES (
            %(from)s, %(to)s, %(content)s, %(is_inbound)s, %(provider_response)s, 
            %(provider_message_id)s, %(occurred_at)s, %(created_at)s, %(updated_at)s, 
            %(status)s, %(sent_by)s, %(migration_source_id)s,  %(lead_id)s,  %(customer_id)s
        )
        RETURNING id
    """
    try: 
        cursor.execute(query, phoneNumberSmsLog)
        conn.commit()
        result = cursor.fetchone()
        if result:
            return result[0]  # Return the ID of the inserted SMS log
        return False
    except Exception as e:
        log(f"Error inserting SMS log: {e}")
        log(f"Data: {phoneNumberSmsLog}")


def get_lead_id_by_phone_number(phone_number):
    if not phone_number:
        return None
    phone_number = add_dashes(phone_number)
    query = """
    SELECT id FROM leads WHERE phone_number = %s and deleted_at is null order by created_at desc limit 1
    """
    cursor.execute(query, (phone_number,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def get_customer_id_by_phone_number(phone_number):
    if not phone_number:
        return None
    phone_number = add_dashes(phone_number)
    query = """
    SELECT customer_id FROM customer_contact_information WHERE mobile_phone = %s and deleted_at is null order by created_at desc limit 1
    """
    cursor.execute(query, (phone_number,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def get_customer_ids_by_phone_number(phone_number):
    if not phone_number:
        return None
    phone_number = add_dashes(phone_number)
    query = """
    SELECT customer_id FROM customer_contact_information WHERE mobile_phone = %s and deleted_at is null order by created_at desc
    """
    cursor.execute(query, (phone_number,))
    result = cursor.fetchone()
    if result:
        return [row[0] for row in cursor.fetchall()]
    return []


def read_csv():
    source_csv = "files/messages.csv"
    # source_csv = "files/lead_mail_view.csv"
    # source_csv = "files/lead_sms_view.csv"
    # source_csv = "files/messages-customers.csv"123
    cache_users()
    start_time = time.time()  # Record the start time

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

            end_time = time.time()  # Record the end time
            time_lapsed = end_time - start_time  # Calculate time lapsed
            print(f"Time lapsed: {time_lapsed:.2f} seconds")




if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
    