import csv
import sys
import uuid
import json
import logging
import traceback
from helpers import *
from multiprocessing import Pool
from tqdm import tqdm

csv.field_size_limit(sys.maxsize)

# Setup logging once
logging.basicConfig(filename="app.log", level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def execute_query(query, values=None, fetchone=False):
    try:
        cursor.execute(query, values)
        if fetchone:
            return cursor.fetchone()
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"DB Error: {e}\nQuery: {query}\nValues: {values}\n{traceback.format_exc()}")
        return False

def process_row(row):
    try:
        row_type = row['type']
        recipient = row['recipient']

        if row_type == 'mail':
            return False  # skipping mails here, can be changed if needed

        if recipient == 'lead' and row_type == 'text':
            return create_lead_sms(row)

        if recipient == 'customer' and row_type == 'text':
            return create_customer_sms(row)

        # Add mail processing if needed here

        return False
    except Exception as e:
        logging.error(f"Error processing row {row.get('messageID', 'N/A')}: {e}\n{traceback.format_exc()}")
        return False

def create_lead_sms(row):
    sentBy = get_user_id_by_email(row['from']) or getRelatedId('users', 'personal_phone_number', row['from']) or 2
    leadId = getRelatedId('leads', 'migration_source_id', row['recipientID'])
    if not leadId:
        return False

    linked_customer = get_linked_customer_by_lead_id(row['recipientID'])
    if linked_customer:
        return create_lead_to_customer_sms(row, linked_customer)

    migId = getRelatedId('phone_number_sms_logs', 'migration_source_id', row['messageID'])
    phone_object = phone_of(add_dashes(row['from']))
    is_inbound = phone_object['id'] is not None

    if migId:
        update_query = """
            UPDATE phone_number_sms_logs SET
                "from" = %s,
                "to" = %s,
                is_inbound = %s,
                created_at = %s,
                updated_at = %s
            WHERE migration_source_id = %s
        """
        return execute_query(update_query, (
            add_dashes(row['from']),
            add_dashes(row['to']),
            is_inbound,
            row['createdAt'],
            row['updatedAt'],
            row['messageID']
        ))

    insert_query = """
    INSERT INTO phone_number_sms_logs (
        "from", "to", content, is_inbound, provider_message_id,
        provider_response, occurred_at, created_at, updated_at,
        status, sent_by, migration_source_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """

    content = row['body'].replace('\x00', '').encode('utf-8').decode('utf-8')
    values = (
        add_dashes(row['from']),
        add_dashes(row['to']),
        content,
        is_inbound,
        str(uuid.uuid4()),
        json.dumps(row),
        row['createdAt'],
        row['createdAt'],
        row['updatedAt'],
        row['status'],
        sentBy,
        row['messageID']
    )
    result = execute_query(insert_query, values, fetchone=True)
    if not result:
        return False

    to_name = get_lead_name(leadId) or 'N/A'
    to_email = get_lead_email(leadId) or 'N/A'
    logData = {
        'from_name': get_username(sentBy),
        'from_id': sentBy,
        'to_id': leadId,
        'to_name': to_name,
        'to_email': to_email,
        'message': content
    }
    createEventLog('customer', leadId, 'sms_sent', logData, row['createdAt'])
    return True

def create_lead_to_customer_sms(row, linked_customer):
    is_inbound = '@' not in row['from']
    from_number = linked_customer[12] if is_inbound else row['from']
    to_number = linked_customer[12] if is_inbound else row['to']

    row['recipientID'] = str(linked_customer[3])
    row['from'] = from_number or row['from']
    row['to'] = to_number or row['to']

    return create_customer_sms(row)

def create_customer_sms(row):
    is_inbound = '@' not in row['from']
    sentBy = None if is_inbound else get_user_id_by_email(row['from'])
    customerId = getRelatedId('customers', 'migration_source_id', row['recipientID'])
    if not customerId:
        return False

    if sentBy is None:
        sentBy = getRelatedId('users', 'personal_phone_number', row['from']) or getRelatedId('users', 'email', row['from']) or 2

    migId = getRelatedId('phone_number_sms_logs', 'migration_source_id', row['messageID'])
    if migId:
        update_query = """
            UPDATE phone_number_sms_logs SET
                "from" = %s,
                "to" = %s
            WHERE migration_source_id = %s
        """
        return execute_query(update_query, (
            add_dashes(row['from']),
            add_dashes(row['to']),
            row['messageID']
        ))

    insert_query = """
    INSERT INTO phone_number_sms_logs (
        "from", "to", content, is_inbound, provider_message_id,
        provider_response, occurred_at, created_at, updated_at,
        status, sent_by, migration_source_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """

    content = row['body'].replace('\x00', '').encode('utf-8').decode('utf-8')
    values = (
        add_dashes(row['from']) if is_inbound else '+13017533306',
        add_dashes(row['to']),
        content,
        is_inbound,
        str(uuid.uuid4()),
        json.dumps(row),
        time_pacific_to_utc(row['createdAt']),
        time_pacific_to_utc(row['createdAt']),
        time_pacific_to_utc(row['updatedAt']),
        row['status'],
        sentBy,
        row['messageID']
    )
    result = execute_query(insert_query, values, fetchone=True)
    if not result:
        return False

    logData = {
        'from_name': get_customer_name(customerId) if is_inbound else get_username(sentBy),
        'from_id': sentBy,
        'to_id': sentBy if is_inbound else customerId,
        'to_name': get_username(sentBy) if is_inbound else get_customer_name(customerId),
        'to_email': row['to'] if is_inbound else get_customer_email(customerId),
        'message': content
    }
    createEventLog('customer', customerId, 'sms_sent', logData, time_pacific_to_utc(row['createdAt']))
    return result[0]

# You can similarly refactor create_lead_email and create_customer_email functions
# ...

# Example usage with multiprocessing (optional)
def main(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    with Pool(processes=15) as pool:
        results = list(tqdm(pool.imap(process_row, rows), total=len(rows)))

    print(f"Processed {len(results)} rows.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py path_to_csv")
        sys.exit(1)
    main(sys.argv[1])
