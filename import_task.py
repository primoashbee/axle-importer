import csv
import os
from  helpers import *
from multiprocessing import Pool
task_type = 'other'


def process_row(row):
    match task_type:
        case 'other':
            return process_row_other(row)
        case _:
            print(f"Invalid task_type '{task_type}'")
            


def process_row_other(row):
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_es_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_es_to_utc(row['time'])
    task_object = {
        "author_id":"2",
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id":getRelatedId('task_categories','slug','other'),
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"1",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id']
    }
    create_task(task_object)
    return True

    

def create_task(task_object):
    insert_query = """
    INSERT INTO tasks (
        author_id,
        customer_id,
        lead_id,
        assignee_id,
        task_status_id,
        task_category_id,
        task_priority_id,
        due_date,
        due_time,
        date_accepted,
        details,
        location,
        is_automated,
        deleted_at,
        created_at,
        updated_at
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
        %s,
        %s
    )
    returning id
    """

    cursor.execute(insert_query, (
        task_object['assignee_id'],
        task_object['customer_id'],
        task_object['lead_id'],
        task_object['assignee_id'],
        task_object['task_status_id'],
        task_object['task_category_id'],
        task_object['task_priority_id'],
        task_object['due_date'],
        task_object['due_time'],
        task_object['date_accepted'],
        task_object['details'],
        task_object['location'],
        task_object['is_automated'],
        task_object['deleted_at'],
        task_object['created_at'],
        task_object['updated_at']
    ))
    conn.commit()
    task_id = cursor.fetchone()[0]
    createEventLog("Domain\\Tasks\\Models\\Task",task_id, "created", task_object, datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    return task_id



def get_attendant(attendant_type, id):
    attendant = {
        'lead_id': None,
        'customer_id': None
    }
    match attendant_type:
        case "lead":
            attendant['lead_id'] = getRelatedId('leads','migration_source_id',id)
        case "customer":
            attendant['customer_id'] = getRelatedId('customers','migration_source_id',id)
    return attendant



def read_csv():
    source_csv = 'files/other.csv'
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