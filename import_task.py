import csv
import os
from  helpers import *
from multiprocessing import Pool
task_type = 'todo'


def process_row(row):
    match task_type:
        case 'other':
            return process_row_other(row)
        case 'todo':
            return process_row_todo(row)
        case _:
            print(f"Invalid task_type '{task_type}'")
            


def process_row_other(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_es_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_es_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
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
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": "Assigned",
            "task_assignee_text": f"Assigned to {assignee} by {author}",
            "task_details": "gg",
            "task_created_by": author,
            "task_due_date": time.strftime('%Y-%m-%d %H:%M:%S'),
            "task_asignee_id":assigneeId,
            "task_author_id":authorId,
            "task_asignee_name": assignee,
            "task_author_name": author
        }
    }
    create_task(task_object)


    
    return True

def process_row_todo(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_es_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_es_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
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
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": "Assigned",
            "task_assignee_text": f"Assigned to {assignee} by {author}",
            "task_details": row['description'],
            "task_created_by": author,
            "task_due_date": time.strftime('%Y-%m-%d %H:%M:%S'),
            "task_asignee_id":assigneeId,
            "task_author_id":authorId,
            "task_asignee_name": assignee,
            "task_author_name": author
        }
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
        updated_at,
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
        task_object['updated_at'],
        task_object['migration_source_id'],
    ))
    conn.commit()
    task_id = cursor.fetchone()[0]
    createEventLog(task_object['attendant']['attendant_type'],task_object['attendant']['attendant_id'], task_object['event_title'], task_object['event'], task_object['created_at'])
    return task_id



def get_attendant(attendant_type, id):
    attendant = {
        'lead_id': None,
        'customer_id': None,
        'attendant_type': "",
    }
    match attendant_type:
        case "lead":
            attendant_id = getRelatedId('leads','migration_source_id',id)
            attendant['lead_id'] = attendant_id
            attendant['attendant_id'] = attendant_id
            attendant['attendant_type'] = 'lead'
        case "customer":
            attendant_id = getRelatedId('customers','migration_source_id',id)
            attendant['customer_id'] = attendant_id
            attendant['attendant_id'] = attendant_id
            attendant['attendant_type'] = 'customer'
    return attendant

def get_source_csv():
    match task_type:
        case "other":
            return "files/others.csv"
        case "todo":
            return "files/to_do.csv"

def read_csv():
    source_csv = get_source_csv()
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