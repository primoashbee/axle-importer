import csv
import os
from  helpers import *
from multiprocessing import Pool
from tqdm import tqdm
import logging
import functools


"""
follow_up x
appointment x
reminder x
outreach x
meeting x
to_do x
other x
callback x
contact x
phone_call x
sold_follow_up x
birthday_follow_up  x
anniversary_follow_up x
unsold_follow_up x
sold_cadence 
unsold_cadence 
send_email 
place_phone_call
"""
# task_type = 'place_phone_call'

tasks = [
    "follow_up",
    "appointment",
    "reminder",
    "outreach",
    "meeting",
    "to_do",
    "other",
    "callback",
    "contact",
    "phone_call",
    "sold_follow_up",
    "birthday_follow_up",
    "anniversary_follow_up",
    "unsold_follow_up",
    "sold_cadence",
    "unsold_cadence", #wip
    "send_email",
    "place_phone_call"
]

task_type = None

def process_row(task_type, row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    
    if row['status'] is None or row['status'] == '' or row['status'] == 'NULL':
        return False
    
    match task_type:
        case 'follow_up':
            return process_row_follow_up(row)
        case 'outreach':
            return process_row_outreach(row)
        case 'meeting':
            return process_row_meeting(row)
        case 'contact':
            return process_row_contact(row)
        case 'sold_follow_up':
            return process_row_sold_follow_up(row)
        case 'birthday_follow_up':
            return process_row_birthday_follow_up(row)
        case 'anniversary_follow_up':
            return process_row_anniversary_follow_up(row)
        case 'unsold_follow_up':
            return process_row_unsold_follow_up(row)
        case 'sold_cadence':
            return process_row_sold_cadence(row)
        case 'unsold_cadence':
            return process_row_unsold_cadence(row)
        case 'send_email':
            return process_row_send_email(row)
        case 'place_phone_call':
            return process_row_place_phone_call(row)
        case 'todo':
            return process_row_todo(row)
        case 'to_do':
            return process_row_todo(row)
        case 'phone_call':
            return process_row_phone_call(row)
        case 'call': #x
            return process_row_call(row)
        case 'callback':
            return process_row_callback(row)
        case 'appointment':
            return process_row_appointment(row)
        case 'reminder':
            return process_row_reminder(row)
        case 'other':
            return process_row_other(row)
        
        case _:
            print(f"Invalid task_type '{task_type}'")
            

def process_row_follow_up(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 9,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_follow_up_created",
        "event": {
            "task_category": "Contact/Follow Up",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_meeting(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 11,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_meeting_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_contact(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug', row['status']) or 8,
        "task_category_id": 12,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_contact_created",
        "event": {
            "task_category": "Contact/Follow Up",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_sold_follow_up(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 13,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_sold_follow_up_created",
        "event": {
            "task_category": "Contact/Follow Up",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_birthday_follow_up(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 14,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_birthday_follow_up_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
def process_row_anniversary_follow_up(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 15,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_anniversary_follow_up_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
def process_row_unsold_follow_up(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 17,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_unsold_follow_up_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_sold_cadence(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 18,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_sold_cadence_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
    # print(f"Task created with migration id {row['id']}")
    return True

def process_row_unsold_cadence(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 17,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_unsold_cadence_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
    # print(f"Task created with migration id {row['id']}")
    return True

def process_row_send_email(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 21,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_send_email_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
    # print(f"Task created with migration id {row['id']}")
    return True

def process_row_place_phone_call(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']) or 8,
        "task_category_id": 22,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_place_phone_call_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
    # print(f"Task created with migration id {row['id']}")
    return True

def process_row_outreach(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author = get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 10,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_outreach_created",
        "event": {
            "task_category": "Task",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_other(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 7,
        "task_priority_id":"2",
        "due_date":time.strftime('%Y-%m-%d'),
        "due_time":time.strftime('%H:%M:%S'),
        "date_accepted":None,
        "details": row['description'],
        "location":None,
        "is_automated":"0",
        "deleted_at":None,
        "created_at": created_date,
        "updated_at":created_date,
        "parent_id":None,
        "migration_source_id": row['id'],
        "attendant": attendant,
        "event_title": "task_other_created",
        "event": {
            "task_category": "Other",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_todo(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 1,
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
        "event_title": "task_todo_created",
        "event": {
            "task_category": "To Do",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_phone_call(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 2,
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
        "event_title": "task_phone_call_created",
        "event": {
            "task_category": "Phone Call",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_call(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 3,
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
        "event_title": "task_call_created",
        "event": {
            "task_category": "Call",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_callback(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 4,
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
        "event_title": "task_callback_created",
        "event": {
            "task_category": "Callback",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

def process_row_appointment(row):
    # log_file = "app.log"
    # logging.basicConfig(
    #     filename=log_file,
    #     level=logging.INFO,
    #     format="%(asctime)s - %(message)s",

    # )
    # logging.info(f"Row ID: {row['id']}")
    # if(row['id'] == 1307191 or row['id'] == "1307191"):
    #     print(row)
    # return False
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        ## print(f"Task with migration source id {row["id"]} exists. Skipping.")
        update_query = """
            UPDATE tasks set task_category_id = %s
            WHERE migration_source_id = %s
        """

        cursor.execute(update_query, (
            5,
            row['id']
        ))
        conn.commit()
        return True
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        ## print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id":getRelatedId('task_statuses','slug',row['status'].lower()),
        "task_category_id": 5,
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
        "event_title": "task_appointment_created",
        "event": {
            "task_category": "Scheduled",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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

    appointment_object = {
        "customer_id" : attendant['customer_id'],
        "appointment_date" : time.strftime('%Y-%m-%d'),
        "appointment_time" : time.strftime('%H:%M:%S'),
        "description": row["description"],
        "created_at": created_date,
        "updated_at": created_date,
        "user_id" : authorId,
        "lead_id" : attendant["lead_id"],
        "attendant" : attendant,
        "event_title": "task_appointment_created",
        "event": {
            "task_category": "Scheduled",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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
    taskId = create_task(task_object)
    appointmentId= create_appointment(appointment_object)
    taskAppointmentId = create_task_appointment(taskId, appointmentId)
    return True

def process_row_reminder(row):
    mig_id = getRelatedId('tasks','migration_source_id',row['id'])
    if (mig_id != None):
        #print(f"Task with migration source id {row["id"]} exists. Skipping.")
        return False
    attendant = get_attendant(row['attendant_type'],row['attendant_id'])
    if(attendant['lead_id'] == None and attendant['customer_id'] == None):
        #print(f'No record found for attendant: {row['attendant_type']} {row['attendant_id']}. Skipping ID {row['id']}')
        return False
    created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    time = time_pacific_to_utc(row['time'])
    taskId = getRelatedId('tasks','migration_source_id',row['id'])
    if taskId:
        # print(f"Task with ID {row['id']} already exists. Skipping")
        return False
    authorId = getRelatedId('users','migration_source_id',row['hostID']) or 2
    assigneeId = getRelatedId('users','migration_source_id',row['userID'])

    assignee = get_username(assigneeId)
    author =get_username(authorId)

    if(assigneeId == None):
        # print(f"no assignee {assigneeId}")
        return False
    task_object = {
        "author_id": authorId,
        "customer_id":attendant['customer_id'],
        "lead_id":attendant['lead_id'],
        "assignee_id":getRelatedId('users','migration_source_id',row['hostID']),
        "task_status_id": getRelatedId('task_statuses','slug',row['status']),
        "task_category_id": 6,
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
        "event_title": "task_reminder_created",
        "event": {
            "task_category": "Reminder",
            "task_priority": "Normal",
            "task_status": row["status"].capitalize(),
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


def create_appointment(appointment_object):
    insert_query="""
    INSERT INTO appointments (
        customer_id,
        appointment_date,
        appointment_time,
        description,
        created_at,
        updated_at,
        user_id,
        lead_id
    ) VALUES (
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
        appointment_object['customer_id'],
        appointment_object['appointment_date'],
        appointment_object['appointment_time'],
        appointment_object['description'] or "N/A",
        appointment_object['created_at'],
        appointment_object['updated_at'],
        appointment_object['user_id'],
        appointment_object['lead_id'],
    ))
    conn.commit()
    appointment_id = cursor.fetchone()[0];
    return appointment_id

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


def create_task_appointment(taskId, appointmentId):
    insert_query  = """
    INSERT INTO task_appointments (
        task_id,
        appointment_id
    ) VALUES (
        %s,
        %s
    )
    returning id
    """
    cursor.execute(insert_query, (
        taskId,
        appointmentId
    ))
    conn.commit()
    taskAppointmentId = cursor.fetchone()[0];
    return taskAppointmentId

def get_source_csv(task_type):
    match task_type:
        case "follow_up":
            return "files/follow_up.csv"
        case "birthday_follow_up":
            return "files/birthday_follow_up.csv"
        case "to_do":
            return "may-2025-files/to_do.csv"
        case "meeting":
            return "files/meeting.csv"
        case "outreach":
            return "files/outreach.csv"
        case "contact":
            return "may-2025-files/contact.csv"
        case "sold_follow_up":
            return "files/sold_follow_up.csv"
        case "anniversary_follow_up":
            return "files/anniversary_follow_up.csv"
        case "unsold_follow_up":
            return "files/unsold_follow_up.csv"
        case "sold_cadence":
            return "files/sold_cadence.csv"
        case "unsold_cadence":
            return "files/unsold_cadence.csv"
        case "send_email":
            return "files/send_email.csv"
        case "place_phone_call":
            return "files/place_phone_call.csv"
        case "phone_call":
            return "may-2025-files/phone_call.csv"
        case "call":
            return "files/call.csv"
        case "callback":
            return "may-2025-files/callback.csv"
        case "appointment":
            return "may-2025-files/appointment.csv"
        case "reminder":
            return "may-2025-files/reminder.csv"
        case "other":
            return "may-2025-files/other.csv"
            

def read_csv(task_type):
    source_csv = get_source_csv(task_type)
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        with Pool(processes=20) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                partial_process_row = functools.partial(process_row, task_type)
                for res in pool.imap(partial_process_row, reader):
                    result.append(res)
                    pbar.update()
            
            succeeded = len([item for item in result if item == True])
            print(f'{succeeded} of {len(result)} imported')


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    # task_type="contact"
    # read_csv()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    export_dir = os.path.join(base_dir, 'may-2025-files')
    filenames = [os.path.splitext(file)[0] for file in os.listdir(export_dir) if os.path.isfile(os.path.join(export_dir, file))]
    filenames = ['callback', 'other', 'to_do', 'contact', 'phone_call', 'appointment', 'reminder'];
    print(filenames)

    # exit
    for task in filenames:
        print(f"Processing {task}...")
        
        read_csv(task.strip())