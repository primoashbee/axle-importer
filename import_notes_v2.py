import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from helpers import createEventLog, getRelatedId, add_dashes, cursor, conn, validate_date, get_username, get_attendant, time_pacific_to_utc
import uuid
import os
from multiprocessing import Pool
from tqdm import tqdm

def process_row(row):
    return create_notes(row)

def create_notes(row):
    if(row['target_type'] == 'event'):
        return False
    if(row['target_id'] not in [2995, "2995"]):
        return False
    attendant = get_attendant(row['target_type'], row['target_id'])
 
    if(attendant['attendant_id'] is None):
        return False
    
    if(attendant['attendant_type'] == 'lead'):
        migId = getRelatedId("lead_notes","migration_source_id", row['id'])
        
    if(attendant['attendant_type'] == 'customer'):
        migId = getRelatedId("customer_notes","migration_source_id", row['id'])

    userId = getRelatedId('users','migration_source_id', row['creator_id'])

    if(migId != None):
        # Update the note to the correct lead / customer
        update_note(row['id'], attendant['attendant_type'], attendant['attendant_id'], row['content'], userId)
        return False
    
    # created_date = time_pacific_to_utc(row['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
    created_date = row['createdAt']

    if(attendant['attendant_type'] == 'lead'):
        insert_query = """
        INSERT into lead_notes (lead_id, user_id, content, migration_source_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        cursor.execute(insert_query, (
            attendant['attendant_id'],
            userId,
            row['content'],
            row['id'],
            created_date,
            created_date,
        ))
        conn.commit()
        noteId = cursor.fetchone()[0]
        logData = {
            "content" : row['content'],
            "user_id": userId,
            "lead_id": attendant['attendant_id'],
            "id": noteId
        }
        
        createEventLog('lead', attendant['attendant_id'], 'lead-note-created', logData, created_date)
        return True

    if(attendant['attendant_type'] == 'customer'):
        
        insert_query = """
        INSERT into customer_notes (customer_id, user_id, content, migration_source_id,created_at,updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        cursor.execute(insert_query, (
            attendant['attendant_id'],
            userId,
            row['content'],
            row['id'],
            created_date,
            created_date
        ))
        conn.commit()
        noteId = cursor.fetchone()[0]
        logData = {
            "content" : row['content'],
            "user_id": userId,
            "lead_id": attendant['attendant_id'],
            "id": noteId
        }
        createEventLog('customer', attendant['attendant_id'], 'Note created', logData, created_date)
        return True


def update_note(migration_source_id, attendant_type, attendant_id, content, user_id):
    if(attendant_type == 'lead'):
        update_query = """
        UPDATE lead_notes set lead_id = %s, user_id = %s where migration_source_id = %s
        """


    if(attendant_type == 'customer'):
        update_query = """
        UPDATE customer_notes set customer_id = %s, user_id = %s where migration_source_id = %s
        """

    cursor.execute(update_query, (
        attendant_id,
        user_id,
        migration_source_id
    ))
    conn.commit()

    print(f'Updated note {migration_source_id} to {attendant_type} {attendant_id}')
    return True

    
def read_csv():
    source_csv = "files/note_view.csv"
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        with Pool(processes=20) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()
                # result =pool.map(process_row, reader)
                
        succeeded = len([item for item in result if item == True])
        print(f'{succeeded} of {len(result)} imported')


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()
        