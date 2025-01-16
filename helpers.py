import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
import pytz
import re

conn = psycopg2.connect(
    dbname="prod_v3_backup",
    user="postgres",
    password="root",
    host="localhost",
    port=5432
)
global cursor
cursor = conn.cursor()

def createEventLog(loggable, loggableId, event, logData, createdAt):
    """
        Mimic createEventLog on Laravel app.
    """
    
    

    data = {
            "user_type" : None,
            "user_id" : None,
            "event" : event,
            "auditable_type" : loggable,
            "auditable_id" : loggableId,
            "old_values" : json.dumps([]),
            "new_values" : json.dumps(logData),
            "url": "http://hagerstownford.axleautosolutions.com/api/v1/workspace/calls/end-call",
            "ip_address": "127.0.0.1",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "created_at": createdAt,
            "updated_at": createdAt
        }
    
    if loggable == "lead":
        data['auditable_type'] = "Domain\\Leads\\Models\\Lead"

    if loggable == "customer":
        data['auditable_type'] = "Domain\\Customers\\Models\\Customer"
    
    insert_query = """
    INSERT INTO audits (
        user_type, user_id, event, auditable_type, auditable_id,
        old_values, new_values, url, ip_address, user_agent, created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    """
    cursor.execute(insert_query, (
        data["user_type"], data["user_id"], data["event"], data["auditable_type"], data["auditable_id"],
        data["old_values"], data["new_values"], data["url"], data["ip_address"], data["user_agent"],
        data["created_at"], data["updated_at"]
    ))

    conn.commit()
    
    
def createUser(name,email,migration_source_id,firstname,lastname):
    data = {
        "name" : name,
        "email" : email,
        "password" : "$2y$12$KWeeoypM9oYWJfiqqp9PMeNEToMzsaZry3iqFlhHqYipOS5rVWisO",
        "first_name": firstname,
        "last_name": lastname,
        "migration_source_id": migration_source_id,
    }

    insert_query = """
            INSERT INTO users (
                name, email, password, first_name, last_name, migration_source_id
            ) VALUES (
                %s, %s, %s, %s, %s,  %s
            )
        """
    cursor.execute(insert_query, (
        data["name"], data["email"], data["password"], data["first_name"], data["last_name"], data["migration_source_id"]
    ))

    conn.commit()

def get_username(id):
    if(id == None):
        return None
    query = sql.SQL(f"SELECT name FROM users WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

def getRelatedId(table, column, value):
    """
    Fetch the related ID from a given table and column, or return None if not found.
    """
    if(value == None or value == ''):
        return None
    
    query = sql.SQL("SELECT id FROM {table} WHERE {column} = %s order by id DESC").format(
        table=sql.Identifier(table),
        column=sql.Identifier(column)
    )
    cursor.execute(query, (value,))
    
    result = cursor.fetchone()
    # if(result == None):
    #     print(cursor.mogrify(query.as_string(cursor), (value,)).decode("utf-8"))

    return result[0] if result else None

def sluggify(string):
    text = string.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', '-', text)
    return text.strip('-')

def createSource(sourceObject):
    insert_query="""
    INSERT into sources (name, slug, type, active)
    VALUES (
        %s,
        %s,
        %s,
        %s
    )
    returning id
    """
    cursor.execute(insert_query,(
        sourceObject['name'],
        sourceObject['slug'],
        sourceObject['type'],
        sourceObject['active'],
    ))

    conn.commit()
    return cursor.fetchone()[0]

def add_dashes(phone_number):
    if phone_number is None:
        return None
    # Add dashes to phone numbers for formatting (e.g., 1234567890 -> 123-456-7890)
    return f"{phone_number[:3]}-{phone_number[3:6]}-{phone_number[6:]}"
# createEventLog("lead", 99999, "event", "logData", "2025-01-09 16:03:13")

def time_es_to_utc(datestring):
    if(datestring == '0000-00-00 00:00:00' or datestring == ''):
        print(f'weird datestring found {datestring}')
        datestring = '1990-01-01 00:00:00'
    d = datetime.strptime(datestring,'%Y-%m-%d %H:%M:%S')
    pst = pytz.timezone("US/Eastern")
    esdate = pst.localize(d)
    utcdate = esdate.astimezone(pytz.utc)
    return utcdate

def convert_null(value):
    return None if value == "NULL" else value

def validate_date(string):
    if not isinstance(string, str) or string in ['0000-00-00', '0000-00-00 00:00:00', '', 'NULL']:
        return None
    try:
        valid_date = datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
        return valid_date
    except ValueError:
        try:
            valid_date = datetime.strptime(string, '%Y-%m-%d')
            return valid_date
        except ValueError:
            return None
def blank_to_none(str):
    if str == "":
        return None
    
    return str
