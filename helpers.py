import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
import pytz
import re

conn = psycopg2.connect(
    dbname="v1",
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
    result = cursor.fetchall()
    return result[0] if result else None

def get_user_phone_by_email(email):
    if(email == None):
        return None
    query = sql.SQL(f"SELECT personal_phone_number FROM users WHERE email = '{email}'")
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

def get_phone_number_from_email(email, type = "user"):
    if(email == None):
        return None
    if(type == "user"):
        query = sql.SQL(f"SELECT personal_phone_number FROM users WHERE email = '{email}'")
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None
    if(type == "customer"):
        query = sql.SQL(f"SELECT mobile_phone FROM customer_contact_information WHERE customer_id = (SELECT id FROM customers WHERE email = '{email}' LIMIT 1) order by id DESC")
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None
        
    if(type == "lead"):
        query = sql.SQL(f"SELECT phone_number FROM leads WHERE email = '{email}' order by id DESC LIMIT 1")
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None

def get_customer_name(id):
    if(id == None):
        return None
    query = sql.SQL(f"select concat(first_name,' ', last_name) as full_name from customers WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchall()
    return result[0] if result else None

def get_customer_email(id):
    if(id == None):
        return None
    query = sql.SQL(f"select email from customers WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchall()
    return result[0] if result else None

def get_lead_name(id):
    if(id == None):
        return None
    query = sql.SQL(f"select concat(first_name,' ', last_name) as full_name from customers WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchall()
    return result[0] if result else None

def get_lead_email(id):
    if(id == None):
        return None
    query = sql.SQL(f"select email from customers WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchall()
    return result[0] if result else None


def get_lead_details(id):
    if(id == None):
        return None
    query = sql.SQL(f"SELECT * FROM leads WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchone()
    return {
        "fields":[field[0] for field in cursor.description],
        "data": result
    } if result else None



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

def get_user_id_by_email(value):
    """
    Fetch the related ID from a given table and column, or return None if not found.
    """
    if(value == None or value == ''):
        return None
    
    query = "SELECT id FROM users WHERE email = %s order by id DESC"
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

def extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    if match:
        return match.group(0)
    return None

def add_dashes(phone_number, type = "user"):
    raw_phone_number = phone_number
    
    if '@' in phone_number:
        email = extract_email(phone_number)
        return get_phone_number_from_email(email, type) or email
    if phone_number is None or phone_number in ["", "NULL"]:
        return None

    # Remove country code +1 or 1 if present
    if phone_number.startswith("+1"):
        phone_number = phone_number[2:]
    elif phone_number.startswith("1"):
        phone_number = phone_number[1:]
    phone_number = phone_number.replace("-", "")
    phone_number = re.sub(r'\D', '', phone_number)
    # Add dashes to phone numbers for formatting (e.g., 1234567890 -> 123-456-7890)
    phone_number = f"{phone_number[:3]}-{phone_number[3:6]}-{phone_number[6:]}"
    if(phone_number == '--'):
        return  get_user_phone_by_email(raw_phone_number)
    return phone_number
# createEventLog("lead", 99999, "event", "logData", "2025-01-09 16:03:13")

def time_es_to_utc(datestring):
    if isinstance(datestring, datetime):
        return datestring
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
        return time_es_to_utc(valid_date)
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