import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
import pytz
import re
import logging
import pymysql
import redis
# host = "axle-prd-usea1-crm-db.cjjspmdvfxbj.us-east-1.rds.amazonaws.com"
mode = "local"
# host = "awseb-e-3rznwcyhm9-stack-awsebrdsdatabase-hmh4mlz3imqs.cjjspmdvfxbj.us-east-1.rds.amazonaws.com",
# QA
# conn = psycopg2.connect(
#     dbname="prod_bup_20250330",
#     user="postgres",
#     password="GAtCxz9a2zGNQQ",
#     host=host
#     port=5432
# )

# if(host == "axle-prd-usea1-crm-db.cjjspmdvfxbj.us-east-1.rds.amazonaws.com"):


#LOCAL 
if(mode == "prod"):
    conn = psycopg2.connect(
        dbname="axle-july-5",
        user="postgres",
        password="Hbo2lUswKWgywwZ",
        host="axle-prd-usea1-crm-db.cjjspmdvfxbj.us-east-1.rds.amazonaws.com",
        port=5432
    )

if(mode == "qa"):
    conn = psycopg2.connect(
        dbname="prod_bup_20250330",
        user="postgres",
        password="GAtCxz9a2zGNQQ",
        host="awseb-e-3rznwcyhm9-stack-awsebrdsdatabase-hmh4mlz3imqs.cjjspmdvfxbj.us-east-1.rds.amazonaws.com",
        port=5432
    )

if(mode == "local"):
    conn = psycopg2.connect(
        dbname="axle-crm-prod",
        user="postgres",
        password="",
        host="localhost",
        port=5432
    )
# )
    

#LOCAL 
# if(mode == "prod"):
#     conn = pymysql.connect(
#         database="axle-crm-prod",
#         user="postgres",
#         password="Hbo2lUswKWgywwZ",
#         host="axle-prd-usea1-crm-db.cjjspmdvfxbj.us-east-1.rds.amazonaws.com",
#         port=5432
#     )

# if(mode == "qa"):
#     conn = pymysql.connect(
#         database="prod_bup_20250330",
#         user="postgres",
#         password="GAtCxz9a2zGNQQ",
#         host="awseb-e-3rznwcyhm9-stack-awsebrdsdatabase-hmh4mlz3imqs.cjjspmdvfxbj.us-east-1.rds.amazonaws.com",
#         port=5432
#     )

# if(mode == "local"):
#     conn = pymysql.connect(
#         database="axle-bup",
#         user="postgres",
#         password="",
#         host="localhost",
#         port=5432
#     )

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
    RETURNING id
    """
    cursor.execute(insert_query, (
        data["user_type"], data["user_id"], data["event"], data["auditable_type"], data["auditable_id"],
        data["old_values"], data["new_values"], data["url"], data["ip_address"], data["user_agent"],
        data["created_at"], data["updated_at"]
    ))

    conn.commit()
    result = cursor.fetchone()
    if result:
        return result[0]
    return None
    
    
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

def get_lead_info(id):
    if(id == None):
        return None
    query = sql.SQL(f"SELECT * FROM leads WHERE id = {id}")
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        fields = [field[0] for field in cursor.description]
        data = dict(zip(fields, result))
        return data
    else:
        return None
    
def get_lead_info_by_migration_source_id(migration_source_id):
    if(id == None):
        return None
    query = sql.SQL(f"select id, concat(first_name,' ', last_name) as full_name from leads WHERE migration_source_id ='{migration_source_id}'")
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        fields = [field[0] for field in cursor.description]
        data = dict(zip(fields, result))
        return data
    else:
        return None
    
def get_customer_info_by_migration_source_id(migration_source_id):
    if(id == None):
        return None
    query = sql.SQL(f"select id, concat(first_name,' ', last_name) as full_name from customers WHERE migration_source_id = '{migration_source_id}'")
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        fields = [field[0] for field in cursor.description]
        data = dict(zip(fields, result))
        return data
    else:
        log(f"Customer with migration_source_id {migration_source_id} not found.")
        return None

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



# def getRelatedId(table, column, value):
#     """
#     Fetch the related ID from a given table and column, or return None if not found.
#     """
#     if(value == None or value == '' or value == 'NULL' or value == "NULL"):
#         return None
    
#     query = sql.SQL("SELECT id FROM {table} WHERE {column} = %s order by id DESC").format(
#         table=sql.Identifier(table),
#         column=sql.Identifier(column)
#     )


#     cursor.execute(query, (value,))
    
#     result = cursor.fetchone()
#     # if(result != None):
#     #     print(cursor.mogrify(query.as_string(cursor), (value,)).decode("utf-8"))

#     return result[0] if result else None

def getRelatedId(table, column, value):
    query = f"SELECT id FROM {table} WHERE {column} = %s order by id DESC"
    cursor.execute(query, (value,))
    result = cursor.fetchone()
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
    if phone_number is None:
        return None
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

def time_pacific_to_utc(datestring):
    if isinstance(datestring, datetime):
        return datestring

    if datestring in ('0000-00-00 00:00:00', '', 'NULL', None):
        datestring = '1990-01-01 00:00:00'

    # Parse the string into naive datetime
    d = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')

    # Localize to old DB timezone (UTC-8, no DST)
    old_tz = pytz.FixedOffset(-480)  # UTC-8 is -480 minutes
    localized = old_tz.localize(d)

    # Convert to UTC
    return localized.astimezone(pytz.utc)

def time_pacific_to_utc(datestring):
    if isinstance(datestring, datetime):
        return datestring

    if datestring in ('0000-00-00 00:00:00', '', 'NULL', None):
        datestring = '1990-01-01 00:00:00'

    # Parse the string into a naive datetime
    d = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')

    # Set the original timezone to Pacific Time (no DST assumed)
    pacific = pytz.timezone("US/Pacific")
    localized_date = pacific.localize(d)

    # Convert to UTC
    utc_date = localized_date.astimezone(pytz.utc)

    return utc_date

def convert_null(value):
    return None if value == "NULL" else value

def validate_date(string):
    if not isinstance(string, str) or string in ['0000-00-00', '0000-00-00 00:00:00', '', 'NULL']:
        return None
    try:
        valid_date = datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
        return time_pacific_to_utc(valid_date)
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

def get_state_name(state):
    file_path = "states.json"
    if(state == None or state == ''):
        return ''
    with open(file_path, "r") as file:
        states = json.load(file)
        return states.get(state, state)

def log(value):
    log_file = "app.log"
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )
    logging.info(value)

def get_linked_customer_by_lead_id(lead_id):
    query = """
    SELECT * FROM lead_customer_linking_migration WHERE lead_migration_id = %s
    """
    cursor.execute(query, (lead_id,))
    result = cursor.fetchone()
    return result if result else None

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PASSWORD = None

# Connect to Redis
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

def cache_users():
    query = "SELECT id, migration_source_id, lower(email), name FROM users"
    cursor.execute(query)
    rows = cursor.fetchall()  # Fetch all rows from the query

    if not rows:
        print("No rows found in the users table.")
        return

    for row in rows:
        # Sanitize data to handle None values
        sanitized_row = {
            'id': row[0] if row[0] is not None else '',
            'migration_source_id': row[1] if row[1] is not None else '',
            'email': row[2] if row[2] is not None else '',
            'name': row[3] if row[3] is not None else ''
        }

        # print(f"Caching user: {sanitized_row}")  # Log the sanitized user data
        redis_client.hset(f"user:{sanitized_row['email']}", mapping=sanitized_row)

def get_user_by_email(email):
    """
    Retrieve user data from Redis using email.
    """
    user_data = redis_client.hgetall(f"user:{email.lower()}")
    if user_data:
        return user_data
    else:
        log(f"User with email {email} not found in Redis.")
        return None
    
def check_if_user_exists(email):
    """
    Check if a user exists in Redis by email.
    """
    return redis_client.exists(f"user:{email}") > 0