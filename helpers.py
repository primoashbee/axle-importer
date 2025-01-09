import json
import psycopg2
from psycopg2 import sql

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

def getRelatedId(table, column, value):
    """
    Fetch the related ID from a given table and column, or return None if not found.
    """
    query = sql.SQL("SELECT id FROM {table} WHERE {column} = %s").format(
        table=sql.Identifier(table),
        column=sql.Identifier(column)
    )
    cursor.execute(query, (value,))
    result = cursor.fetchone()
    return result[0] if result else None




def add_dashes(phone_number):
    # Add dashes to phone numbers for formatting (e.g., 1234567890 -> 123-456-7890)
    return f"{phone_number[:3]}-{phone_number[3:6]}-{phone_number[6:]}"
# createEventLog("lead", 99999, "event", "logData", "2025-01-09 16:03:13")