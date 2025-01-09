import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json

class PhoneNumberRepository:
    @staticmethod
    def add_dashes(phone_number):
        # Add dashes to phone numbers for formatting (e.g., 1234567890 -> 123-456-7890)
        return f"{phone_number[:3]}-{phone_number[3:6]}-{phone_number[6:]}"

def get_related_id(table, column, value, cursor):
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

def import_data_to_pg(source_csv, db_config):
    """
    Import data from a CSV file and insert it into a PostgreSQL database.
    """
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            print(row);
            caller_user_id = get_related_id("users", "migration_source_id", row["userID"], cursor)
            caller_user_id = caller_user_id or row["userID"]

            data = {
                "lead_id": None,
                "customer_id": None,
                "caller_user_id": caller_user_id,
                "from_number": "+13017533306",
                "to_number": PhoneNumberRepository.add_dashes(row["phone"]),
                "provider_response": json.dumps({}),
                "provider_response_id": 111111,
                "direction": "outbound",
                "status": row["status"],
                "end_call_status": row["description"],
                "created_at": datetime.strptime(row["createdAt"], "%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.strptime(row["createdAt"], "%Y-%m-%d %H:%M:%S"),
            }

            if row["attendant_type"] == "lead":
                data["lead_id"] = get_related_id("leads", "migration_source_id", row["attendant_id"], cursor)

            if row["attendant_type"] == "customer":
                data["customer_id"] = get_related_id("customers", "migration_source_id", row["attendant_id"], cursor)

            # Define the insert query
            insert_query = """
            INSERT INTO phone_number_sms_logs (
               phone_number_id, from, to, content, is_inbound, provider_response, provider_message_id, occured_at, created_at, updated_at, status, sent_by
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
            )
            """

            # Pass dictionary values as a tuple
            cursor.execute(insert_query, (
                data["lead_id"],
                data["customer_id"],
                data["caller_user_id"],
                data["from_number"],
                data["to_number"],
                data["provider_response"],
                data["provider_response_id"],
                data["direction"],
                data["status"],
                data["end_call_status"],
                data["created_at"],
                data["updated_at"],
            ))

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print("Data successfully imported into PostgreSQL.")

# Example usage
source_csv = "files/phone_call.csv"  # Path to your source CSV file
db_config = {
    "dbname": "prod_v3_backup",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": 5432,
}
import_data_to_pg(source_csv, db_config)
