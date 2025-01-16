import csv
import os
from  helpers import *
from multiprocessing import Pool
import uuid

def process_row(row):
    customerId = getRelatedId("customers","migration_source_id", row['customerID'])
    originalSource = row['source']
    if(row['source'].lower() == "website"):
        row['source'] = "Web"
    row['source'] = sluggify(row['source']) + "-customers"
    source_id = getRelatedId("sources","slug", row['source'])
    if(validate_date(row['createdAt']) == None or validate_date(row['updatedAt']) == None) :
        return False
    customerObject = {
        "id": customerId,
        "customer_system_id" : str(uuid.uuid4()),
        "first_name": row['firstName'],
        "middle_name": None,
        "last_name": row['lastName'],
        "email" : row['email'],
        "birth_date" : validate_date(row['dateOfBirth']),
        "driver_license_id" : row['driverLicenseID'],
        "status": getRelatedId("sources", "slug", row['source']),
        "created_at" : validate_date(row['createdAt']),
        "updated_at" : validate_date(row['updatedAt']),
        "source_id"  : source_id,
        "deal_status" : row['status'],
        "migration_source_id": row['customerID'],
        "contact" : {
            "mobile_phone": add_dashes(row['phone']),
            "home_phone": None,
            "work_phone": None
        },
        "address" : {
            "street_address": row['streetAddress'],
            "address_line_1": row['streetAddress'],
            "address_line_2": row['streetAddress'],
            "city": row['city'],
            "state": row['state'],
            "zip_code": row['postalCode'],
        }
    }

    if(source_id == None):
        sourceObject = {
            "name": originalSource,
            "slug": sluggify(originalSource) + "-customers",
            "type": "customers",
            "active": True
        }
        source_id = createSource(sourceObject)
        customerObject['source_id'] = source_id
        

    if(customerId == None):
        # print(customerObject)
        create_customer(customerObject)
    else:
        update_customer(customerObject)
    return True
    
def create_customer(customerObject):
    insert_query = """
    INSERT INTO customers (
    customer_system_id,
    first_name,
    last_name,
    email,
    birth_date,
    driver_license_id,
    source_id,
    deal_status,
    migration_source_id,
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
    %s
    )
    returning id
    """

    cursor.execute(insert_query, (
        customerObject['customer_system_id'],
        customerObject['first_name'],
        customerObject['last_name'],
        customerObject['email'],
        customerObject['birth_date'],
        customerObject['driver_license_id'],
        customerObject['source_id'],
        customerObject['deal_status'],
        customerObject['migration_source_id'],
        customerObject['created_at'],
        customerObject['updated_at'],
    ))
    conn.commit()
    customerId = cursor.fetchone()[0]
    contactId = upsert_contact(customerObject['contact'], customerId)
    addressId = upsert_address(customerObject['address'], customerId)
    return

def upsert_contact(contactObject, customerId):
    contactId = getRelatedId("customer_contact_information", 'customer_id', customerId )
    if(contactId == None):
        insert_query = """
        INSERT INTO customer_contact_information (
            customer_id, mobile_phone, work_phone, home_phone
        ) VALUES (
            %s,
            %s,
            %s,
            %s
        )
        RETURNING id
        """
        cursor.execute(insert_query, (
            customerId,
            contactObject['mobile_phone'],
            contactObject['work_phone'],
            contactObject['home_phone'],
        ))
        conn.commit()
        result = cursor.fetchone()
        return result[0] if result else None
    
    
    update_query = """
    UPDATE customer_contact_information 
    SET mobile_phone = %s, work_phone = %s, home_phone = %s
    WHERE id = %s
    """
    print("updating customer contact")
    cursor.execute(update_query, (
        contactObject['mobile_phone'],
        contactObject['work_phone'],
        contactObject['home_phone'],
        contactId
    ))

    conn.commit()
    return contactId
    
def upsert_address(addressObject, customerId):
    addressId = getRelatedId("customer_addresses", 'customer_id', customerId )
    if(addressId == None):
        insert_query = """
        INSERT INTO customer_addresses (
            customer_id, street_address, address_line_1, address_line_2, city, state, zip_code
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) 
        RETURNING id
        """
        cursor.execute(insert_query, (
            customerId,
            addressObject['street_address'],
            addressObject['address_line_1'],
            addressObject['address_line_2'],
            addressObject['city'],
            addressObject['state'],
            addressObject['zip_code'],
        ))
        result = cursor.fetchone()
        conn.commit()
        print('inserting new address')
        return result[0] if result else None
    

    update_query = """
    UPDATE customer_addresses 
    SET street_address = %s, address_line_1 = %s, address_line_2 = %s,
    city = %s, state = %s, zip_code = %s
    WHERE id = %s
    """

    cursor.execute(update_query, (
        addressObject['street_address'],
        addressObject['address_line_1'],
        addressObject['address_line_2'],
        addressObject['city'],
        addressObject['state'],
        addressObject['zip_code'],
        addressId
    ))

    print("updating customer address")
    
    conn.commit()
    return addressId

def update_customer(customerObject):
    update_query = """
    UPDATE customers SET 
    first_name = %s, 
    last_name= %s, 
    email = %s, 
    birth_date = %s, 
    driver_license_id= %s, 
    source_id = %s, 
    deal_status = %s 
    WHERE 
    migration_source_id = %s
    """
    cursor.execute(update_query, (
        customerObject['first_name'],
        customerObject['last_name'],
        customerObject['email'],
        customerObject['birth_date'],
        customerObject['driver_license_id'],
        customerObject['source_id'],
        customerObject['deal_status'],
        customerObject['migration_source_id']
    ))

    conn.commit();
    contactId = upsert_contact(customerObject['contact'], customerObject['id'])
    addressId = upsert_address(customerObject['address'], customerObject['id'])
    
def get_source_csv():
    return "files/customers.csv"

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