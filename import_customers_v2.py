import csv
import os
from  helpers import *
from multiprocessing import Pool
import uuid
from tqdm import tqdm
from datetime import datetime


def process_row(row):
    customerObject = formatCustomerObject(row)
    if customerObject is False:
        log(f"Skipping row with customerID {row['customerID']} due to missing or invalid data.")
        return False
    return process_customer(customerObject, customerObject['crm_customer_id']) 
    
    
def process_customer(customerObject, customerId=None):

    if(customerId is None):
        return create_customer(customerObject)
    update_customer(customerObject, customerId)
    return create_customer(customerObject)

def formatCustomerObject(row):
    row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
    if(row['customerID'] is None or row['customerID'] == ''):
        return False
    customerId = getRelatedId("customers","migration_source_id", row['customerID'])
    

    # if customerId is not None:
    #     return False
    
    if(row['source'].lower() == "website"):
        row['source'] = "Web"
    row['source'] = sluggify(row['source']) + "-customers"
    source_id = getRelatedId("sources","slug", row['source'])
    
    # created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    created_at = "2024-01-01 00:00:00"
    
    if(validate_date(row['createdAt']) != None or validate_date(row['updatedAt']) != None) :
        created_at = row['createdAt']
    
    
    customerObject = {
        # "id": customerId,
        "crm_customer_id": customerId or None,
        "customer_system_id" : str(uuid.uuid4()),
        "first_name": row['firstName'],
        "middle_name": None,
        "last_name": row['lastName'] or '',
        "email": row['email'].lower() if row['email'] is not None else None,
        "birth_date" : validate_date(row['dateOfBirth']),
        "driver_license_id" : row['driverLicenseID'],
        "status": getRelatedId("sources", "slug", row['source']),
        "created_at" : created_at,
        "updated_at" : created_at,
        "source_id"  : source_id,
        "deal_status" : row['status'],
        "migration_source_id": row['customerID'],
        "axledesk_id": row['dscCustomerID'] or None,
        "provider_response": json.dumps(row),
        "contact" : {
            "mobile_phone": add_dashes(row['phone']),
            "home_phone": None,
            "work_phone": None
        },
        "address" : {
            "street_address": row['streetAddress'] or '',
            "address_line_1": row['streetAddress'],
            "address_line_2": row['streetAddress'],
            "city": row['city'] or '',
            "state": get_state_name(row['state']),
            "zip_code": row['postalCode'] or '',
        }
    }


    originalSource = row['source']
    if(source_id == None):
        sourceObject = {
            "name": originalSource,
            "slug": sluggify(originalSource) + "-customers",
            "type": "customers",
            "active": True
        }
        source_id = createSource(sourceObject)
        customerObject['source_id'] = source_id  
    return customerObject
    
    
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
    provider_response,
    axledesk_id,
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
        customerObject['provider_response'],
        customerObject['axledesk_id'],
        customerObject['created_at'],
        customerObject['updated_at'],
    ))
    conn.commit()
    customerId = cursor.fetchone()
    contactId = upsert_contact(customerObject['contact'], customerId)
    addressId = upsert_address(customerObject['address'], customerId)
    return bool(customerId)

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
    # print("updating customer contact")
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
        # print('inserting new address')
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

    # print("updating customer address")
    
    conn.commit()
    return addressId

def update_customer(customerObject, customerId):

    update_query = """
    UPDATE customers SET 
    first_name = %s, 
    last_name= %s, 
    email = %s, 
    birth_date = %s, 
    driver_license_id= %s, 
    source_id = %s, 
    deal_status = %s,
    provider_response = %s,
    migration_source_id = %s,
    axledesk_id = %s
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
        customerObject['provider_response'],
        f"_{customerObject['migration_source_id']}",
        customerObject['axledesk_id'],
        customerObject['migration_source_id']
    ))

    conn.commit();
    contactId = upsert_contact(customerObject['contact'], customerId)
    addressId = upsert_address(customerObject['address'], customerId)
    return True
    
def get_source_csv():
    return "files/customers.csv"

def read_csv():
    source_csv = get_source_csv()
    i = 0
    with open(source_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = list(csv.DictReader(file))
        with Pool(processes=10) as pool:
            with tqdm(total=len(reader), desc="Processing rows") as pbar:
                result = []
                for res in pool.imap(process_row, reader):
                    result.append(res)
                    pbar.update()
            
        succeeded = len([item for item in result if item == True])
        print(f'{succeeded} of {len(result)} rows processed successfully')
            # result =pool.map(process_row, reader)
            
            # succeeded = len([item for item in result if item == True])
            # print(f'{succeeded} of {len(result)} imported')
    # createdAt = '2018-03-29 14:17:05'
    # print(validate_date(createdAt))

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Starting process')
    read_csv()