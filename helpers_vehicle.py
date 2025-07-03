from helpers import *

def get_or_create_make(make, created_at, updated_at):
    cursor.execute("SELECT id FROM vehicle_makes where lower(name) = lower(%s)",[make])    
    id = cursor.fetchone()
    if(id):
        return id[0]
    
    sql = """
        INSERT INTO vehicle_makes(
            name,
            created_at,
            updated_at
        )
        VALUES(
            %s,
            %s,
            %s
        )
        RETURNING id
    """
    cursor.execute(sql, [make, created_at,updated_at])
    conn.commit()
    id = cursor.fetchone()

def get_or_create_type(type, created_at, updated_at):
    if(type == None):
        return None
    cursor.execute("SELECT id FROM vehicle_types where lower(name) = lower(%s)",[type])    
    id = cursor.fetchone()
    if(id):
        return id[0]
    sql = """
        INSERT INTO vehicle_types(
            name,
            created_at,
            updated_at
        )
        VALUES(
            %s,
            %s,
            %s
        )
        RETURNING id
    """
    cursor.execute(sql, [type, created_at,updated_at])
    conn.commit()
    id = cursor.fetchone()

def get_or_create_model(model, make_id, created_at, updated_at):
    cursor.execute("SELECT id FROM vehicle_models where lower(name) = lower(%s) and make_id = %s",[model, make_id])    
    id = cursor.fetchone()
    if(id):
        return id[0]
    sql = """
        INSERT INTO vehicle_models(
            name,
            make_id,
            created_at,
            updated_at
        )
        VALUES(
            %s,
            %s,
            %s,
            %s
        )
        RETURNING id
    """
    cursor.execute(sql, [model, make_id, created_at,updated_at])
    conn.commit()
    id = cursor.fetchone()
    return id[0]



def get_vehicle_id(data):
    created_at = time_pacific_to_utc(data['createdAt']) #.strftime('%Y-%m-%d %H:%M:%S')
    updated_at = time_pacific_to_utc(data['updatedAt']) #.strftime('%Y-%m-%d %H:%M:%S')

    make_id = get_or_create_make(data['make'],created_at,updated_at)
    model_id = get_or_create_model(data['model'],make_id, created_at, updated_at)
    sql = """
        SELECT id FROM vehicles WHERE vin = %s AND model_id = %s AND make_id = %s AND year = %s
    """
    cursor.execute(sql, [data['VIN'],model_id, make_id, data['year']])
    id = cursor.fetchone()
    if id:
        return id[0]
    else:
        return None
    
def get_vehicle_by_vin(vin):
    if(len(vin) == 0):
        return None
    sql = """
        SELECT id FROM vehicles WHERE lower(vin) = %s
    """
    cursor.execute(sql, [vin.lower()])
    id = cursor.fetchone()
    if id:
        return id[0]
    else:
        return None