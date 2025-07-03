from helpers import *


def run(): 
    sql = "SELECT * from users where id = 594"
    cursor.execute(sql)
    result = cursor.fetchall()
    print("result: ", result)
    print("test script");

run();