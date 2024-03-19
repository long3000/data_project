import psycopg2.extras 
import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os, json, csv

csv_path = 'CSV'
csv_file = 'dim.plans.csv'
file_path = os.path.join(csv_path, csv_file) 

table_name = 'dim_plans'

cred_path = 'configs'
cred_file = 'cred.json'
cred_path = os.path.join(cred_path, cred_file) 

with open(cred_path) as cred:
    parsed_cred = json.load(cred)

conn = pg.connect(
    database = 'dim_test',
    host = '192.168.0.10',
    user = parsed_cred['username'],
    password = parsed_cred['password'],
    port = 5432
    )

conn.autocommit = True
cursor = conn.cursor() 
 


# table_statement = """CREATE TABLE IF NOT EXISTS {}""".format(table_name)

drop_statement = """DROP TABLE IF EXISTS {}""".format(table_name)

try:
    cursor.execute(drop_statement)
except:
    print('Error has occurred while dropping table')
else:
    print("Successfully dropped table [{}]".format(table_name))

# try:
#     cursor.execute(table_statement)
# except:
#     print('Error has occurred while creating new table ')
# else:
#     print("Successfully create table [{}]".format(table_name))
    
with open(file_path, "r") as f: 
    next(f)
    cursor.copy_from(f, table_name, sep=",", null = "") 


print("Finished copying")

conn.commit() 
cursor.close() 
conn.close() 