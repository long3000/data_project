import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os, json, csv

def header_transform(header):
    return list(map(lambda x: x.lower().replace(" ", "_"), header))

csv_path = 'CSV'
csv_file = 'Laptops.csv'
file_path = os.path.join(csv_path, csv_file) 

table_name = 'laptop_specs'

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

with open(file_path, newline='') as f:
    csv_reader = csv.reader(f)
    csv_header = next(csv_reader)

# CLEAN HEADER
transformed_header = header_transform(csv_header)
while("" in transformed_header):
    transformed_header.remove("")

drop_statement = """DROP TABLE IF EXISTS {}""".format(table_name)

table_statement = """
    CREATE TABLE IF NOT EXISTS {}({})
""".format(table_name, ', '.join(str(e) + ' TEXT' for e in transformed_header))
print(table_statement)

load_statement = """
    COPY {}({})
    FROM '{}'
    DELIMITER ','
    CSV HEADER;
""".format(table_name, ', '.join(transformed_header[1:-1]), str(file_path))


try:
    cursor.execute(drop_statement)
except:
    print('Error has occurred while dropping table')
else:
    print("Successfully dropped table [{}]".format(table_name))

try:
    cursor.execute(table_statement)
except:
    print('Error has occurred while creating new table ')
else:
    print("Successfully create table [{}]".format(table_name))
    try:
        # cursor.execute(load_statement)
        with open(file_path,'r') as f:
            f_read = csv.reader(f)
            next(f_read)
            for idx, row in enumerate(f_read):
                insert_statement = """
                    INSERT INTO {}({}) VALUES ({})
                """.format(table_name, 
                           ', '.join(transformed_header), 
                           ', '.join(repr(str(e)) for e in row[1:])
                        )
                cursor.execute(insert_statement)
    except pg.Error as e:
        print('Error has occurred while loading data into table [{}]'.format(table_name))
        print(e)
    else:
        print("Successfully loading data into table [{}]".format(table_name))



conn.commit() 
conn.close() 