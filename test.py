import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os, json, csv

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

test_statement  = """
select * from test_table;
"""

cursor.execute(test_statement)
data = cursor.fetchall()

print(data)