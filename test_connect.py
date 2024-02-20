import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os, json

csv_path = '/CSV'
csv_file = 'laptop.csv'
file_path = os.path.join(csv_path, csv_file) 

cred_path = '/configs'
cred_file = 'cred.json'
cred_path = os.path.join(cred_path, cred_file) 

print(cred_path)
cred_parsed = json.loads(cred_path)

print( cred_parsed['username'])

# conn = pg.connect(
#     database = 'crypto_daily',
#     host = '192.168.0.10',
#     user = cred_parsed['username'],
#     password = cred_parsed['password'],
#     port = 5432
#     )


# cursor = conn.cursor()
# output = cursor.execute("CREATE TABLE IF NOT EXISTS ")

# with open(csv_path)