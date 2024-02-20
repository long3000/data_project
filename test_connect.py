import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os, json, csv

def header_transform(header):
    return list(map(lambda x: x.lower().replace(" ", "_"), header))

csv_path = 'CSV'
csv_file = 'Laptops.csv'
file_path = os.path.join(csv_path, csv_file) 

cred_path = 'configs'
cred_file = 'cred.json'
cred_path = os.path.join(cred_path, cred_file) 

with open(cred_path) as cred:
    parsed_cred = json.load(cred)

conn = pg.connect(
    database = 'crypto_daily',
    host = '192.168.0.10',
    user = parsed_cred['username'],
    password = parsed_cred['password'],
    port = 5432
    )


with open(file_path, newline='') as f:
    csv_reader = csv.reader(f)
    csv_header = next(csv_reader)


transformed_header = header_transform(csv_header)
