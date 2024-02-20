import psycopg2 as pg 
from psycopg2 import Error as pg_error
import pandas as pd
import os

csv_path = '/CSV'
csv_file = 'laptop.csv'
file_path = os.path.join(csv_path, csv_file) 

print(file_path)

conn = pg.connect(
    database = 'crypto_daily',
    host = '192.168.0.10',
    user = 'nate.nguyen',
    password = 'fantasy88',
    port = 5432
    )


# cursor = conn.cursor()
# output = cursor.execute("CREATE TABLE IF NOT EXISTS ")

# with open(csv_path)