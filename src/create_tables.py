import csv
import glob
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Go up one directory
parent_dir = os.path.dirname(script_dir)

# Load environment variables
load_dotenv()

# Connect to your postgres DB
conn = psycopg2.connect(
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Query to create the table
table_create_query = sql.SQL("""
    -- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    CREATE TABLE IF NOT EXISTS intraday (
        -- id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
        number SERIAL PRIMARY KEY,
        symbol VARCHAR,
        -- number SERIAL,
        time TIMESTAMP,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC,
        CONSTRAINT unique_time_symbol UNIQUE (time, symbol)
    );
""")

# Execute the query
cur.execute(table_create_query)

filepaths = glob.glob(os.path.join(parent_dir, 'data/tsla/202104', '*.csv'))
filepaths += glob.glob(os.path.join(parent_dir, 'data/tsla/202305', '*.csv'))

# Insert statement with unique constraint on (time, symbol) combination
insert_statement = sql.SQL("""
    INSERT INTO intraday (symbol, time, open, high, low, close, volume) 
    VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (time, symbol) DO NOTHING;
""")

# Iterate over the filepaths
for filepath in filepaths:
    print('file:', filepath)
    with open(filepath, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)  # Skip the header row

        # Collect the rows to be inserted
        rows_to_insert = []
        for row in reader:
            row = ['TSLA'] + row
            rows_to_insert.append(row)

        # Batch insert the rows using execute_batch()
        execute_batch(cur, insert_statement, rows_to_insert)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
