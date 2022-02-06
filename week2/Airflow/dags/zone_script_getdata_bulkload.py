#!/usr/bin/env python
# coding: utf-8

# Use Connection and SQL Load - https://blog.panoply.io/how-to-load-pandas-dataframes-into-sql
# Convert String to DateTime
import os

from time import time

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import TIMESTAMP, FLOAT
import psycopg2


def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name, csv_file)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection = engine.connect()
    print(f'Connection Successful')

    # read first 3 records and to get header info  
    df = pd.read_csv(csv_file, nrows=3)

    # Create Table - Drop if exists
    print(f'If table exists, dropping and recreating')
    df.head(0).to_sql(
        name=table_name, con=engine, if_exists='replace'
    )
    print(f'Table creation completed')
    
    # Close sqlalchemy connection
    connection.close()
    print(f'Connection Closed')

    # Bulk load data
    conn = psycopg2.connect(f'host={host} port={port} dbname={db} user={user} password={password}')
    cur = conn.cursor()
    print(f'Connection Successful with psycopg2')

    print(f'Loading data from file..')
    t_start = time()
    with open(csv_file, 'r') as f:
        next(f) # Skip the header row.
        cur.copy_from(f, table_name, sep=',', null='', columns=['LocationID','Borough','Zone','service_zone'])

    conn.commit()
    t_end = time()
    print(f'Completed loading data from file')

    print('Total time taken - %.3f second' % (t_end - t_start))