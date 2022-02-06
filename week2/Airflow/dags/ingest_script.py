#!/usr/bin/env python
# coding: utf-8

# Use Connection and SQL Load - https://blog.panoply.io/how-to-load-pandas-dataframes-into-sql
# Convert String to DateTime
import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name, csv_file)

    csv_name = csv_file

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print(f'Connection Successful')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print(f'If table exists, drop successful')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    print(f'Appending')

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    from sqlalchemy.types import BigInteger, TIMESTAMP, FLOAT, VARCHAR        