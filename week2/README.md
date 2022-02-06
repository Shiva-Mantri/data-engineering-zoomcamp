
## Data Loading using Postgres copy_from
- Using file iteration and data load takes **8 mins**. 
- **Using copy_from takes 30sec/file**.
- You would need to define dtype for any columns you want to setup in a difference format than what is automatically deduced.

```Python
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
        name=table_name, con=engine, if_exists='replace',
        dtype={
            "trip_distance" : FLOAT,
            "tpep_pickup_datetime" : TIMESTAMP, 
            "tpep_dropoff_datetime" : TIMESTAMP,
            "fare_amount" : FLOAT,
            "extra" : FLOAT,
            "mta_tax" : FLOAT,
            "tip_amount" : FLOAT,
            "tolls_amount" : FLOAT, 
            "improvement_surcharge" : FLOAT,
            "total_amount" : FLOAT,
            "congestion_surcharge" : FLOAT
        }
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
    #with open('output_2021-03.csv', 'r') as f:
        next(f) # Skip the header row.
        #cur.copy_from(f, table_name, sep=',')
        cur.copy_from(f, table_name, sep=',', null='', columns=['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance', 'RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge'])
        #cur.copy_from(f, 'yellow_taxi_2021_03', sep=',', columns=['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance', 'RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge'])

    conn.commit()
    t_end = time()
    print(f'Completed loading data from file')

    print('Total time taken - %.3f second' % (t_end - t_start))
```
