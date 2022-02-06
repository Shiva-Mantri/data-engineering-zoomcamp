import os

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from zone_script_getdata_bulkload import ingest_callable

local_workflow = DAG(
    dag_id='Zone_DAG_Wk2',
    schedule_interval='0 6 2 * *',
    # latest data is good enough
    start_date=datetime(2022, 1, 1),
    # cannot run in parallel. one process can clean other
    max_active_runs=1, 
    concurrency=1
)

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

## ZONE
url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
outputfile = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
tablename = 'taxi_zone_lookup'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {url} > {outputfile}'
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=tablename,
            csv_file=outputfile
        ),
    )

    cleancsv_task = BashOperator(
        task_id='cleancsv',
        bash_command=f'rm -f {outputfile}'
    )

    wget_task >> ingest_task >> cleancsv_task