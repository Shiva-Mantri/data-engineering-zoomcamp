import os

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from FHV_script_getdata_bulkload import ingest_callable

local_workflow = DAG(
    dag_id='FHV_DAG_Wk2',
    schedule_interval='0 6 2 * *',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    max_active_runs=3,
    concurrency=3
)

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

## FHV
url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
outputfile = AIRFLOW_HOME + '/output_fhv_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
tablename = 'for_hire_vehicle_{{ execution_date.strftime(\'%Y_%m\') }}'

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