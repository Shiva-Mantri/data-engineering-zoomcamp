import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "ncaa-data-eng-zc-2022")
BUCKET = "ncaa-datalake-bucket_ncaa-data-eng-zc-2022"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ncaa_datawarehouse_bigquery')


home_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
data_dir = home_dir + '/data'
data_file_ext = '.csv'


# Store RAW data into GCS Bucket - DataLake
def upload_files_to_gcs (dir_path, file_ext, bucket):
    # Get list of files in directory
    files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

    # Loop through files and upload to GCS
    for file in files:
        if file.endswith(file_ext):
            upload_to_gcs(bucket, f'raw/{file}', dir_path + '/' + file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    dag_id='ncaa_DataEng_DAG',
    schedule_interval='0 6 2 * *',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 2, 1),
    max_active_runs=3,
    concurrency=3
)

with local_workflow:

    bash_task = BashOperator(
        task_id='bash',
        bash_command=f'hostname'
    )

    # Upload RAW data to GCS Bucket - DataLake
    upload_files_to_gcs_task = PythonOperator(
        task_id="upload_files_to_gcs",
        python_callable=upload_files_to_gcs,
        op_kwargs={
            "dir_path": f"{data_dir}",
            "file_ext": f"{data_file_ext}",
            "bucket": BUCKET
        },
    )

    bash_task >> upload_files_to_gcs_task