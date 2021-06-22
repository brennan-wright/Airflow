import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import (PythonOperator,
                                               PythonVirtualenvOperator)
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 30),
    "email": ["brennan.wright@dainese.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="12 * * * *",
    default_args=default_args,
    catchup=False,
)


def download_file_FTP(remote_path, local_path, conn_id, **kwargs):
    conn = FTPHook(ftp_conn_id=conn_id)
    remote_filepath = remote_path
    local_filepath = local_path
    for i in conn.list_directory(remote_filepath):
        if i in ['5MB.zip']:
            conn.retrieve_file(
                remote_filepath + i, local_filepath + i)
            #conn.delete_file(remote_filepath + i)
            print(i + 'Processed')
        else:
            pass


def upload_file_FTP(remote_path, local_path, conn_id, **kwargs):
    conn = FTPHook(ftp_conn_id=conn_id)
    remote_filepath = remote_path
    local_filepath = local_path
    for root, dirs, files in os.walk(local_filepath):
        for i in files:
            conn.store_file(
                remote_filepath + i, local_filepath + i)
            os.remove(local_filepath + i)
            print(i + 'Processed')
        else:
            pass


def upload_file_SFTP(remote_path, local_path, conn_id, **kwargs):
    conn = SFTPHook(ftp_conn_id=conn_id)
    remote_filepath = remote_path
    local_filepath = local_path
    print(conn.list_directory(remote_filepath))


t1 = DummyOperator(task_id="Start", retries=3, dag=dag)

t2 = PythonOperator(
    task_id='download_file_FTP',
    python_callable=download_file_FTP,
    provide_context=True,
    op_kwargs={'remote_path': '/',
               'local_path': '/opt/airflow/temp/',
               'conn_id': 'FTP_Test'
               },
    dag=dag
)

t3 = PythonOperator(
    task_id='upload_file_FTP',
    python_callable=upload_file_FTP,
    provide_context=True,
    op_kwargs={'remote_path': '/upload/',
               'local_path': '/opt/airflow/temp/',
               'conn_id': 'FTP_Test'
               },
    dag=dag
)

t4 = PythonOperator(
    task_id='upload_file_SFTP',
    python_callable=upload_file_SFTP,
    provide_context=True,
    op_kwargs={'remote_path': '/',
               'local_path': '/opt/airflow/temp/',
               'conn_id': 'SFTP_Test'
               },
    dag=dag
)

t5 = PostgresOperator(
    task_id="create_pet_table",
    postgres_conn_id="postgres_default",
    sql="sql/pet_schema.sql",
    dag=dag
)

t6 = DummyOperator(task_id="End", retries=3, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
