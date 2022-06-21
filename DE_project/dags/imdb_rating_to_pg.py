import pandas as pd
import airflow.utils.dates

from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from os import environ

args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 8),
    'provide_context': True,
}

POSTGRES_USER = environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = environ.get("POSTGRES_HOST")
POSTGRES_DEFAULT_DB = environ.get("POSTGRES_DEFAULT_DB")
POSTGRES_PORT = environ.get("POSTGRES_PORT")

MINIO_ACCESS_KEY = environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = environ.get("MINIO_SECRET_KEY")
MINIO_HOST = environ.get("MINIO_HOST")


def get_minio():
    """Returns connection with Minio"""
    return Minio(
        endpoint=MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def read_from_csv(file_name, bucket):
    minio_client = get_minio()
    minio_obj = minio_client.get_object(bucket, file_name)
    pd_df = pd.read_csv(
        minio_obj,
        lineterminator='\n',
        sep='\t',
        header=0, dtype=object, na_filter=False
    )
    return pd_df

def get_postgres(db_name):
    """Establishes postgres connection"""
    conn_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{db_name}'
    db = create_engine(conn_string)
    conn = db.connect()
    return conn

def db_minio_to_postgres(database, table, if_exists='replace'):
    conn = get_postgres(db_name="tmdb")
    database.to_sql(table, con=conn, if_exists=if_exists, index=False)

def imdb_rating_to_pg_task():
    print("read title_ratings.tsv")
    data_base = read_from_csv(file_name="title_ratings.tsv", bucket="imdb")

    print("put title_ratings.tsv to postgres")
    db_minio_to_postgres(database=data_base, table='title_ratings')


with DAG(
        'imdb_rating_to_pg',
        description='creates title_ratings table on postgres from title_ratings.tsv on Minio',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval=None,
        catchup=False,
        default_args=args) as dag:
    imdb_rating_to_pg = PythonOperator(task_id="imdb_rating_to_pg", python_callable=imdb_rating_to_pg_task)

