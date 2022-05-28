"""Module establishes connection with Minio service, takes csv files from 'newbucket'
and creates tables inside Postgres database"""
from os import environ
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio


def get_minio_client(host, access, secret):
    """Gets Minio client"""

    return Minio(
        host,
        access_key=access,
        secret_key=secret,
        secure=False
    )


if __name__ == '__main__':
    # Crete minio client
    minio_host = environ.get("MINIO_HOST")
    minio_key = environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = environ.get("MINIO_SECRET_KEY")
    minio_bucket = environ.get("MINIO_BUCKET")
    minio_client = get_minio_client(minio_host, minio_key, minio_secret_key)

    # Establish connection to DB
    db_host = environ.get("PG_HOST")
    user = environ.get("POSTGRES_USER")
    password = environ.get("POSTGRES_PASSWORD")
    db_name = environ.get("POSTGRES_DB")
    conn_string = f'postgresql://{user}:{password}@{db_host}/{db_name}'
    db = create_engine(conn_string)
    conn = db.connect()

    # Create tables
    objects = minio_client.list_objects(minio_bucket)
    for obj in objects:
        minio_obj = minio_client.get_object(minio_bucket, obj.object_name)
        table = pd.read_csv(minio_obj)
        table_name = obj.object_name.replace('-', '_')
        table.to_sql(table_name, con=conn, if_exists='replace', index=False)
        print(table, table_name)
