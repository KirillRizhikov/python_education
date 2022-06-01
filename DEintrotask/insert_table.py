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

    # Create table
    objects = minio_client.list_objects(minio_bucket)
    table = pd.DataFrame({'date': [],
                          'state': [],
                          'region_code': [],
                          'region_denomination': [],
                          'province_code': [],
                          'province_denomination': [],
                          'province_abbreviation': [],
                          'lat': [],
                          'long': [],
                          'total_cases': [],
                          'Note': [],
                          'nuts_code_1': [],
                          'nuts_code_2': [],
                          'nuts_code_3': []})
    columns = table.columns.values.tolist()
    for obj in objects:
        minio_obj = minio_client.get_object(minio_bucket, obj.object_name)
        table_pd = pd.read_csv(minio_obj)
        table_pd.columns.values[:] = columns
        table = pd.concat([table, table_pd])
    table.to_sql('covid_ita', con=conn, if_exists='replace', index=False)
