import pandas as pd
import requests
import airflow.utils.dates
import re
import psycopg2

from io import BytesIO
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from os import environ


args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 8),
    'provide_context': True,
    'retries': 100}

API_KEY = environ.get("API_KEY")
MINIO_ACCESS_KEY = environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = environ.get("MINIO_SECRET_KEY")
MINIO_HOST = environ.get("MINIO_HOST")

POSTGRES_USER = environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = environ.get("POSTGRES_HOST")
POSTGRES_DEFAULT_DB = environ.get("POSTGRES_DEFAULT_DB")
POSTGRES_PORT = environ.get("POSTGRES_PORT")


def get_start_end_date():
    date_now = datetime.now().strftime('%Y-%m-%d')
    db_start_date = Variable.get('db_start_date', default_var='2019-01-01')
    db_end_date = Variable.get('db_end_date', default_var=date_now)
    print('db start date is: ', db_start_date)
    print('db end date is: ', db_end_date)
    return db_start_date, db_end_date


def get_minio():
    """Returns connection with Minio"""
    return Minio(
        endpoint=MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def get_psycopg(database):
    conn = psycopg2.connect(
        database=database,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    conn.autocommit = True
    return conn


def get_postgres(db_name):
    """Establishes postgres connection"""
    conn_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{db_name}'
    db = create_engine(conn_string)
    conn = db.connect()
    return conn


def put_csv_to_minio(data_base, file_name, minio_bucket):
    """Puts database into proper bucket on Minio"""
    minio_client = get_minio()
    print(f"put {file_name} to {minio_bucket}")
    if minio_client.bucket_exists(minio_bucket):
        print(minio_bucket, " exists")
    else:
        minio_client.make_bucket(minio_bucket)
    csv = data_base.to_csv(index=False).encode('utf-8')
    minio_client.put_object(
        minio_bucket,
        file_name,
        data=BytesIO(csv),
        length=len(csv),
        content_type='application/csv')


def get_data_base(start_date, end_date):
    """Downloads data within defined period from TMDB API into database """
    movi_discover_api = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&' \
                        f'&language=en-US&sort_by=primary_release_date.asc&' \
                        f'primary_release_date.gte={start_date}&' \
                        f'primary_release_date.lte={end_date}'
    json_data = requests.get(movi_discover_api).json()
    pages = json_data['total_pages']
    movie_data_base = pd.DataFrame()
    print('number of pages: ', pages)

    for page in range(1, pages + 1):
        movi_discover_page_api = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&' \
                                 f'&language=en-US&sort_by=primary_release_date.asc&' \
                                 f'page={page}&primary_release_date.gte={start_date}&' \
                                 f'primary_release_date.lte={end_date}'
        json_data = requests.get(movi_discover_page_api).json()
        results = json_data['results']
        df = pd.json_normalize(results)
        movie_data_base = pd.concat([movie_data_base, df])

    return movie_data_base


def get_imdb_id(data_base):
    """Sends requests to get IMDB index and add them to database"""
    data_base.insert(loc=2, column='imdb_id', value=['' for i in range(data_base.shape[0])])
    number_of_rows = data_base.shape[0]
    for idx, movie_id in enumerate(data_base['id']):
        if idx % 100 == 0:
            print("progress: ", round(idx / number_of_rows * 100, 2), ' %')
        movie_id_api = f'https://api.themoviedb.org/3/movie/{movie_id}/external_ids?api_key={API_KEY}'
        response = None
        while response is None:
            try:
                response = requests.get(movie_id_api)
            except:
                pass

        if response.status_code == 200:
            json_data = response.json()
            imdb_id = json_data['imdb_id']
            data_base.loc[data_base["id"] == movie_id, "imdb_id"] = imdb_id
        else:
            print(f'Film IMDB index for TMDB {movie_id} not found')

    return data_base


def combine_pieces(minio_bucket, start_date, end_date):
    """Combines all months within defined period into single database"""
    minio_client = get_minio()
    objects = minio_client.list_objects(minio_bucket)
    tmdb = pd.DataFrame()
    file_pattern = '\d{4}-\d{2}--tmdb_month.csv'
    file_month = '(\d{4}-\d{2})--tmdb_month.csv'
    for obj in objects:
        print(obj.object_name)
        current_month = datetime.strptime(re.findall(file_month, obj.object_name)[0], '%Y-%m')
        if re.match(file_pattern, obj.object_name) and (start_date <= current_month <= end_date):
            minio_obj = minio_client.get_object(minio_bucket, obj.object_name)
            month = pd.read_csv(minio_obj, lineterminator='\n')
            tmdb = pd.concat([tmdb, month])
    return tmdb


def download_data_base(**kwargs):
    """Retrieves database from TMDB API and stores it on local Minio service"""
    ti = kwargs['ti']
    """Requests data from themoviedb API"""
    print("START TMDB download data base")
    minio_client = get_minio()

    db_start_date, db_end_date = get_start_end_date()
    db_start_time = datetime.strptime(db_start_date, '%Y-%m-%d')
    db_end_time = datetime.strptime(db_end_date, '%Y-%m-%d')
    month_time = db_start_time

    while month_time < db_end_time:
        end_month_time = month_time + relativedelta(months=1) - timedelta(days=1)

        if end_month_time > db_end_time:
            end_month_time = db_end_time
        month = month_time.strftime('%Y-%m-%d')
        print(f"work on {month}")
        end_month = end_month_time.strftime('%Y-%m-%d')
        file_name = month_time.strftime('%Y-%m') + '--tmdb_month.csv'
        list_of_objects = [minio_object.object_name for minio_object in minio_client.list_objects('tmdb-by-month')]

        if file_name not in list_of_objects:
            month_data_base = get_data_base(start_date=month, end_date=end_month)
            tmdb_imdb_id = get_imdb_id(data_base=month_data_base)
            put_csv_to_minio(
                data_base=tmdb_imdb_id,
                file_name=month_time.strftime('%Y-%m') + '--tmdb_month.csv',
                minio_bucket='tmdb-by-month',
            )
        else:
            print(f"File: {file_name} exists, skipping")
        month_time = end_month_time + timedelta(days=1)
    full_tmdb = combine_pieces(minio_bucket='tmdb-by-month', start_date=db_start_time, end_date=db_end_time)
    put_csv_to_minio(
        data_base=full_tmdb,
        file_name=f'{db_start_date}--{db_end_date}--tmdb.csv',
        minio_bucket='tmdb',
    )
    ti.xcom_push(key="full_tmdb", value=f'{db_start_date}--{db_end_date}--tmdb.csv')


def db_minio_to_postgres(**kwargs):
    """
    Creates tmdb database on Postgres.
    Puts  database from CSV file on Minio to the table on postgres
    """

    ti = kwargs['ti']
    content = ti.xcom_pull(key='full_tmdb', task_ids=['extract_data_base'])[0]
    print(f'put {content} to postgres')

    # Establish connection to DB
    conn = get_psycopg(POSTGRES_DEFAULT_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'tmdb'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('CREATE DATABASE tmdb')
    print("Database has been created successfully !!");
    conn.close()

    # Create table
    minio_client = get_minio()
    conn = get_postgres(db_name="tmdb")
    tmdb_minio = minio_client.get_object('tmdb', content)
    tmdb_data_base = pd.read_csv(tmdb_minio, lineterminator='\n')
    tmdb_data_base.to_sql('tmdb', con=conn, if_exists='replace', index=False)


def join_imdb_rating_task():
    conn = get_psycopg("tmdb")
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE tmdb_imdb_rating '
                   'AS SELECT t.*, "averageRating" FROM tmdb t LEFT JOIN title_ratings r ON(r.tconst = t.imdb_id);')

    cursor.execute('ALTER TABLE tmdb_imdb_rating ALTER COLUMN "averageRating" TYPE FLOAT USING "averageRating"::FLOAT;')
    conn.close()


with DAG(
        'extract_data_base',
        description='extracts TMDB for defined period and stores it on minio server in csv format',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval=None,
        catchup=False,
        default_args=args) as dag:
    extract_data = PythonOperator(task_id="extract_data_base", python_callable=download_data_base)

    db_from_minio_to_pg = PythonOperator(task_id="db_from_minio_to_pg", python_callable=db_minio_to_postgres)
    join_imdb_rating = PythonOperator(task_id="join_imdb_rating", python_callable=join_imdb_rating_task)
    extract_data >> db_from_minio_to_pg >> join_imdb_rating
