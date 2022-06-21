import re
import pandas as pd
import requests
import airflow.utils.dates

from io import BytesIO
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from minio.commonconfig import REPLACE, CopySource
from os import environ
from airflow.utils.email import send_email

args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 8),
    'provide_context': True}

API_KEY = environ.get("API_KEY")
MINIO_ACCESS_KEY = environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = environ.get("MINIO_SECRET_KEY")
MINIO_HOST = environ.get("MINIO_HOST")

POSTGRES_USER = environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = environ.get("POSTGRES_HOST")
POSTGRES_DEFAULT_DB = environ.get("POSTGRES_DEFAULT_DB")
POSTGRES_PORT = environ.get("POSTGRES_PORT")


def get_minio():
    """Returns connection with Minio"""
    return Minio(
        endpoint=MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


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


def update_tmdb(**kwargs):
    """Checks last day of the existing TMDB database and updates it"""
    ti = kwargs['ti']
    minio_client = get_minio()
    list_of_objects = minio_client.list_objects('tmdb')
    db_file_pattern = '\d{4}-\d{2}-\d{2}--\d{4}-\d{2}-\d{2}--tmdb.csv'
    for minio_object in list_of_objects:
        object_name = minio_object.object_name
        print(re.match(db_file_pattern, object_name))
        if re.match(db_file_pattern, object_name):
            start_date, end_date = re.findall('\d{4}-\d{2}-\d{2}', object_name)
            file_db = minio_object
            print("file was found", minio_object.object_name)
            break
        else:
            file_db = False
        if not file_db:
            print("nothing to update")
            return False

    tmdb_minio = minio_client.get_object('tmdb', file_db.object_name)
    tmdb = pd.read_csv(tmdb_minio, lineterminator='\n')

    date_now = datetime.now().strftime('%Y-%m-%d')
    tmdb_update = download_data_base(end_date, date_now)
    last_day_films = tmdb[tmdb['release_date'] == end_date]['id']

    for idx in last_day_films:
        tmdb_update.drop(tmdb_update[tmdb_update['id'] == int(idx)].index, inplace=True)

    tmdb = pd.concat([tmdb, tmdb_update])
    minio_client.copy_object(
        "tmdb-obsolete",
        "obsolete" + file_db.object_name,
        CopySource("tmdb", file_db.object_name),
    )
    minio_client.remove_object("tmdb", file_db.object_name)

    put_csv_to_minio(
        data_base=tmdb,
        file_name=f'{start_date}--{date_now}--tmdb.csv',
        minio_bucket='tmdb')

    list_of_new_films = 'Hey man, here are your new films:'
    for idx in tmdb_update['original_title']:
        list_of_new_films += '<p>' + idx + '</p>'
    ti.xcom_push(key="new_films", value=list_of_new_films)

    conn = get_postgres(db_name='tmdb')
    tmdb_update.to_sql('tmdb', con=conn, if_exists='append', index=False)


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


def download_data_base(db_start_date, db_end_date):
    """Download datas from TMDB API to a database"""
    print("START")
    minio_client = get_minio()
    db_start_time = datetime.strptime(db_start_date, '%Y-%m-%d')
    db_end_time = datetime.strptime(db_end_date, '%Y-%m-%d')
    month_time = db_start_time
    while month_time < db_end_time:
        end_month_time = month_time + relativedelta(months=1) - timedelta(days=1)
        if end_month_time > db_end_time:
            end_month_time = db_end_time
        month = month_time.strftime('%Y-%m-%d')
        print(month)
        end_month = end_month_time.strftime('%Y-%m-%d')
        file_name = month_time.strftime('%Y-%m') + '--update_tmdb.csv'
        list_of_objects = [minio_object.object_name for minio_object in minio_client.list_objects('tmdb')]
        if file_name not in list_of_objects:
            month_data_base = get_data_base(month, end_month)
            themoviedb_imdb_id = get_imdb_id(data_base=month_data_base)
            put_csv_to_minio(
                data_base=themoviedb_imdb_id,
                file_name=month_time.strftime('%Y-%m') + '--update_tmdb.csv',
                minio_bucket='tmdb',
            )
        else:
            print(f"File: {file_name} exists, skipping")
        month_time = end_month_time + timedelta(days=1)
    full_tmdb = combine_pieces(minio_bucket='tmdb', start_date=db_start_time, end_date=db_end_time)
    return full_tmdb


def combine_pieces(minio_bucket, start_date, end_date):
    """Combines all months within defined period into single database"""
    print(start_date, end_date)
    minio_client = get_minio()
    objects = minio_client.list_objects(minio_bucket)
    tmdb = pd.DataFrame()
    file_pattern = '\d{4}-\d{2}--update_tmdb.csv'
    for obj in objects:
        if re.match(file_pattern, obj.object_name):
            minio_obj = minio_client.get_object(minio_bucket, obj.object_name)
            month = pd.read_csv(minio_obj, lineterminator='\n')
            tmdb = pd.concat([tmdb, month])
            minio_client.remove_object(minio_bucket, obj.object_name)
    return tmdb


def email_callback(**kwargs):
    """Sends email with new films"""
    ti = kwargs['ti']
    content = ti.xcom_pull(key='new_films', task_ids=['extract_data_base'])[0]
    print(content)
    send_email(
        to=[
            'rizhikovkirill@gmail.com'
        ],
        subject='updates TMDB to a defined data',
        html_content=content,
    )


with DAG(
        'update_tmdb_email',
        description='updates TMDB to a defined data ',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval="0 0 * * *",
        catchup=False,
        default_args=args) as dag:
    send_email_notification = PythonOperator(
        task_id='send_email_notification_1',
        python_callable=email_callback,
        provide_context=True
    )

    extract_data = PythonOperator(task_id="extract_data_base", python_callable=update_tmdb)

    extract_data >> send_email_notification
