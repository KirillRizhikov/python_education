from io import BytesIO
import pandas as pd
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow.utils.dates
from datetime import datetime, timedelta

args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 8),
    'provide_context': True}


def get_minio():
    """Returns connection with Minio"""
    return Minio(
        endpoint="minio:9000",
        access_key="MINIO_ACCESS_KEY",
        secret_key="MINIO_SECRET_KEY", secure=False
    )


def get_raw_data(service):
    """Combines all raw data from Minio that were made during last hour into one pandas dataframe"""
    minio_client = get_minio()
    minio_bucket = service + "-raw-data"
    last_hour_date_time = datetime.now() - timedelta(hours=1)
    # last_hour_date_time = datetime.now()
    processed_file_name = last_hour_date_time.strftime('%Y-%m-%d_%H')
    prefix = service + "_" + processed_file_name
    objects = minio_client.list_objects(bucket_name=minio_bucket, prefix=prefix)

    table = pd.DataFrame()

    for obj in objects:
        print(obj.object_name)
        minio_obj = minio_client.get_object(minio_bucket, obj.object_name)
        table_pd = pd.read_csv(minio_obj, index_col=0)
        table = pd.concat([table, table_pd])

    return table


def put_to_bucket(service, data):
    """Puts given data into proper bucket on Minio"""
    processed_data_bucket = service + "-processed-data"
    minio_client = get_minio()
    if minio_client.bucket_exists(processed_data_bucket):
        print(processed_data_bucket, " exists")
    else:
        minio_client.make_bucket(processed_data_bucket)

    last_hour_date_time = datetime.now() - timedelta(hours=1)
    processed_file_name = last_hour_date_time.strftime('%Y-%m-%d_%H')

    parquet = data.to_parquet(index=False)
    minio_client.put_object(
        processed_data_bucket,
        service + processed_file_name + ".parquet",
        data=BytesIO(parquet),
        length=len(parquet),
        content_type='application/parquet')


def bitfinex_process_data():
    """Processes bitfinex data"""
    service = "bitfinex"
    bitfinex_data = get_raw_data(service)
    bitfinex_data.rename(columns={'timestamp': 'time', 'tid': 'ID'}, inplace=True)
    print(bitfinex_data)
    bitfinex_data.drop('exchange', inplace=True, axis=1)
    bitfinex_data['time'] = pd.to_datetime(bitfinex_data['time'], unit='s')
    put_to_bucket(service, bitfinex_data)


def bitmex_process_data():
    """Processes bitmex data"""
    service = "bitmex"
    bitmex_data = get_raw_data(service)

    bitmex_data.rename(columns={'timestamp': 'time',
                                'side': 'type',
                                'trdMatchID': 'ID',
                                'homeNotional': 'amount'},
                       inplace=True)
    bitmex_data.drop(['symbol', 'size', 'tickDirection', 'grossValue', 'foreignNotional'], inplace=True, axis=1)
    bitmex_data = bitmex_data[['time', 'ID', 'price', 'amount', 'type']]
    bitmex_data['time'] = bitmex_data['time'].str[:-5]
    bitmex_data['time'] = pd.to_datetime(bitmex_data['time'], format="%Y-%m-%dT%H:%M:%S", errors='coerce')

    put_to_bucket(service, bitmex_data)


def poloniex_process_data():
    """Processes poloniex data"""
    service = "poloniex"
    poloniex_data = get_raw_data(service)
    poloniex_data.rename(columns={'date': 'time',
                                  'globalTradeID': 'ID',
                                  'rate': 'price'},
                         inplace=True)
    poloniex_data.drop(['tradeID', 'total', 'orderNumber'], inplace=True, axis=1)
    poloniex_data = poloniex_data[['time', 'ID', 'price', 'amount', 'type']]
    put_to_bucket(service, poloniex_data)


with DAG(
        'hourly_combine',
        description='creates one hour trader log parquet file',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval='* */1 * * *',
        catchup=False,
        default_args=args) as dag:
    bitfinex_process_data = PythonOperator(task_id="bitfinex_hourly_combine", python_callable=bitfinex_process_data)
    bitmex_process_data = PythonOperator(task_id="bitmex_hourly_combine", python_callable=bitmex_process_data)
    poloniex_process_data = PythonOperator(task_id="poloniex_hourly_combine", python_callable=poloniex_process_data)
