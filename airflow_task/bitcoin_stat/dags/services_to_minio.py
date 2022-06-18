from io import BytesIO
import pandas as pd
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import airflow.utils.dates
from datetime import datetime, timedelta
import time

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


def put_to_minio(service, json_data):
    """Puts json_data into proper bucket on Minio"""
    df = pd.json_normalize(json_data)
    minio_client = get_minio()
    minio_bucket = service + "-raw-data"

    if minio_client.bucket_exists(minio_bucket):
        print(minio_bucket, " exists")
    else:
        minio_client.make_bucket(minio_bucket)

    file_name = service + datetime.now().strftime('_%Y-%m-%d_%H-%M.csv')

    csv = df.to_csv(index=False).encode('utf-8')
    minio_client.put_object(
        minio_bucket,
        file_name,
        data=BytesIO(csv),
        length=len(csv),
        content_type='application/csv')


def bitfinex_extract_data(**kwargs):
    """Requests data from bitfinex API"""
    ti = kwargs['ti']
    # request
    service = "bitfinex"
    end_time = int(time.mktime(datetime.now().timetuple()))
    start_time = int(time.mktime((datetime.now() - timedelta(minutes=1)).timetuple()))
    service_host = f"https://api.bitfinex.com/v1/trades/btcusd?start={start_time}&end={end_time}"
    response = requests.get(service_host)

    if response.status_code == 200:
        # process response
        json_data = response.json()
        ti.xcom_push(key=(service + '_json'), value=json_data)


def bitfinex_load_data(**kwargs):
    """Loads bitfinex raw data to a Minio"""
    ti = kwargs['ti']
    service = "bitfinex"
    json_data = ti.xcom_pull(key=(service + '_json'), task_ids=[service + "_extract_data"])[0]
    put_to_minio(service, json_data)


def bitmex_extract_data(**kwargs):
    """Requests data from bitmex API"""
    ti = kwargs['ti']
    # request
    service = "bitmex"
    start_time = (datetime.utcnow() - timedelta(minutes=1)).strftime('%Y-%m-%d%%20%H%%3A%M')
    end_time = datetime.utcnow().strftime('%Y-%m-%d%%20%H%%3A%M')
    service_host = f"https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&startTime={start_time}&endTime={end_time}"
    response = requests.get(service_host)

    if response.status_code == 200:
        # process response
        json_data = response.json()
        ti.xcom_push(key=(service + '_json'), value=json_data)


def bitmex_load_data(**kwargs):
    """Loads bitmex raw data to a Minio"""
    ti = kwargs['ti']
    service = "bitmex"
    json_data = ti.xcom_pull(key=(service + '_json'), task_ids=[service + "_extract_data"])[0]
    put_to_minio(service, json_data)


def poloniex_extract_data(**kwargs):
    """Requests data from poloniex API"""
    ti = kwargs['ti']
    # request
    service = "poloniex"
    end_time = int(time.mktime(datetime.now().timetuple()))
    start_time = int(time.mktime((datetime.now() - timedelta(minutes=1)).timetuple()))
    service_host = f"https://poloniex.com/public?command=" \
                   f"returnTradeHistory&currencyPair=" \
                   f"USDT_BTC&start={start_time}&end={end_time}"
    response = requests.get(service_host)

    if response.status_code == 200:
        # process response
        json_data = response.json()
        ti.xcom_push(key=(service + '_json'), value=json_data)


def poloniex_load_data(**kwargs):
    """Loads poloniex raw data to a Minio"""
    ti = kwargs['ti']
    service = "poloniex"
    json_data = ti.xcom_pull(key=(service + '_json'), task_ids=[service + "_extract_data"])[0]
    put_to_minio(service, json_data)


with DAG(
        'services_to_minio',
        description='requests bitcoin price and saves it to Minio server',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval='*/1 * * * *',
        catchup=False,
        default_args=args) as dag:
    bitfinex_extract_data = PythonOperator(task_id="bitfinex_extract_data", python_callable=bitfinex_extract_data)
    bitfinex_load_data = PythonOperator(task_id="bitfinex_load_data", python_callable=bitfinex_load_data)
    bitmex_extract_data = PythonOperator(task_id="bitmex_extract_data", python_callable=bitmex_extract_data)
    bitmex_load_data = PythonOperator(task_id="bitmex_load_data", python_callable=bitmex_load_data)
    poloniex_extract_data = PythonOperator(task_id="poloniex_extract_data", python_callable=poloniex_extract_data)
    poloniex_load_data = PythonOperator(task_id="poloniex_load_data", python_callable=poloniex_load_data)

    bitfinex_extract_data >> bitfinex_load_data
    bitmex_extract_data >> bitmex_load_data
    poloniex_extract_data >> poloniex_load_data
