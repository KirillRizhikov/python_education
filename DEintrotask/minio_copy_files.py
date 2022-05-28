"""Module copies files to Minio service"""
from minio import Minio
from minio.error import (InvalidResponseError)
import os


def get_minio_client(access, secret):
    return Minio(
        'localhost:9000',
        access_key=access,
        secret_key=secret,
        secure=False
    )


if __name__ == '__main__':
    minioClient = get_minio_client('kirillkey', 'kirillsecretkey')

    if (not minioClient.bucket_exists('newbucket')):
        try:
            minioClient.make_bucket('newbucket')
        except InvalidResponseError as identifier:
            raise

    try:
        with open('./covid_csv/dpc-covid19-ita-regioni-20200224.csv', 'rb') as file:
            statdata = os.stat('./covid_csv/dpc-covid19-ita-regioni-20200224.csv')
            minioClient.put_object(
                'newbucket',
                'dpc-covid19-ita-regioni-20200224.csv',
                file,
                statdata.st_size
            )
    except InvalidResponseError as identifier:
        raise
