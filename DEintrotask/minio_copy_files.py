"""Module copies files to Minio service"""
from minio import Minio
from minio.error import (InvalidResponseError)
import os

from os import listdir
from os.path import isfile, join


def get_minio_client(access, secret):
    return Minio(
        'localhost:9000',
        access_key=access,
        secret_key=secret,
        secure=False
    )


if __name__ == '__main__':
    minioClient = get_minio_client('kirillkey', 'kirillsecretkey')

    if not minioClient.bucket_exists('newbucket'):
        try:
            minioClient.make_bucket('newbucket')
        except InvalidResponseError as identifier:
            raise
    dir_path = './covid_ita/'
    files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]

    try:
        for file_name in files:
            with open(dir_path + file_name, 'rb') as file:
                statdata = os.stat(dir_path + file_name)
                minioClient.put_object(
                    'newbucket',
                    file_name,
                    file,
                    statdata.st_size
                )
    except InvalidResponseError as identifier:
        raise
