#!/usr/bin/env python
# coding: utf-8

# In[33]:

import os
import argparse
import pandas as pd
import requests
import gzip

from time import time
from sqlalchemy import create_engine
from io import BytesIO


def download_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

def main(params):
    user = params.user
    password = params.password
    db = params.db
    host = params.host
    port = params.port
    table_name = params.table_name

    # Download file
    url = params.url
    
    file_content = download_file(url)

    # Decompress the file
    with gzip.GzipFile(fileobj=file_content, mode="rb") as f:
        csv_name = f.read()


    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    df_iter = pd.read_csv(BytesIO(csv_name), iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')


    df.to_sql(name=table_name, con=engine, if_exists='append')

    try:
        while True:

            t_start = time()

            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f seconds' %(t_end - t_start))

    except StopIteration:
        pass # The iteration has reached the end of the file

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Requirements to build the Postgres database...

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the CSV file')


    args = parser.parse_args()
    main(args)


    

