import datetime as dt
from datetime import timedelta
from elasticsearch import Elasticsearch, helpers

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from sqlalchemy import create_engine
import pandas as pd

def extract_data():
    # Corrected database URL with the server and database separately
    db_url = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)
    df = pd.read_sql_query('select * from table_m3',con=engine)

    # Save the data to a CSV file
    df.to_csv('/opt/airflow/P2M3_furqon_data_raw.csv', index=False)

def clean_mobile():
    # read data
    df = pd.read_csv('/opt/airflow/P2M3_furqon_data_raw.csv')

    # rename data
    df = df.rename(columns={
    'Name': 'name',
    'Brand': 'brand',
    'Model': 'model',
    'Battery capacity (mAh)': 'battery_capacity_mah',
    'Screen size (inches)': 'screen_size_inches',
    'Touchscreen': 'touchscreen',
    'Resolution x': 'resolution_x',
    'Resolution y': 'resolution_y',
    'Processor': 'processor',
    'RAM (MB)': 'ram_mb',
    'Internal storage (GB)': 'internal_storage_gb',
    'Rear camera': 'rear_camera',
    'Front camera': 'front_camera',
    'Operating system': 'operating_system',
    'Wi-Fi': 'wi_Fi',
    'Bluetooth': 'bluetooth',
    'GPS': 'gps',
    'Number of SIMs': 'number_of_sims',
    '3G': '3G',
    '4G/ LTE': '4g_lte',
    'Price': 'price',
    })

    # Melihat tipe data
    df.dtypes

    # Missing Value
    df = df.dropna()

    #  Menghapus baris yang memiliki nilai yang sama
    df.drop_duplicates(inplace= True)


    df.to_csv('/opt/airflow/P2M3_furqon_data_clean.csv')

def post_es():
    es = Elasticsearch('http://elasticsearch:9200')
    print("Elasticsearch connection:", es.ping())

    # Read data from CSV file
    df = pd.read_csv('/opt/airflow/P2M3_furqon_data_clean.csv')
    
    
    for _, row in df.iterrows():
        doc = row.to_json()
        res = es.index(index='index', body=doc)

    # # Prepare bulk data for indexing
    # actions = [
    #     {
    #         "_op_type": "index",
    #         "_index": "milestone",
    #         "_source": row.to_dict()
    #     }
    #     for _, row in df.iterrows()
    # ]

    # # Perform bulk indexing
    # response = helpers.bulk(es, actions)
    # print("Bulk indexing response:", response)

default_args = {
'owner': 'furqon',
'start_date': dt.datetime(2023, 11, 23),
'retries': 1,
'retry_delay': dt.timedelta(minutes=2),
}

with DAG('H8CleanData',
        default_args=default_args,
        schedule_interval=timedelta(minutes=5),
        concurrency=1,  # Sesuaikan sesuai kebutuhan Anda
        max_active_runs=1,  # Hanya satu instance yang diizinkan pada suatu waktu
        ) as dag:

    getData = PythonOperator(task_id='get', python_callable=extract_data)

    cleanData = PythonOperator(task_id='clean', python_callable=clean_mobile)

    elasSearch = PythonOperator(task_id='push', python_callable=post_es)

getData >> cleanData >> elasSearch