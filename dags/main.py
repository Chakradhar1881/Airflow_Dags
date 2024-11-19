from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os

dag_path = os.getcwd()


# defining task 1 to transform data 
def transform_data():
    booking = pd.read_csv("file_path", low_memory=False)
    client = pd.read_csv("file_path", low_memory=False)
    hotel = pd.read_csv("file_path", low_memory=False)
    
    data = pd.merge(booking,client, on='client_id')
    data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)
    
    data = pd.merge(data,hotel, on='hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace=True)
    
    data['booking_date'] = pd.to_datetime(data['booking_date'], infer_datetime_format=True)
    
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace("EUR", "GBP", inplace=True)
    data = data.drop('address', 1)
    
    data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)
    
    
# defining second task to load data into db
def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    client_id INTEGER NOT NULL,
                    booking_date TEXT NOT NULL,
                    room_type TEXT(512) NOT NULL,
                    hotel_id INTEGER NOT NULL,
                    booking_cost NUMERIC,
                    currency TEXT,
                    age INTEGER,
                    client_name TEXT(512),
                    client_type TEXT(512),
                    hotel_name TEXT(512)
                );
             ''')
    records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
    records.to_sql('booking_record', conn, if_exists='replace', index=False)
    
    
# initializing the default arguments 
default_args = {
    'owner' : 'airflow',
    'start_date' : days_ago(5)
}

ingestion_dag = DAG(
    'booking_ingestion',
    default_args = default_args,
    description = 'Aggregates booking records for data analysis',
    schedule_interval = timedelta(days=1),
    catchup = False
)

task1 = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    dag = ingestion_dag
)

task_2 = PythonOperator(
    task_id = 'load_data',
    PythonOperator = load_data,
    dag = ingestion_dag
)

task1 >> task_2