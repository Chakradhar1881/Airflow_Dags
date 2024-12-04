from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import logging
from pytz import timezone

IST = timezone('Asia/Kolkata')

# kpi_connection_id = 'kpi_table_connection'
kpi_connection_id = Variable.get("kpi_db_id")
masterdata_conn = Variable.get("master_db_id")

# Default arguments for the DAG
default_args = {
    'owner': 'Rule Manager',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'timezone' : IST
}

# Function to establish DB connection
def get_db_connection(conn_id):
    try:
        logging.info('Connecting to database...')
        postgres = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres.get_conn()
        logging.info("Connected successfully...")
        return conn
    except Exception as err:
        logging.error(f"Exception while creating connection to KPI Configurator database: {str(err)}")
        raise err

# Function to fetch data from the DB
def get_db_data(connection_id, query):
    try:
        conn = get_db_connection(connection_id)
        df = pd.read_sql_query(query, conn)
        conn.close()
        # Extract unique frequencies and pass them to XCom for further tasks
        # scheduling_frequencies = df['scheduling_freq'].unique().tolist()
        # logging.info(f"Scheduling frequencies: {scheduling_frequencies}")
        return df
    except Exception as e:
        logging.error(f"Exception while getting KPI data from DB: {str(e)}")
        raise e

# Function to dynamically create DAGs
def create_dag(dag_id, default_args, schedule_interval, filter):
    filtered_kpi = kpi_data[kpi_data['scheduling_freq']==filter]
    independent_kpis = filtered_kpi[filtered_kpi['kpi_ids'].str.len()==0]
    dependent_kpis = filtered_kpi[filtered_kpi['kpi_ids'].str.len()>0]
    tag_ids_list = tuple({num for row in filtered_kpi['tag_model_ids'] for num in row})
    
    def get_time_intervals(current_time, interval):
        if interval == "1_Minutes":
            # Round down to the nearest 1 minute
            rounded_time = current_time.replace(second=0, microsecond=0)
            end_time = rounded_time
            start_time = end_time - timedelta(minutes=1)

        elif interval == "5_Minutes":
            # Round down to the nearest 5 minutes
            rounded_time = current_time - timedelta(minutes=current_time.minute % 5, 
                                                    seconds=current_time.second, 
                                                    microseconds=current_time.microsecond)
            end_time = rounded_time
            start_time = end_time - timedelta(minutes=5)

        elif interval == "30_Minutes":
            # Round down to the nearest 30 minutes
            rounded_time = current_time - timedelta(minutes=current_time.minute % 30, 
                                                    seconds=current_time.second, 
                                                    microseconds=current_time.microsecond)
            end_time = rounded_time
            start_time = end_time - timedelta(minutes=30)

        elif interval == "Hourly":
            # Round down to the nearest hour
            rounded_time = current_time.replace(minute=0, second=0, microsecond=0)
            end_time = rounded_time
            start_time = end_time - timedelta(hours=1)

        elif interval == "Daily":
            # Round down to the start of the day
            rounded_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = rounded_time
            start_time = end_time - timedelta(days=1)

        else:
            raise ValueError("Unsupported interval. Use '1min', '5min', '30min', '1hour', or '1day'.")
        
        return start_time, end_time
    
    def masterdata_to_excel():
        
        csv_path = r'C:\Users\3101177\OneDrive - JSW\Desktop\projects\Personal\Airflow\raw_data'
        current_time = datetime.now(IST)
        start_time, end_time = get_time_intervals(current_time, filter)
        logging.info(f"Fetching data from master db and storing excel between {start_time} and {end_time}")
        
        if len(tag_ids_list) is 1:
            master_query = f'''SELECT * FROM raw_data WHERE tag_obj_id = {tag_ids_list[0]} AND ts BETWEEN '{start_time}' and '{end_time}' '''
        else:
            master_query = f'''SELECT * FROM raw_data WHERE tag_obj_id in {tag_ids_list} AND ts BETWEEN '{start_time}' and '{end_time}' '''
        
        logging.info(f"Establishing connection to Master DB and query is {master_query}")
        temp_data = get_db_data(masterdata_conn, master_query)
        logging.info(f"data fetched successfully from master db and size is {temp_data.shape}")
        logging.info(temp_data)
        temp_data.to_csv(f"C:\\Users\\3101177\\OneDrive - JSW\\Desktop\\projects\\Personal\\Airflow\\raw_data\\temp_{filter}_{start_time}_{end_time}.csv", index=False)
        logging.info("data stored successfully")
        
    def display_kpi_list():
        kpi_list = filtered_kpi['kpi_name'].unique()
        logging.info(f"list of kpi's where scheduled frequency is {filter} are : {kpi_list}")
        logging.info(f"tags used for this dag are : {tag_ids_list}")
    
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        max_active_runs=1
    ) as dag:
        
        globals()["list_of_kpi"] = PythonOperator(
            task_id='list_of_kpi',
            dag = dag,
            python_callable=display_kpi_list,
            provide_context=True
        )
        
        globals()["fetch_master_data"] = PythonOperator(
            task_id="fetch_master_Data",
            dag=dag,
            provide_context=True,
            python_callable=masterdata_to_excel
        )
        
        globals()["list_of_kpi"] >> globals()["fetch_master_data"]
        
    return dag



def create_schedule_interval(freq):
    if '_' in freq:
        time = int(freq.split('_')[0])
        frequency = freq.split('_')[1]
    else:
        time = None
        frequency = freq
    
    if (frequency == "Minutes"):
        if time == 1 :
            return '* * * * *'
        elif time == 5:
            return '*/5 * * * *'
        elif time == 30:
            return '*/30 * * * *'
        
    elif (frequency == "Hourly"):
        return "0 * * * *"
    
    elif (frequency == "Daily"):
        return "0 0 * * *"
    

query = '''SELECT * FROM kpi_main'''
start_time = datetime.now()
logging.info("connecting to kpi_main table")
kpi_data = get_db_data(kpi_connection_id, query)
end_time = datetime.now()
logging.info(f"connection established and data fetched successfully in {end_time - start_time} seconds")

for freq in kpi_data['scheduling_freq'].unique():
    dag_id = f"KPIs_for_{freq}"
    schedule = create_schedule_interval(freq)
    
    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        filter=freq
    )
