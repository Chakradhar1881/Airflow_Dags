from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pytz

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

object_ids = ['11002CC087CEB000',
                    '11003170EE0EB000',
                    '11003170FD4EB000',
                    '110031710ACEB000',
                    '110031717B8EB000',
                    '11003171B24EB000',
                    '11003171C00EB000',
                    '11003171DD8EB000',
                    '11003171EB8EB000',
                    '110031733E0EB000',
                    '110031734C4EB000',
                    '110031735B4EB000',
                    '11003176A54EB000',
                    '11003177284EB000',
                    '1100317739CEB000',
                    '110031720D0EB000',
                    '11003173790EB000',
                    '11003173A68EB000',
                    '110031758CCEB000',
                    '11003176424EB000',
                    '11003176520EB000',
                    '11003176620EB000',
                    '11003176834EB000',
                    '11003173004EB000',
                    '110031730FCEB000',
                    '11003175A08EB000',
                    '11003175C24EB000',
                    '11003175D24EB000',
                    '11003175E94EB000',
                    '11003175FB4EB000',
                    '11003176324EB000',
                    '110031775B8EB000',
                    '11003171260EB000',
                    '11003171344EB000',
                    '11003171438EB000',
                    '11003171520EB000',
                    '11003171608EB000',
                    '110031716F0EB000',
                    '1100317188CEB000',
                    '11003171960EB000',
                    '11003171A48EB000',
                    '11003171CF0EB000',
                    '11003171FF0EB000',
                    '1100317369CEB000',
                    '110031723ACEB000',
                    '11003172488EB000',
                    '1100317257CEB000',
                    '11003172664EB000',
                    '11003172750EB000',
                    '1100317283CEB000',
                    '11003172928EB000',
                    '11003175B1CEB000',
                    '11003176724EB000',
                    '11003176938EB000',
                    '110031774A8EB000',
                    '110031721D0EB000',
                    '110031722CCEB000',
                    '11003172A20EB000',
                    '11003172B08EB000',
                    '11003172BFCEB000',
                    '11003173894EB000',
                    '11003173988EB000',
                    '1100317621CEB000',
                    '11003176C94EB000',
                    '11003176DCCEB000',
                    '11003177174EB000',
                    '11003172CE4EB000',
                    '11003172DD8EB000',
                    '11003172F0CEB000',
                    '11003173B58EB000',
                    '11003173C54EB000',
                    '11003173D50EB000',
                    '11003174760EB000',
                    '11003175698EB000',
                    '11003176B68EB000',
                    '11003176F14EB000',
                    '1100317705CEB000',
                    '11003174B6CEB000',
                    '11003175278EB000',
                    '11003175374EB000',
                    '110031755A0EB000',
                    '11003173F2CEB000',
                    '11003174C68EB000',
                    '11003174D68EB000',
                    '11003174F64EB000',
                    '11003175060EB000',
                    '11003175178EB000',
                    '11003175480EB000',
                    '110031760C4EB000',
                    '11003173E34EB000',
                    '11003174030EB000',
                    '11003174660EB000',
                    '1100317487CEB000',
                    '11003174978EB000',
                    '11003174A7CEB000',
                    '110031731F0EB000',
                    '110031732F0EB000',
                    '1100317411CEB000',
                    '11003174214EB000',
                    '11003174324EB000',
                    '1100317443CEB000',
                    '11003174544EB000',
                    '11003174E6CEB000',
                    '110031757B4EB000']

object_dict = {
        '11002CC087CEB000': 'MAN-086_002',
        '11003170EE0EB000': 'MAN-087_003',
        '11003170FD4EB000': 'MAN-088_004',
        '110031710ACEB000': 'MAN-089_005',
        '110031717B8EB000': 'MAN-097_018',
        '11003171B24EB000': 'MAN-100_024',
        '11003171C00EB000': 'MAN-102_026',
        '11003171DD8EB000': 'MAN-104_079',
        '11003171EB8EB000': 'MAN-105_082',
        '110031733E0EB000': 'MAN-015_253',
        '110031734C4EB000': 'MAN-083_255',
        '110031735B4EB000': 'MAN-028_258',
        '11003176A54EB000': 'MAN-082_490',
        '11003177284EB000': 'MAN-001_498',
        '1100317739CEB000': 'MAN-008_499',
        '110031720D0EB000': 'MAN-003_125',
        '11003173790EB000': 'MAN-025_280',
        '11003173A68EB000': 'MAN-057_301',
        '110031758CCEB000': 'MAN-063_388',
        '11003176424EB000': 'MAN-084_480',
        '11003176520EB000': 'MAN-085_481',
        '11003176620EB000': 'MAN-079_483',
        '11003176834EB000': 'MAN-081_486',
        '11003173004EB000': 'MAN-021_199',
        '110031730FCEB000': 'MAN-022_201',
        '11003175A08EB000': 'MAN-075_398',
        '11003175C24EB000': 'MAN-064_412',
        '11003175D24EB000': 'MAN-065_415',
        '11003175E94EB000': 'MAN-069_416',
        '11003175FB4EB000': 'MAN-066_417',
        '11003176324EB000': 'MAN-078_444',
        '110031775B8EB000': 'MAN-052_505',
        '11003171180EB000': 'MAN-090_010',
        '11003171260EB000': 'MAN-091_011',
        '11003171344EB000': 'MAN-092_012',
        '11003171438EB000': 'MAN-093_013',
        '11003171520EB000': 'MAN-094_015',
        '11003171608EB000': 'MAN-095_016',
        '110031716F0EB000': 'MAN-096_017',
        '1100317188CEB000': 'MAN-098_019',
        '11003171960EB000': 'MAN-014_020',
        '11003171A48EB000': 'MAN-099_022',
        '11003171CF0EB000': 'MAN-103_055',
        '11003171FF0EB000': 'MAN-007_115',
        '1100317369CEB000': 'MAN-016_264',
        '110031723ACEB000': 'MAN-101_136',
        '11003172488EB000': 'MAN-009_146',
        '1100317257CEB000': 'MAN-010_158',
        '11003172664EB000': 'MAN-011_159',
        '11003172750EB000': 'MAN-012_160',
        '1100317283CEB000': 'MAN-013_161',
        '11003172928EB000': 'MAN-074_162',
        '11003175B1CEB000': 'MAN-073_405',
        '11003176724EB000': 'MAN-080_485',
        '11003176938EB000': 'MAN-029_488',
        '110031774A8EB000': 'MAN-062_504',
        '110031721D0EB000': 'MAN-004_126',
        '110031722CCEB000': 'MAN-005_127',
        '11003172A20EB000': 'MAN-006_184',
        '11003172B08EB000': 'MAN-076_185',
        '11003172BFCEB000': 'MAN-017_189',
        '11003173894EB000': 'MAN-026_297',
        '11003173988EB000': 'MAN-027_299',
        '1100317621CEB000': 'MAN-077_443',
        '11003176C94EB000': 'MAN-034_493',
        '11003176DCCEB000': 'MAN-030_494',
        '11003177174EB000': 'MAN-050_497',
        '11003172CE4EB000': 'MAN-018_194',
        '11003172DD8EB000': 'MAN-019_196',
        '11003172F0CEB000': 'MAN-020_197',
        '11003173B58EB000': 'MAN-031_304',
        '11003173C54EB000': 'MAN-032_305',
        '11003173D50EB000': 'MAN-035_311',
        '11003174760EB000': 'MAN-044_328',
        '11003175698EB000': 'MAN-060_379',
        '11003176B68EB000': 'MAN-002_492',
        '11003176F14EB000': 'MAN-047_495',
        '1100317705CEB000': 'MAN-033_496',
        '11003174B6CEB000': 'MAN-048_349',
        '11003175278EB000': 'MAN-056_366',
        '11003175374EB000': 'MAN-071_370',
        '110031755A0EB000': 'MAN-059_378',
        '11003173F2CEB000': 'MAN-067_314',
        '11003174C68EB000': 'MAN-049_350',
        '11003174D68EB000': 'MAN-068_351',
        '11003174F64EB000': 'MAN-053_356',
        '11003175060EB000': 'MAN-054_361',
        '11003175178EB000': 'MAN-055_362',
        '11003175480EB000': 'MAN-058_377',
        '110031760C4EB000': 'MAN-070_437',
        '11003173E34EB000': 'MAN-036_312',
        '11003174030EB000': 'MAN-037_319',
        '11003174660EB000': 'MAN-043_326',
        '1100317487CEB000': 'MAN-045_340',
        '11003174978EB000': 'MAN-072_341',
        '11003174A7CEB000': 'MAN-046_345',
        '110031731F0EB000': 'MAN-023_215',
        '110031732F0EB000': 'MAN-024_222',
        '1100317411CEB000': 'MAN-038_320',
        '11003174214EB000': 'MAN-039_321',
        '11003174324EB000': 'MAN-040_322',
        '1100317443CEB000': 'MAN-041_323',
        '11003174544EB000': 'MAN-042_324',
        '11003174E6CEB000': 'MAN-051_353',
        '110031757B4EB000': 'MAN-061_380'
    }

# function for fetching start time and end time 
def get_rounded_time_interval():
    
    now = datetime.utcnow()
    
    if now.minute != 0:  
        start_time = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)
    else:
        start_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)

    return start_time.isoformat() + 'Z', end_time.isoformat() + 'Z'


def fetch_active_power_kpi(**kwargs):
    """Fetch active power KPI data from the API."""
    http_hook = HttpHook(http_conn_id='bazefield_api', method='GET')
    endpoint = '/Bazefield.Services/api/objects/timeseries/aggregated'
    object_ids_str = ','.join(object_ids)
    starttime , endtime = get_rounded_time_interval()
    print(f'fetching api data between {starttime} and {endtime}')
    params = {
        'objectIds': object_ids_str,
        'points': 'ActivePower',
        'aggregates': 'TIMEAVERAGE',
        'from': starttime,
        'to': endtime,
        'interval': '10m',
        'format': 'json'
    }
    query_string = '&'.join(f"{key}={value}" for key, value in params.items())
    full_endpoint = f"{endpoint}?{query_string}"
    try:
        response = http_hook.run(endpoint=full_endpoint)
        response.raise_for_status()
        data = response.json()

    except Exception as e:
        raise ValueError(f"Error fetching data from API: {e}")
    
    print(data)
    extracted_data = []
    
    for object_id, object_data in data['objects'].items():
        for point_name, points in object_data['points'].items():
            for point in points:
                for time_series in point['timeSeries']:
                    v_value = time_series.get('v', None)
                    # Append the extracted data as a dictionary
                    extracted_data.append({
                        'Wtg': object_dict[point['objectId']],
                        't_local': time_series['t_local'],
                        'pointName': point_name,
                        'v': v_value
                    })
    
    df = pd.DataFrame(extracted_data)
    pivot_df = df.pivot_table(index=['Wtg', 't_local'], 
                            columns='pointName', 
                            values='v').reset_index()
    pivot_df.rename(columns={'Wtg':'SystemName','t_local':'Timestamp'},inplace=True)
    pivot_df['Timestamp'] = pd.to_datetime(pivot_df['Timestamp'])
    print('fetched data : ')
    print(pivot_df)
    
    # push pivoted df to next task 
    kwargs['ti'].xcom_push(key='pivot_df', value=pivot_df.to_json(orient='records'))

def calculate_kpi(**kwargs):
    """Calculate the KPI using the formula."""
    # Pull API data from XCom
    pivot_json = kwargs['ti'].xcom_pull(key='pivot_df', task_ids='fetch_data')
    
    if not pivot_json:
        raise ValueError("No data received from API")

    # Assuming the API data has a list of turbines with a "theoretical_power" field
    pivot_df = pd.read_json(pivot_json, orient='records')
    park_active_power = pivot_df['ActivePower'].sum()
    park_generation = park_active_power/6
    print(f'maniyachi site generation for this hour : {park_generation}')
    
    # Push the calculated KPI to XCom
    kwargs['ti'].xcom_push(key='park_generation', value=park_generation)

def store_kpi_in_postgres(**kwargs):
    """Store the calculated KPI in a PostgreSQL table."""
    # Pull the calculated KPI from XCom
    park_generation = kwargs['ti'].xcom_pull(key='park_generation', task_ids='calculate_kpi')
    if park_generation is None:
        raise ValueError("No KPI value to store")

    # Insert into PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = postgres_hook.get_conn()
    if connection:
        print("successfully connected")
    cursor = connection.cursor()
    starttime , endtime = get_rounded_time_interval()

    # Define the insert query (replace with your table and column names)
    insert_query = """
        INSERT INTO kpi_table (time_stamp, kpi_name, value) VALUES (%s, %s, %s)
    """
    try:
        # Example: Use current timestamp for this example
        cursor.execute(insert_query, (starttime,'maniyachi_site_generation', park_generation))
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise ValueError(f"Error inserting KPI into PostgreSQL: {e}")
    finally:
        cursor.close()
        connection.close()

# Define the DAG
with DAG(
    'kpi_calculation_pipeline',
    default_args=default_args,
    description='DAG to fetch API data, calculate KPI, and store results in PostgreSQL',
    schedule_interval='5 * * * *',  
    start_date=datetime(2024, 11, 15),
    catchup=True,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_active_power_kpi,
        provide_context=True,
    )

    calculate_kpi = PythonOperator(
        task_id='calculate_kpi',
        python_callable=calculate_kpi,
        provide_context=True,
    )

    store_kpi = PythonOperator(
        task_id='store_kpi',
        python_callable=store_kpi_in_postgres,
        provide_context=True,
    )

    # Task dependencies
    fetch_data >> calculate_kpi >> store_kpi