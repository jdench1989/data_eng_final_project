from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable, XCom


# Define the function to extract and load data
def extract_data(source_conn_id, country):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    country_offset = int(Variable.get(f'test_extract_offset_{country}', default_var=0))
    extract_data_sql = f"""SELECT id, cnt, tmins, escs, durecec, belong FROM responses OFFSET {country_offset};"""
    Variable.set(f'extracted_data_{country}', source_hook.get_records(extract_data_sql))

def load_data(destination_conn_id, country):
    destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)
    if f'extracted_data_{country}':
        destination_hook.insert_rows(table="igor_test", rows=extracted_data, target_fields=["submission_id", "cnt", "tmins", "escs", "durecec", "belong"])
    live_count_sql = f"SELECT COUNT(*) from igor_test WHERE cnt = '{country.upper()}';"
    live_count = destination_hook.get_records(live_count_sql)
    new_lines = int(live_count[0][0])
    Variable.set(f'test_extract_offset_{country}', new_lines)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2024, 1, 10)
}

dag = DAG(
    'igor_etl_dag',
    default_args=default_args,
    description='Cycle through RDS databases for data extraction',
    schedule_interval= timedelta(minutes=1),  # Define your preferred schedule
    max_active_runs= 1,
    concurrency=1,
    catchup=False  # Decide if you want to backfill or not
)

source_db_country_list = ['alb', 'arg', 'aus', 'aut', 'bel', 'bgr', 'bih', 'blr', 'bra', 'brn', 'can', 'che', 'chl', 'col', 'cri', 'cze', 'deu', 'dnk', 'dom', 'esp']
# ['alb', 'arg', 'aus', 'aut', 'bel', 'bgr', 'bih', 'blr', 'bra', 'brn', 'can', 'che', 'chl', 'col', 'cri', 'cze', 'deu', 'dnk', 'dom', 'esp']
# Use commented list once we are ready to bring more countries onboard

# Loop through 20 RDS databases
for country in source_db_country_list:
    source_conn_id = f'rds_source_db_{country}'  # Replace with your connection IDs
    destination_conn_id = 'analytical_db_connection'  # Replace with your destination connection ID
    task_id = f'extract_and_load_data_{country}'
    
    # Define the PythonOperator for each iteration
    extract_load_task = PythonOperator(
        task_id=task_id,
        python_callable=extract_and_load_data,
        op_kwargs={'source_conn_id': source_conn_id, 'destination_conn_id': destination_conn_id, 'country': country},
        provide_context=True,  # Pass task instance context
        dag=dag,
    )

extract_load_task