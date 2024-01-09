from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the function to extract and load data
def extract_and_load_data(source_conn_id, destination_conn_id):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)
    extract_data_sql = """SELECT id, cnt, tmins, escs, pared, hisei, durecec, belong FROM responses;"""
    extracted_data = source_hook.get_records(extract_data_sql)
    if extracted_data:
        for row in extracted_data:
            row = list(row)
            insert_query = "INSERT INTO test (submission_id, cnt, tmins, escs, pared, hisei, durecec, belong) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (submission_id) DO NOTHING"
            destination_hook.run(insert_query, parameters=row)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'etl_through_rds_databases',
    default_args=default_args,
    description='Cycle through RDS databases for data extraction',
    schedule_interval='@hourly',  # Define your preferred schedule
    catchup=False  # Decide if you want to backfill or not
)

source_db_country_list = ['alb', 'arg', 'aus', 'aut', 'bel', 'bgr', 'bih', 'blr', 'bra', 'brn', 'can', 'che', 'chl', 'col', 'cri', 'cze', 'deu', 'dnk', 'dom', 'esp']
# Loop through 20 RDS databases
for country in source_db_country_list:
    source_conn_id = f'rds_source_db_{country}'  # Replace with your connection IDs
    destination_conn_id = 'analytical_db_connection'  # Replace with your destination connection ID

    task_id = f'extract_and_load_data_{country}'
    
    # Define the PythonOperator for each iteration
    extract_load_task = PythonOperator(
        task_id=task_id,
        python_callable=extract_and_load_data,
        op_kwargs={'source_conn_id': source_conn_id, 'destination_conn_id': destination_conn_id},
        provide_context=True,  # Pass task instance context
        dag=dag,
    )
