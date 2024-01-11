from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable, XCom

source_db_country_list = ['alb', 'arg', 'aus', 'aut', 'bel', 'bgr', 'bih', 'blr', 'bra', 'brn', 'can', 'che', 'chl', 'col', 'cri', 'cze', 'deu', 'dnk', 'dom', 'esp']

def extract_and_load_all_data(**kwargs):
    destination_conn_id = 'analytical_db_connection'  # Replace with your destination connection ID
    destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)

    for country in source_db_country_list:
        source_conn_id = f'rds_source_db_{country}'  # Replace with your connection IDs
        country_offset = int(Variable.get(f'test_extract_offset_{country}', default_var=0))
        extract_data_sql = f"""SELECT id, cnt, tmins, escs, durecec, belong FROM responses OFFSET {country_offset} LIMIT 1000;"""

        source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        extracted_data = source_hook.get_records(extract_data_sql)

        if extracted_data:
            destination_hook.insert_rows(table="live", rows=extracted_data, replace = True, replace_index="id", target_fields=["submission_id", "cnt", "tmins", "escs", "durecec", "belong"])

        live_count_sql = f"SELECT COUNT(*) from live WHERE cnt = '{country.upper()}';"
        live_count = int(destination_hook.get_records(live_count_sql)[0][0])
        if live_count != country_offset:
            new_offset = country_offset + live_count
            Variable.set(f'test_extract_offset_{country}', new_offset)


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
    'etl_dag',
    default_args=default_args,
    description='Cycle through RDS databases for data extraction',
    schedule_interval=timedelta(minutes=1),  # Define your preferred schedule
    max_active_runs=1,
    # concurrency=1,
    catchup=False  # Decide if you want to backfill or not
)

# Define the PythonOperator for the entire process
extract_load_all_task = PythonOperator(
    task_id='extract_and_load_all_data',
    python_callable=extract_and_load_all_data,
    provide_context=True,  # Pass task instance context
    dag=dag,
)

# Set the task dependencies
extract_load_all_task