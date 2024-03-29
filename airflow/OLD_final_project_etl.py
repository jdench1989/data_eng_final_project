from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def delete_data_from_table(source_conn_id):
    pg_hook = PostgresHook(postgres_conn_id=source_conn_id)
    delete_sql_command = "DELETE FROM live"
    pg_hook.run(delete_sql_command, autocommit=True)

# Define the function to extract and load data
def extract_and_load_data(source_conn_id, destination_conn_id):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)
    extract_data_sql = """SELECT id, cnt, tmins, escs, pared, hisei, durecec, belong FROM responses;"""
    extracted_data = source_hook.get_records(extract_data_sql)
    if extracted_data:
        destination_hook.insert_rows(table="live", rows=extracted_data, target_fields=["submission_id", "cnt", "tmins", "escs", "pared", "hisei", "durecec", "belong"])
        # for row in extracted_data:
        #     row = list(row)
        #     insert_query = "INSERT INTO live (submission_id, cnt, tmins, escs, pared, hisei, durecec, belong) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (submission_id) DO NOTHING"
        #     destination_hook.run(insert_query, parameters=row)

def record_total_submissions(source_conn_id, destination_conn_id):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    destination_hook = PostgresHook(postgres_conn_id=destination_conn_id)
    count_sql = "SELECT COUNT(*) from live"
    count = int(source_hook.get_records(count_sql)[0][0])
    last_run_count = int(Variable.get('total_submissions_last_run', default_var=0))
    time_hour = (datetime.now()).hour
    subs_per_hour = count - last_run_count
    params = [time_hour, count, subs_per_hour]
    insert_sql = "INSERT INTO time (hour, submissions, subs_per_hour) VALUES (%s, %s, %s)"
    destination_hook.run(insert_sql, parameters = params)
    Variable.set('total_submissions_last_run', count)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 10),
}

dag = DAG(
    'etl_through_rds_databases',
    default_args=default_args,
    description='Cycle through RDS databases for data extraction',
    schedule_interval='@hourly',  # Define your preferred schedule
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
        op_kwargs={'source_conn_id': source_conn_id, 'destination_conn_id': destination_conn_id},
        provide_context=True,  # Pass task instance context
        dag=dag,
    )

record_total_submissions_task = PythonOperator(
        task_id='record_total_submissions_task',
        python_callable=record_total_submissions,
        op_kwargs={'source_conn_id': 'analytical_db_connection', 'destination_conn_id': 'analytical_db_connection'},
        provide_context=True,  # Pass task instance context
        dag=dag,
    )

delete_data_task = PythonOperator(
    task_id='delete_data_from_table',
    python_callable=delete_data_from_table,
    op_kwargs={'source_conn_id': 'analytical_db_connection'},
    provide_context=True,  # Pass task instance context
    dag=dag,
)

delete_data_task >> extract_load_task >> record_total_submissions_task