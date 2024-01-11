from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

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

# dag = DAG(
#     'igor_time_dag',
#     default_args=default_args,
#     description='Calculate new submissions per hour and store in DB time table',
#     schedule_interval='@hourly',  # Define your preferred schedule
#     catchup=False  # Decide if you want to backfill or not
# )

record_total_submissions_task = PythonOperator(
        task_id='record_total_submissions_task_test',
        python_callable=record_total_submissions,
        op_kwargs={'source_conn_id': 'analytical_db_connection', 'destination_conn_id': 'analytical_db_connection'},
        provide_context=True,  # Pass task instance context
        dag=dag,
    )

record_total_submissions_task