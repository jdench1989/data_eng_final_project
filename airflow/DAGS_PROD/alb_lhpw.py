from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sql_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

drop_new_data_alb = '''
DROP TABLE IF EXISTS aggregated_data_alb;
'''

t1 = PostgresOperator(
    task_id='drop_new_data_alb',
    sql=drop_new_data_alb,
    postgres_conn_id='analytical_db_connection_to_airflow',
    dag=dag,
)

extract_data_alb_sql = """
SELECT TMINS
    FROM (SELECT cast(TMINS as INT) From responses WHERE tmins != 'NA')sub_res
    LIMIT 1000;
"""

def extract_data_alb(ti):
    pg_hook = PostgresHook.get_hook('source_db_connection_to_airflow')
    results = pg_hook.get_records(extract_data_alb_sql)
    ti.xcom_push(key='results', value=results) # comunica la tarea 3 con las otras tareas

t2 = PythonOperator(task_id="task1", python_callable = extract_data_alb, dag = dag )
#inseta los datos en la db local
def insert_data(ti):
    imported = ti.xcom_pull(key='results')
    print(imported)
    pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
    target_fields = ['TMINS']
    pg_hook.insert_rows('new_data_alb', imported, target_fields)
        

t3 = PythonOperator(task_id="task2", python_callable = insert_data, dag = dag )


# extract_new_data_alb = """
#     CREATE TABLE IF NOT EXISTS new_data_alb AS
#     SELECT TMINS
#     FROM (SELECT cast(TMINS as INT) From responses WHERE tmins != 'NA')sub_res
# """

# # extracts new data since the last update of the data_summary table.
# t1 = PostgresOperator(
#     task_id='extract_new_data_alb',
#     sql=extract_new_data_alb,
#     postgres_conn_id='source_db_connection_to_airflow',
#     dag=dag,
# )


aggregate_new_data_alb = '''
    CREATE TABLE IF NOT EXISTS aggregated_data_alb AS
    SELECT AVG(TMINS)/60
    FROM new_data_alb;
'''

# aggregates the new data.
t4 = PostgresOperator(
    task_id='aggregate_new_data_alb',
    sql=aggregate_new_data_alb,
    postgres_conn_id='analytical_db_connection_to_airflow',
    dag=dag,
)

# update_data_summary = """
#     INSERT INTO data_summary (summary_date, total_orders, total_revenue)
#     SELECT date, total_orders, total_amount
#     FROM aggregated_orders
#     ON CONFLICT (summary_date) DO UPDATE SET
#         total_orders = orders_summary.total_orders + excluded.total_orders,
#         total_revenue = orders_summary.total_revenue + excluded.total_revenue;
# """

# updates the orders_summary table with the aggregated data.
# t3 = PostgresOperator(
#     task_id='update_orders_summary',
#     sql=update_orders_summary,
#     postgres_conn_id='your_connection_id',
#     dag=dag,
# )

# drop_new_data_alb = '''
# DROP TABLE new_data_alb;
# '''

# t4 = PostgresOperator(
#     task_id='drop_new_data_alb',
#     sql=drop_new_data_alb,
#     postgres_conn_id='source_db_connection_to_airflow',
#     dag=dag,
# )

# drop_aggregated_data = '''
# DROP TABLE aggregated_orders;
# '''

# t5 = PostgresOperator(
#     task_id="drop_aggregated_orders",
#     sql=drop_aggregated_orders,
#     postgres_conn_id="your_connection_id",
#     dag=dag,
# )

t1 >> t2 >> t3 >> t4
