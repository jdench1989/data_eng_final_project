from datetime import datetime, timedelta

from airflow import DAG

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
    'example_gina_dag',
    default_args=default_args,
    description='take all the data',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2020, 4, 28),
    catchup=False,
)

# ###### alb - ALBANIA #####
# choose_data_alb = """
# SELECT count(*)
# FROM test_alb;
# """

# def choosing_data_alb(ti):
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     results = pg_hook.get_records(choose_data_alb)
#     if results[0][0] is None:
#         ti.xcom_push(key='offset', value='0')
#     else:
#         ti.xcom_push(key='offset', value=results[0])
    
# t1 = PythonOperator(task_id="task1", python_callable = choosing_data_alb, dag = dag )

# extract_data_alb_sql = """
# SELECT cnt, TMINS, escs, pared, hisei, homepos, durecec, belong
#     FROM responses
#     OFFSET %s
#     LIMIT 1000;
# """

# def extract_data_alb(ti):
#     imported = ti.xcom_pull(key = 'offset')
#     pg_hook = PostgresHook.get_hook('rds_source_db_alb')
#     results = pg_hook.get_records(extract_data_alb_sql, parameters = [imported])
#     ti.xcom_push(key='results', value=results) # comunica la tarea 3 con las otras tareas

# t2 = PythonOperator(task_id="task2", python_callable = extract_data_alb, dag = dag )

# def insert_data_alb(ti):
#     imported = ti.xcom_pull(key='results')
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
#     pg_hook.insert_rows('test_alb', imported, target_fields)

# t3 = PythonOperator(task_id="task3", python_callable = insert_data_alb, dag = dag )

# def insert_data_all_countries_alb(ti):
#     imported = ti.xcom_pull(key='results')
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
#     pg_hook.insert_rows('test', imported, target_fields)

# t4 = PythonOperator(task_id="task4", python_callable = insert_data_all_countries_alb, dag = dag )

# ###### arg - ARGENTINA #####
# choose_data_arg = """
# SELECT count(*)
# FROM test_arg;
# """

# def choosing_data_arg(ti):
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     results = pg_hook.get_records(choose_data_arg)
#     if results[0][0] is None:
#         ti.xcom_push(key='offset', value='0')
#     else:
#         ti.xcom_push(key='offset', value=results[0])
    
# t5 = PythonOperator(task_id="task5", python_callable = choosing_data_arg, dag = dag )

# extract_data_arg_sql = """
# SELECT cnt, TMINS, escs, pared, hisei, homepos, durecec, belong
#     FROM responses
#     OFFSET %s
#     LIMIT 1000;
# """

# def extract_data_arg(ti):
#     imported = ti.xcom_pull(key = 'offset')
#     pg_hook = PostgresHook.get_hook('rds_source_db_alb')
#     results = pg_hook.get_records(extract_data_arg_sql, parameters = [imported])
#     ti.xcom_push(key='results', value=results) # comunica la tarea 3 con las otras tareas

# t6 = PythonOperator(task_id="task6", python_callable = extract_data_arg, dag = dag )

# def insert_data_arg(ti):
#     imported = ti.xcom_pull(key='results')
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
#     pg_hook.insert_rows('test_arg', imported, target_fields)

# t7 = PythonOperator(task_id="task7", python_callable = insert_data_arg, dag = dag )

# def insert_data_all_countries_arg(ti):
#     imported = ti.xcom_pull(key='results')
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
#     pg_hook.insert_rows('test', imported, target_fields)

# t8 = PythonOperator(task_id="task8", python_callable = insert_data_all_countries_arg, dag = dag )

# t1 >> t2 >> t3 >> t4
# t5 >> t6 >> t7 >> t8

country_list = ['alb', 'arg', 'aus', 'aut', 'bel', 'bgr',
                'bih', 'blr', 'bra', 'brn', 'can', 'che',
                'chi', 'col', 'cri', 'cze', 'deu', 'dnk',
                'dom', 'esp']

def choosing_data(**kwargs):
    ti = kwargs['ti']
    country_list = kwargs['country_list']
    for country_code in country_list:
        choose_data = f"""
        SELECT count(*)
        FROM test_{country_code};
        """
        pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
        results = pg_hook.get_records(choose_data)
        if results[0][0] is None:
            ti.xcom_push(key=f'offset_{country_code}', value='0')
        else:
            ti.xcom_push(key=f'offset_{country_code}', value=results[0])

def extract_data(**kwargs):
    ti = kwargs['ti']
    country_list = kwargs['country_list']
    for country_code in country_list:
        extract_data_sql = """
        SELECT cnt, TMINS, escs, pared, hisei, homepos, durecec, belong
        FROM responses
        OFFSET %s
        LIMIT 1000;
        """
        imported = ti.xcom_pull(key = f'offset_{country_code}')
        pg_hook = PostgresHook.get_hook(f'rds_source_db_{country_code}')
        results = pg_hook.get_records(extract_data_sql, parameters = [imported])
        ti.xcom_push(key=f'results_{country_code}', value=results) # comunica la tarea 3 con las otras tareas

def insert_data(**kwargs):
    ti = kwargs['ti']
    country_list = kwargs['country_list']
    for country_code in country_list:
        imported = ti.xcom_pull(key=f'results_{country_code}')
        pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
        target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
        pg_hook.insert_rows(f'test_{country_code}', imported, target_fields)

def insert_data_all_countries(**kwargs):
    ti = kwargs['ti']
    country_list = kwargs['country_list']
    for country_code in country_list:
        imported = ti.xcom_pull(key=f'results_{country_code}')
        pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
        target_fields = ['cnt', 'TMINS', 'escs', 'pared', 'hisei', 'homepos', 'durecec', 'belong']
        pg_hook.insert_rows('test', imported, target_fields)

t1 = PythonOperator(task_id=f"task1", python_callable = choosing_data, op_kwargs={'country_list': country_list}, dag = dag )
t2 = PythonOperator(task_id=f"task2", python_callable = extract_data, op_kwargs={'country_list': country_list}, dag = dag )
t3 = PythonOperator(task_id=f"task3", python_callable = insert_data, op_kwargs={'country_list': country_list}, dag = dag )
t4 = PythonOperator(task_id=f"task4", python_callable = insert_data_all_countries, op_kwargs={'country_list': country_list}, dag = dag )
t1 >> t2 >> t3 >> t4


# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator

# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
# }

# dag = DAG(
#     'sql_dag',
#     default_args=default_args,
#     description='A simple tutorial DAG',
#     schedule_interval=timedelta(days=1),
#     start_date=datetime(2023, 4, 28),
#     catchup=False,
# )

# drop_new_data_alb = '''
# DROP TABLE IF EXISTS aggregated_data_alb;
# '''

# t1 = PostgresOperator(
#     task_id='drop_new_data_alb',
#     sql=drop_new_data_alb,
#     postgres_conn_id='analytical_db_connection_to_airflow',
#     dag=dag,
# )

# extract_data_alb_sql = """
# SELECT TMINS
#     FROM (SELECT cast(TMINS as INT) From responses WHERE tmins != 'NA')sub_res
#     LIMIT 1000;
# """

# def extract_data_alb(ti):
#     pg_hook = PostgresHook.get_hook('source_db_connection_to_airflow')
#     results = pg_hook.get_records(extract_data_alb_sql)
#     ti.xcom_push(key='results', value=results) # comunica la tarea 3 con las otras tareas

# t2 = PythonOperator(task_id="task1", python_callable = extract_data_alb, dag = dag )
# #inseta los datos en la db local
# def insert_data(ti):
#     imported = ti.xcom_pull(key='results')
#     print(imported)
#     pg_hook = PostgresHook.get_hook('analytical_db_connection_to_airflow')
#     target_fields = ['TMINS']
#     pg_hook.insert_rows('new_data_alb', imported, target_fields)
        

# t3 = PythonOperator(task_id="task2", python_callable = insert_data, dag = dag )


# # extract_new_data_alb = """
# #     CREATE TABLE IF NOT EXISTS new_data_alb AS
# #     SELECT TMINS
# #     FROM (SELECT cast(TMINS as INT) From responses WHERE tmins != 'NA')sub_res
# # """

# # # extracts new data since the last update of the data_summary table.
# # t1 = PostgresOperator(
# #     task_id='extract_new_data_alb',
# #     sql=extract_new_data_alb,
# #     postgres_conn_id='source_db_connection_to_airflow',
# #     dag=dag,
# # )


# aggregate_new_data_alb = '''
#     CREATE TABLE IF NOT EXISTS aggregated_data_alb AS
#     SELECT AVG(TMINS)/60
#     FROM new_data_alb;
# '''

# # aggregates the new data.
# t4 = PostgresOperator(
#     task_id='aggregate_new_data_alb',
#     sql=aggregate_new_data_alb,
#     postgres_conn_id='analytical_db_connection_to_airflow',
#     dag=dag,
# )

# # update_data_summary = """
# #     INSERT INTO data_summary (summary_date, total_orders, total_revenue)
# #     SELECT date, total_orders, total_amount
# #     FROM aggregated_orders
# #     ON CONFLICT (summary_date) DO UPDATE SET
# #         total_orders = orders_summary.total_orders + excluded.total_orders,
# #         total_revenue = orders_summary.total_revenue + excluded.total_revenue;
# # """

# # updates the orders_summary table with the aggregated data.
# # t3 = PostgresOperator(
# #     task_id='update_orders_summary',
# #     sql=update_orders_summary,
# #     postgres_conn_id='your_connection_id',
# #     dag=dag,
# # )

# # drop_new_data_alb = '''
# # DROP TABLE new_data_alb;
# # '''

# # t4 = PostgresOperator(
# #     task_id='drop_new_data_alb',
# #     sql=drop_new_data_alb,
# #     postgres_conn_id='source_db_connection_to_airflow',
# #     dag=dag,
# # )

# # drop_aggregated_data = '''
# # DROP TABLE aggregated_orders;
# # '''

# # t5 = PostgresOperator(
# #     task_id="drop_aggregated_orders",
# #     sql=drop_aggregated_orders,
# #     postgres_conn_id="your_connection_id",
# #     dag=dag,
# # )

# t1 >> t2 >> t3 >> t4
