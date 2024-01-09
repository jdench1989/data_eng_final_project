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
#     schedule_interval=timedelta(minutes=2),
#     start_date=datetime(2023, 4, 28),
#     catchup=False,
# )

# select_max_id = """
# SELECT SUM(batch_size)
#     FROM aggregated_data_alb
# """

# def extract_max_student_id(ti):
#     pg_hook = PostgresHook.get_hook('data_loading_alb')
#     results = pg_hook.get_records(select_max_id)
#     print(results)
#     print(results[0])
#     if results[0][0] is None:
#         ti.xcom_push(key='offset', value='0')
#     else:
#         ti.xcom_push(key='offset', value=results[0])

# t1 = PythonOperator(task_id="task1", python_callable = extract_max_student_id, dag = dag )

# extract_data_alb_sql = """
# SELECT TMINS as student_id
#     FROM (SELECT cast(TMINS as INT) From responses WHERE tmins != 'NA' OFFSET %s)sub_res
#     LIMIT 1000;
# """

# def extract_data_alb(ti):
#     imported = ti.xcom_pull(key='offset')
#     pg_hook = PostgresHook.get_hook('final_project_lhpw_alb')
#     results = pg_hook.get_records(extract_data_alb_sql, parameters=[imported])
#     ti.xcom_push(key='results', value=results) # comunica la tarea 3 con las otras tareas

# t2 = PythonOperator(task_id="task2", python_callable = extract_data_alb, dag = dag )
# #inseta los datos en la db local
# def insert_data(ti):
#     imported = ti.xcom_pull(key='results')
#     pg_hook = PostgresHook.get_hook('data_loading_alb')
#     target_fields = ['TMINS']
#     pg_hook.insert_rows('new_data_alb', imported, target_fields)
        

# t3 = PythonOperator(task_id="task3", python_callable = insert_data, dag = dag )


# aggregate_new_data_alb = '''
#     INSERT INTO aggregated_data_alb
#     SELECT SUM(TMINS), COUNT(TMINS)
#     FROM new_data_alb;
# '''

# # aggregates the new data.
# t4 = PostgresOperator(
#     task_id='aggregate_new_data_alb',
#     sql=aggregate_new_data_alb,
#     postgres_conn_id='data_loading_alb',
#     dag=dag,
# )


# clean_new_data_alb = '''
# DELETE FROM new_data_alb;
# '''

# t5 = PostgresOperator(
#     task_id='drop_new_data_alb',
#     sql=clean_new_data_alb,
#     postgres_conn_id='data_loading_alb',
#     dag=dag,
# )

# t1 >> t2 >> t3 >> t4 >> t5
