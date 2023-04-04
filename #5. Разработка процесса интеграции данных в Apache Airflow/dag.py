import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator

def get_return_reasons_new_data():
	sql_stmt = "SELECT return_reasons_new.Order_ID FROM return_reasons_new"
	pg_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow_data')
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()
	cursor.execute(sql_stmt)
	data = cursor.fetchall()
	return data

def get_orders_west_data():
	sql_stmt = "SELECT orders_west.Order_ID, orders_west.segment, orders_west.category FROM orders_west"
	pg_hook = PostgresHook(
	postgres_conn_id='postgres_default',
	schema='airflow_data'
	)
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()
	cursor.execute(sql_stmt)
	data = cursor.fetchall()
	return data

def process_orders_data(ti):
	orders_west = ti.xcom_pull(task_ids=['get_orders_west_data'])
	return_reasons = ti.xcom_pull(task_ids=['get_return_reasons_new_data'])

	flat_orders_west = [item for sublist in orders_west for item in sublist]
	flat_return_reasons = [item for sublist in return_reasons for item in sublist]

	orders_west = pd.DataFrame(data=flat_orders_west, columns=['order_id', 'segment', 'category'])

	return_reasons = pd.DataFrame(data=flat_return_reasons, columns=['order_id'])

	result = orders_west.merge(return_reasons, how='inner', on='order_id')

	result = result.groupby(['segment', 'category'])['order_id'].nunique().reset_index()

	result.to_csv('/tmp/result.csv', index=False)

with DAG(
	dag_id='project_konopelko',
	schedule_interval='@daily',
	start_date = datetime(year=2023, month=3, day=1),
	catchup=False
	) as dag:

	task_get_orders_west = PythonOperator(task_id='get_orders_west_data', python_callable=get_orders_west_data)

	task_get_return_reasons_new_data = PythonOperator(task_id='get_return_reasons_new_data', python_callable=get_return_reasons_new_data)

	task_process_orders_data = PythonOperator(task_id="process_orders_data", python_callable=process_orders_data)

	task_truncate_table = PostgresOperator(
	task_id='truncate_table',
	postgres_conn_id='postgres_default',
	sql="TRUNCATE TABLE orders_return_by_segment"
	)

	task_load_orders_data = BashOperator(
	task_id='load_orders_data',
	bash_command=(
	'psql -d airflow_data -U konopelko -c "'
	'\copy orders_return_by_segment(segment, category, orders_count) '
	"FROM '/tmp/result.csv' "
	"DELIMITER ',' "
	'CSV HEADER"'
	)
	)

task_get_return_reasons_new_data >> task_get_orders_west >> task_process_orders_data >> task_truncate_table >> task_load_orders_data
