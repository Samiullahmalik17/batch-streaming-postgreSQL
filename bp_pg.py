from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import os
import psycopg2
import psycopg2.extras

# Define your PostgreSQL connection details
# Define a connection in Airflow's Connection UI
pg_conn_id = 'bp_pg'

# Define the directory path
csv_output_dir = '/home/sami/airflow/output/'

# Ensure the directory exists, or create it if it doesn't
if not os.path.exists(csv_output_dir):
    os.makedirs(csv_output_dir)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bp_dag_',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # Run every 2 minutes
    catchup=False,  # Disable catchup
)

# Task 1: Execute an SQL query


def execute_sql_query():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Define your SQL query
    sql_query = """
    SELECT *
    FROM stream_data
    --WHERE Date >= NOW() - interval '10 years'
    --AND Date <= NOW();
    """

    cursor.execute(sql_query)
    result = cursor.fetchall()
    conn.close()

    return result


execute_sql_task = PythonOperator(
    task_id='execute_sql_query',
    python_callable=execute_sql_query,
    dag=dag,
)

# Task 2: Export the query results to a CSV file


def export_to_csv(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='execute_sql_query')

    if result:
        # Define the column names
        column_names = ['id', 'date', 'customer_id', 'transaction_id',
                        'sku_category', 'sku', 'quantity', 'sales_amount']

        csv_filename = f"{csv_output_dir}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header using the extracted column names
            csv_writer.writerow(column_names)
            # Write the data
            csv_writer.writerows(result)
    else:
        # Handle the case where there is no data in the result
        print("===No data to export to CSV.===")


export_csv_task = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_to_csv,
    provide_context=True,
    dag=dag,
)

execute_sql_task >> export_csv_task
