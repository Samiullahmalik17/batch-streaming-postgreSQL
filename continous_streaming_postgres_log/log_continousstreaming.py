from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import os
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define your PostgreSQL connection details (Ensure it's defined in Airflow's Connection UI)
pg_conn_id = 'bp_pg'

# Define the directory path and the CSV file name
csv_output_dir = '/home/sami/airflow/output/'
csv_filename = 'streaming_log.csv'

# Ensure the directory exists, or create it if it doesn't
os.makedirs(csv_output_dir, exist_ok=True)


default_args = {
    'owner': 'sami',
    'start_date': datetime(2023, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG(
    'cs_dag',
    default_args=default_args,
    schedule_interval=timedelta(seconds=1),  # Run every 30 seconds
    catchup=False,  # Disable catchup
)

# Task 1: Execute an SQL query


def fetch_log_from_db():
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Define your SQL query
    sql_query = """
    SELECT *
    FROM stream_data_modifications
    --WHERE Date >= NOW() - interval '1 years'
    --AND Date <= NOW();
    """

    cursor.execute(sql_query)
    result = cursor.fetchall()
    conn.close()

    return result


fetch_logs = PythonOperator(
    task_id='fetch_log_from_db',
    python_callable=fetch_log_from_db,
    dag=dag,
)

# Task 2: Append the query results to the same CSV file


def get_last_processed_id(csv_filename):
    # Initialize the last processed ID as -1, indicating no data has been processed yet
    last_processed_id = -1

    if os.path.isfile(csv_filename):
        # If the CSV file exists, read it to find the last processed ID
        with open(csv_filename, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # Skip the header row
            for row in csv_reader:
                last_processed_id = max(last_processed_id, int(row[0]))

    return last_processed_id


def append_log_to_csv(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='fetch_log_from_db')

    if result:
        # Define the column names
        column_names = ['modification_id', 'record_id', 'modified_column_name', 'old_value',
                        'new_value', 'modification_timestamp']

        # Properly reference the global csv_filename
        csv_filepath = os.path.join(csv_output_dir, csv_filename)

        # Check if the file already exists
        file_exists = os.path.isfile(csv_filepath)

        with open(csv_filepath, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            # If the file doesn't exist, write the header
            if not file_exists:
                csv_writer.writerow(column_names)

            # Determine the last processed ID
            last_processed_id = get_last_processed_id(csv_filepath)

            # Append only the new data
            new_data = [row for row in result if row[0] > last_processed_id]
            csv_writer.writerows(new_data)
    else:
        # Handle the case where there is no data in the result
        print("No data to append to CSV.")


append_log = PythonOperator(
    task_id='append_log_to_csv',
    python_callable=append_log_to_csv,
    provide_context=True,
    dag=dag,
)

fetch_logs >> append_log

print("Task Executed")
