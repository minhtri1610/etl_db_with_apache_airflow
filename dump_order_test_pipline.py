from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import tarfile
import psycopg2
from psycopg2 import sql
import subprocess
from airflow.models import Variable
from pathlib import Path

db_params = {
    'user': 'postgres',
    'password': 'admin123',
    'host': '192.168.1.2',
    'port': '5432'
}

url_api_production = "https://order-system.management-partners.co.jp/api"
url_api_test = "https://testt.order-system.trust-growth.co.jp/api"
api_key = ""


# from airflow.operators.bash_operator import BashOperator

# Define your default_args, schedule_interval, and DAG
default_args = {
    'owner': 'order_team',
    'start_date': datetime(2023, 10, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_database_clone_and_import',
    default_args=default_args,
    description='DAG to clone a database from an API, create a new database, and import an SQL file',
    schedule_interval=None,  # Define your schedule interval
    catchup=False,  # Set to True if you want historical runs
)

# Define Python functions for your tasks
def clone_database_from_api(type):
    # Define the API URL
    api_url_export = url_api_production + '/pg_export_sql'
    Variable.set('api_action', url_api_production)
    if type == 'test':
        Variable.set('api_action', url_api_test)
        api_url_export = url_api_test + '/pg_export_sql'

    # Define query parameters, including your API key
    params = {
        'key_api': api_key,
    }

    try:
        print(api_url_export)
        response = requests.get(api_url_export, params=params)
        
        if response.status_code == 200:
            # API request was successful
            data = response.json()  # Parse the JSON response
            # You can now work with the 'data' variable, which contains the response from the API
            url = data['path']
            output_file = data['file_name'] + ".tar.gz"
            download_and_tar_file(url, output_file)
            Variable.set('database_name', data['file_name'])
            return data['file_name']
        else:
            print(f"API request failed with status code: {response.status_code}")
            return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    # Your code to clone the database from an API goes here


def download_and_tar_file(url, output_file):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(output_file, 'wb') as file:
                file.write(response.content)
            print(f"File downloaded as {output_file}")

            input_file = output_file
            output_dir = "./"

            try:
                with tarfile.open(input_file, "r:gz") as tar:
                    tar.extractall(output_dir)
                print(f"File '{input_file}' has been successfully extracted to '{output_dir}'")
                return input_file
            except Exception as e:
                print(f"An error occurred: {str(e)}")
                
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def create_database():
    try:
        new_db_name = Variable.get('database_name')
        if new_db_name is not None:
            conn = psycopg2.connect(**db_params)
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if the database exists before creating it
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(sql.Literal(new_db_name)))
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
                print(f"Database '{new_db_name}' created successfully.")
            else:
                print(f"Database '{new_db_name}' already exists.")

            cursor.close()
            conn.close()
            return new_db_name
    except Exception as e:
        print(f"Error creating the database: {str(e)}")
    # Your code to create a new database goes here

def import_sql_file_to_database():
    try:
        new_db_name = Variable.get('database_name')
        if new_db_name is not None:
            sql_file = './' + new_db_name
            pg_restore_path = r'pg_restore' 
            process = subprocess.Popen(
                [
                    pg_restore_path,  # Assuming that 'pg_restore' is in your system's PATH
                    f'--dbname=postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:5432/{new_db_name}',
                    sql_file
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            print("Standard Output:")
            print(stdout.decode("utf-8"))
            print("Standard Error:")
            print(stderr.decode("utf-8"))

            if process.returncode == 0:
                print(f'Successfully imported SQL file into {new_db_name}')
                return 1
            else:
                print(f'Error importing SQL file. Return code: {process.returncode}')
                return 0
        return False
    except Exception as e:
        print(f'Issue with the db restore: {e}')
    # Your code to import an SQL file into the database goes here


def unlink_file_dump():

    url_api =  Variable.get('api_action')
    api_url = url_api + "/unlink_file"

    file_db = Variable.get('database_name')
    file_name = file_db + ".tar.gz"


    file_sql = Path(file_db)
    file_tar = Path(file_name)

    # Define the payload as a dictionary
    payload = {
        "key_api": api_key,
        "file_name": file_name
    }

    # Make the POST request
    response = requests.post(api_url, data=payload)

    # Check the response status code
    if response.status_code == 200:
        print("POST request successful.")
        if file_sql.exists():
            # Delete the file
            file_sql.unlink()
            print(f"File {file_sql} has been deleted.")
        if file_tar.exists():
            # Delete the file
            file_tar.unlink()
            print(f"File {file_tar} has been deleted.")
        # You can access the response content using response.text
    else:
        print(f"POST request failed with status code: {response.status_code}")
        print(response.text)  # Display the response content in case of an error


def update_data_supplier():
    # reset suppliers_send_setting
    try:
        new_database = Variable.get('database_name')
        conn = psycopg2.connect(**db_params,database=new_database)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("UPDATE suppliers_send_setting SET mail_1 = 'epsminhtrihue@gmail.com', mail_2 = NULL, mail_3 = NULL, mail_4 = NULL, mail_5 = NULL")
        return True
    except Exception as e:
            print(f'Error: {e}')


# Define PythonOperator tasks
clone_db_task_test = PythonOperator(
    task_id='clone_database_test',
    python_callable=clone_database_from_api,
    op_args=['test'],
    dag=dag,
)


create_db_task = PythonOperator(
    task_id='create_database',
    python_callable=create_database,
    dag=dag,
)

import_sql_task = PythonOperator(
    task_id='import_sql_file',
    python_callable=import_sql_file_to_database,
    dag=dag,
)

unlink_file_downloaded = PythonOperator(
    task_id='id_unlink_file_downloaded',
    python_callable=unlink_file_dump,
    dag=dag,
)


# Define task dependencies
clone_db_task_test >> create_db_task >> import_sql_task >> unlink_file_downloaded
