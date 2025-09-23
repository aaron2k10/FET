from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.types import Numeric, VARCHAR
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
from datetime import datetime
from zowe.zos_files_for_zowe_sdk import Files
from Scripts.JobRun import main
from Scripts.Jobdetails import jbdt
from Scripts.Compare import compare
import os
import json
import re
import subprocess
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 25),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=None,
    catchup=False,
)

# Define the tasks
file_path1 = "/home/arunkua/home/arunkua/dags/dags/Scripts/media"
jobs = []
jobnames = []

def fetch_first_excel_filename(folder_path):
    """Fetch the first Excel filename in the given folder."""
    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            return filename
    return None

joballocation = fetch_first_excel_filename(file_path1)
if joballocation:
    file_path = os.path.join(file_path1, joballocation)
    df = pd.read_excel(file_path)
    for index, row in df.iterrows():
        match = re.search(r'\((\w+)\)', row['Jobs'])
        if match:
            jobnames.append(match.group(1))
            mainframe = PythonOperator(
                task_id=f"mainframe-{match.group(1)}",
                python_callable=main,
                op_args=[row],
                dag=dag,
            )
            jobs.append(mainframe)

def run_script(script_path):
    """Run a Python script from the given path."""
    try:
        exec(open(script_path).read())
    except Exception as e:
        print(f"Error running script {script_path}: {e}")

scripts_folder = '/home/arunkua/home/arunkua/dags/dags/Scripts/pythonscript'
dynamic_tasks = []
for script in os.listdir(scripts_folder):
    script_name = script[:-3]
    if script.endswith('.py') and script_name in jobnames:
        task = PythonOperator(
            task_id=f'{script_name}',
            python_callable=run_script,
            op_args=[os.path.join(scripts_folder, script)],
            dag=dag,
        )
        dynamic_tasks.append(task)

comparison = PythonOperator(
    task_id='compare',
    python_callable=compare,
    dag=dag,
)

jobdetails = PythonOperator(
    task_id='jobdetails',
    python_callable=jbdt,
    dag=dag,
)

# Set dependencies
jobdetails >> dynamic_tasks
jobdetails >> jobs
comparison.set_upstream(dynamic_tasks + jobs)
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.types import Numeric,VARCHAR
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
from datetime import datetime
from zowe.zos_files_for_zowe_sdk import Files
from Scripts.JobRun import main
from Scripts.Compare import compare
import os,json,re,subprocess
import pandas as pd


# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 25),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)

# Define the tasks
mainframe = PythonOperator(
    task_id='mainframe',
    python_callable=main,
    dag=dag,
)
def run_script(script_path):
    exec(open(script_path).read())

scripts_folder ='/home/kalharis/airflow/dags/Scripts/pythonscript'
dynamic_tasks = []
for script in os.listdir(scripts_folder):
    if script.endswith('.py'):
        script_name = script[:-3]
        task = PythonOperator(
            task_id=f'{script_name}',
            python_callable=run_script,
            op_args=[os.path.join(scripts_folder, script)],
            dag=dag,
        )
        dynamic_tasks.append(task)
comparison = PythonOperator(
    task_id='compare',
    python_callable=compare,
    dag=dag,
)
for task in dynamic_tasks:
    task >> comparison
mainframe >> comparison

"""
