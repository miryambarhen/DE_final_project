import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Add the path to the project directory to the Python path
project_dir = '/tmp/pycharm_project_4'
batch_dir = os.path.join(project_dir, 'batch')  # Add the path to the batch directory
sys.path.append(project_dir)
sys.path.append(batch_dir)  # Add the path to the batch directory to the Python path

default_args = {
    'owner': 'Naya Trades',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
    'retries': 0
}

dag = DAG(
    'daily_loading',
    default_args=default_args,
    description='Loads daily data into MySQL database',
    schedule_interval='0 0 * * 1-5',  # Run on weekdays at 00:00
    catchup=False
)

load_data_task = BashOperator(
    task_id='load_data',
    bash_command='python {}/batch/daily_loading.py'.format(project_dir),
    dag=dag
)

load_data_task


