import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set project directory path
project_dir = '/tmp/pycharm_project_4'
# Add the project directory to system path so modules can be imported
sys.path.insert(0, project_dir)


# Function to run a Python script using subprocess module
def load_data_func():
    import subprocess
    subprocess.run(['python', f'{project_dir}/batch/daily_loading.py'])


# Default DAG arguments
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5, 22, 0)
}

# Define the DAG
dag = DAG(
    dag_id='loading_dag',
    default_args=default_args,
    description='Loads daily data into MySQL database',
    schedule_interval='0 22 * * 1-5',  # Run on weekdays at 22:00
)

# Define the task to load daily data
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_func,
    dag=dag
)

load_data_task
