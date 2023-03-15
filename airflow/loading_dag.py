import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Add the project directory to the Python path
sys.path.insert(0, '/tmp/pycharm_project_4')

def install_dependencies():
    try:
        from pip._internal import main as pipmain
    except:
        from pip import main as pipmain
    pipmain(['install', '-r', '/tmp/pycharm_project_4/requirements.txt'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 14),
    'retries': 1
}

dag = DAG(
    'daily_loading_dag',
    default_args=default_args,
    description='Get and load daily data on monday-friday at 00:00',
    schedule_interval='0 0 * * 1-5'
)

# Define the PythonOperator that installs the required packages
install_deps = PythonOperator(
    task_id='install_dependencies',
    python_callable=install_dependencies,
    dag=dag
)

# Define the BashOperator that runs the script
run_script = BashOperator(
    task_id='run_daily_loading_script',
    bash_command='python /tmp/pycharm_project_4/batch/daily_loading.py',
    dag=dag
)

# Define the dependencies between the tasks
install_deps >> run_script
