import sys
from airflow.models import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Set project directory path
project_dir = '/tmp/pycharm_project_4'
# Add the project directory to system path so modules can be imported
sys.path.insert(0, project_dir)


# Function to run a Python script using subprocess module
def run_python_script(script_path):
    import subprocess
    subprocess.run(['python', script_path])


# Default DAG arguments
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23)
}

# Define the DAG
dag = DAG(
    dag_id='streaming_dag',
    default_args=default_args,
    description='Get real-time New York stock exchange data',
    schedule_interval='58 8 * * 1-5',  # Run on weekdays at 08:58 AM
)

# Define the task to run producer
producer_task = PythonOperator(
    task_id='run_producer',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/streaming/current_price_producer.py'},
    dag=dag
)

# Define the task to run kafka
kafka_task = PythonOperator(
    task_id='run_kafka',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/streaming/consumer_kafka.py'},
    dag=dag
)

# Define the task to run hdfs
hdfs_task = PythonOperator(
    task_id='run_hdfs',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/streaming/consumer_hdfs.py'},
    dag=dag
)

# Define the task to run mongo
mongo_task = PythonOperator(
    task_id='run_mongo',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/streaming/consumer_mongo.py'},
    dag=dag
)

# Define the task to run emails sending
emails_task = PythonOperator(
    task_id='run_send_emails',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/streaming/stream_send_emails.py'},
    dag=dag
)


def stop_dag_if_after_16pm():
    now_new_york = datetime.now(pytz.timezone('US/Eastern'))
    if now_new_york.hour >= 16:
        raise ValueError('DAG stop running after 16:00')


stop_operator = PythonOperator(
    task_id='stop_dag',
    python_callable=lambda: print("not 16 yet"),
    on_failure_callback=stop_dag_if_after_16pm,
    dag=dag,
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> emails_task
dummy_task >> kafka_task
dummy_task >> hdfs_task
dummy_task >> mongo_task
dummy_task >> producer_task
dummy_task >> stop_operator
