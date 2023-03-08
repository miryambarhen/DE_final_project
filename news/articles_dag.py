from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5, 22, 0)
}

dag = DAG(
    dag_id='articles_dag',
    default_args=default_args,
    description='Get and send articles every night at 22:00',
    schedule_interval='0 22 * * *',
)

get_articles = BashOperator(
    task_id='get_articles',
    bash_command='python /tmp/pycharm_project_4/news/get_articles.py',
    env={'PYTHONPATH': '/tmp/pycharm_project_4/logs/logging_config.py'},
    dag=dag,
)

send_articles = PythonOperator(
    task_id='send_articles',
    python_callable=lambda: exec(compile(open('/tmp/pycharm_project_4/news/send_articles.py').read(),
                                            '/tmp/pycharm_project_4/news/send_articles.py', 'exec')),
    dag=dag
)

get_articles >> send_articles

# sudo cp /tmp/pycharm_project_4/news/articles_dag.py /home/naya/miniconda3/envs/airflow/lib/python3.6/site-packages/airflow/example_dags/articles_dag.py
