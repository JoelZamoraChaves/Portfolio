from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Joel',
    'start_date': days_ago(0),
    'email': ['johez1933@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag-practice',
    default_args=default_args,
    description='dag-practice',
    schedule_interval=timedelta(days=1),
)

download = BashOperator(
    task_id='download',
    bash_command='curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" -o web-server-access-log.txt',
    dag=dag,
)

extract = BashOperator(
    task_id='extract',
    bash_command='cat web-server-access-log.txt | cut -d "#" -f1,4 > extracted.txt',
    dag=dag
)

transform = BashOperator(
    task_id='transform',
    bash_command='cat extracted.txt | tr [:"lower":] [:"upper":] > capitalized.txt',
    dag=dag
)

load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt',
    dag=dag
)

download >> extract >> transform >> load