from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Joel',
    'start_date': days_ago(0),
    'email': ['joel@dummymail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

destination_directory = '/home/project/airflow/dags/finalassignment/'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf ' + destination_directory + 'tolldata.tgz -C ' + destination_directory,
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f"""
        cat {destination_directory}vehicle-data.csv |
        cut -d ',' -f1-4 > {destination_directory}csv_data.csv
    """,
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"""
        cat {destination_directory}tollplaza-data.tsv |
        cut -d $'\t' -f5,6,7 | 
        tr $'\t' ',' > {destination_directory}tsv_data.csv
    """,
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f"""
        cat {destination_directory}payment-data.txt |
        rev |
        cut -d ' ' -f1,2 |
        rev |
        tr ' ' ',' > {destination_directory}fixed_width_data.csv
    """,
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f"""
        paste -d ',' {destination_directory}csv_data.csv \
        {destination_directory}tsv_data.csv \
        {destination_directory}fixed_width_data.csv > {destination_directory}extracted_data.csv \
    """,
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f"""
        paste -d ',' \
        <(cut -d ',' -f 1-3 {destination_directory}extracted_data.csv) \
        <(cut -d ',' -f 4 {destination_directory}extracted_data.csv | tr '[:lower:]' '[:upper:]') \
        <(cut -d ',' -f 5- {destination_directory}extracted_data.csv) > {destination_directory}transformed_data.csv
    """,
    dag=dag
)

unzip_data >> extWhich of these lists three of the Apache Airflow's main features?

1 point

Drag-and-drop accessibility to ETL tools, commercial, and no need to know SQL



Automatic java code, AWS glue, enterprise solution



Standard Python, many plug-and-play integrations, open source



Handles ETL and ELT, stream processing, highly scalable platform


ract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data