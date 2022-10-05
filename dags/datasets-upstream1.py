
"""
This is an example of a DAG with dataset producer tasks.
upstream_task_1 updates dag1_dataset.

This is meant to show how data driven scheduling can work in Airflow 2.4.
"""


import pendulum

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

dag1_dataset = Dataset('s3://dataset1/output_1.txt')

with DAG(
    dag_id='dataset_upstream1',
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule='@daily',
    tags=['upstream'],
    doc_md=__doc__
) as dag:

    BashOperator(
        task_id='upstream_task_1', 
        bash_command="sleep 5",
        outlets=[dag1_dataset] 
    )

