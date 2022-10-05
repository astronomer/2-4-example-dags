"""
This is an example of a DAG with dataset producer tasks.
upstream_task_2 and upstream_task_3 update dag2_dataset.

This is meant to show off data driven scheduling in Airflow 2.4
"""

import pendulum

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator



dag2_dataset = Dataset('s3://dataset2/output_2.txt')

with DAG(
    dag_id='dataset_upstream2',
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=['upstream'],
    doc_md=__doc__
) as dag:

    task1= BashOperator(
        task_id='upstream_task_2', 
        bash_command="sleep 5",
        outlets=[dag2_dataset]
    )

    task2= BashOperator(
        task_id='upstream_task_3', 
        bash_command="sleep 30",
        outlets=[dag2_dataset]
    )

    task1 >> task2
