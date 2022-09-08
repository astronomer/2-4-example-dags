import pendulum

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

"""
This is an example of a DAG with dataset producer tasks.
upstream_task_1 updates dag1_dataset.
"""

dag1_dataset = Dataset('s3://dataset1/output_1.txt')

with DAG(
    dag_id='dataset_upstream1',
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule='@daily',
    tags=['upstream'],
) as dag:

    BashOperator(
        task_id='upstream_task_1', 
        bash_command="sleep 5",
        outlets=[dag1_dataset] 
    )

