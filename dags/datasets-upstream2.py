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
) as dag2:

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
