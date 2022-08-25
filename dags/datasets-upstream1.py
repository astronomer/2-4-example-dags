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
) as dag1:

    BashOperator(
        task_id='upstream_task_1', 
        bash_command="sleep 5",
        outlets=[dag1_dataset] 
    )

