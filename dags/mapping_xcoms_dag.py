from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow import XComArg

with DAG(
    dag_id="mapping_xcoms_dag",
    start_date=datetime(2022,7,1),
    schedule_interval=None,
    catchup=False
) as dag:

    # upstream and downstream task are defined using the TaskFlowAPI
    @task
    def one_two_three_TF():
        return [1,2,3]
    
    @task
    def plus_10_TF(x):
        return x+10

    plus_10_TF.partial().expand(x=one_two_three_TF())


    # upstream task is defined using the TaskFlowAPI, downstream task is 
    # a classical operator

    @task
    def one_two_three_TF():
        return [[1],[2],[3]]

    def plus_10_classic(x):
        return x+10

    #one_two_three_task = one_two_three_TF()

    plus_10_task = PythonOperator.partial(
        task_id="plus_10_task",
        python_callable=plus_10_classic
    ).expand(
        op_args=one_two_three_TF()
    )


    # upstream task is defined using a classical operator, downstream task
    # is defined using the TaskFlowAPI

    def one_two_three_classical():
        return [1,2,3]

    @task
    def plus_10_TF(x):
        return x+10

    one_two_three_task = PythonOperator(
        task_id="one_two_three_task",
        python_callable=one_two_three_classical
    )

    plus_10_TF.partial().expand(x=XComArg(one_two_three_task))


    # both upstream and downstream tasks are defined using classical operators

    def one_two_three_classical():
        return [[1],[2],[3]]

    def plus_10_classic(x):
        return x+10

    one_two_three_task_2 = PythonOperator(
        task_id="one_two_three_task_2",
        python_callable=one_two_three_classical
    )

    plus_10_task_2 = PythonOperator.partial(
        task_id="plus_10_task_2",
        python_callable=plus_10_classic
    ).expand(
        op_args=XComArg(one_two_three_task_2)
    )

    one_two_three_task_2 >> plus_10_task_2

