from itertools import zip_longest
from airflow import DAG, XComArg
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import re
import logging

task_logger = logging.getLogger('airflow.task')

S3_BUCKET_1 = "myexamplebucketone"
S3_BUCKET_2 = "myexamplebuckettwo"

with DAG(
    dag_id="2_3_example_dag_zip_workaround",
    start_date=datetime(2022,9,1),
    schedule_interval=None,
    catchup=False
) as dag:

    list_files_in_S3_one = S3ListOperator(
        task_id="list_files_in_S3_one",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET_1
    )

    list_files_in_S3_two = S3ListOperator(
        task_id="list_files_in_S3_two",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET_2
    )

    query_snowflake = SnowflakeOperator(
        task_id="query_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql="""
            SELECT CAST(DATE AS TEXT) AS DATE, CUSTOMER AS CUSTOMER
            FROM SANDBOX.TAMARAFINGERLIN.ZIP_EXAMPLE
            ORDER BY DATE ASC
        """
    )

    @task
    def zip_manually(S3_file_list_1, S3_file_list_2, snowflake_information):
        
        zipped_file_names = list(
            zip_longest(
                S3_file_list_1,
                S3_file_list_2,
                snowflake_information,
                fillvalue="MISSING INFO"
            )
        )

        return zipped_file_names

    @task 
    def compare_dates_logfiles(input_tuple):
        date_file_1 = re.findall("(\d+_\d+_\d+)", input_tuple[0])[0]
        name_file_2 = re.findall("\d+_\d+_\d+_(.+).txt", input_tuple[1])[0]
        snowflake_entry = input_tuple[2]

        date_file_1_converted_format = date_file_1.replace("_", "-")
        
        if (
            snowflake_entry['DATE'] == date_file_1_converted_format
            and snowflake_entry['CUSTOMER'] == name_file_2
        ):
            task_logger.info(f"Matching entries on {date_file_1}")
        else:
            raise ValueError(
                f"""Mismatch Snowflake and S3 data!
                On {date_file_1} {S3_BUCKET_2}'s file is named {name_file_2},
                while the Snowflake entry is {snowflake_entry['CUSTOMER']}"""
            )

    manually_zipped_list = zip_manually(
        XComArg(list_files_in_S3_one),
        XComArg(list_files_in_S3_two),
        XComArg(query_snowflake)
    )

    compare_dates_logfiles.expand(input_tuple=manually_zipped_list)