import airflow
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta

import csv
import requests
import json

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="dataset_credit_to_hdfs", schedule_interval="@daily",
         default_args=default_args, catchup=False) as dag:

    copy_dataset_credit_raw = BashOperator(
        task_id="copy_dataset_to_hdfs",
        bash_command=
        """
            hdfs dfs -mkdir -p /dataset_credit_risk && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/dataset_credit_risk.csv /dataset_credit_risk
        """
    )

    read_credit_dataset = SparkSubmitOperator(
        task_id="read_credit_dataset_from_hdfs",
        application="/usr/local/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    copy_dataset_credit_raw >> read_credit_dataset
