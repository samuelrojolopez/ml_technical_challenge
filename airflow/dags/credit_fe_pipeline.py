import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import timedelta


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
            hdfs dfs -put -f $AIRFLOW_HOME/data/raw_data/dataset_credit_risk.csv /dataset_credit_risk
        """
    )

    creating_fraud_features_table = HiveOperator(
        task_id="creating_credit_fraud_feaures_table",
        hive_cli_conn_id="hive_conn",
        hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS credit_fraud_features(
                    id INT,
                    age INT,
                    years_on_the_job DOUBLE,
                    nb_previous_loans INT,
                    avg_amount_loans_previous DOUBLE,
                    flag_own_car INT,
                    status INT
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
    )

    process_features = SparkSubmitOperator(
        task_id="process_credit_dataset_from_hdfs",
        application="/usr/local/airflow/dags/scripts/credit_fe_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    get_processed_features = BashOperator(
        task_id="get_features_from_hdfs",
        bash_command=
        """
            hdfs dfs -get -f /dataset_credit_risk/credit_risk_features.parquet $AIRFLOW_HOME/data/processed_data/credit_risk_features.parquet
        """
    )

    copy_dataset_credit_raw >> creating_fraud_features_table
    creating_fraud_features_table >> process_features >> get_processed_features
