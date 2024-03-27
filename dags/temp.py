import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


dag = DAG(
    dag_id = "temp",
    default_args = {
        "owner": "seongcheol Lee",
        "start_date": datetime.now()

    },
)


python_job = SparkSubmitOperator(
    task_id="generateRsi",
    conn_id="spark-conn",
    application="jobs/python/generateRsi.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar",
    dag=dag
)


python_job