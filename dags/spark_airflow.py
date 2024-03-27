import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "owner": "seongcheol Lee",
        "start_date": datetime.now()
    },
    schedule_interval="0 * * * *"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="1_hour_kline_data_extract",
    conn_id="spark-conn",
    application="jobs/python/binanceApi.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar",
    dag=dag
)

python_job2 = PythonOperator(
    task_id="transfer_4hour",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

python_job3 = PythonOperator(
    task_id="transfer_1day",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

python_job5 = PythonOperator(
    task_id="save",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)


python_job4 = PythonOperator(
    task_id="send_topic",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

python_job6 = PythonOperator(
    task_id="Generate_Diversions",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> [python_job2 , python_job3] >> python_job6 >> [python_job4,  python_job5] >> end
