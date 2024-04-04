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
#    schedule_interval="0 * * * *"
    schedule_interval="0 0 * * *",  # 한국 시간으로 오전 9시 정각
#    schedule_interval="59 8 * * *"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

collect_binance = SparkSubmitOperator(
    task_id="collect_kline_1d",
    conn_id="spark-conn",
    application="jobs/python/binanceApi.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar,/opt/airflow/dags/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    dag=dag
)

generate_rsi = SparkSubmitOperator(
    task_id="generate_rsi_1d",
    conn_id="spark-conn",
    application="jobs/python/updateRsi.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar,/opt/airflow/dags/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    dag=dag
)

catch_diversions = SparkSubmitOperator(
    task_id="catch_diversions_1d",
    conn_id="spark-conn",
    application="jobs/python/catchDiversions.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar,/opt/airflow/dags/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    dag=dag
)

check_convert = SparkSubmitOperator(
    task_id="check_conver_1d",
    conn_id="spark-conn",
    application="jobs/python/checkConvert.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar,/opt/airflow/dags/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

start >> collect_binance >> generate_rsi >> catch_diversions >> check_convert >> end
check_convert >> end