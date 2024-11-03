from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="olist_analysis_process",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Olist Sales Analysis Process",
    start_date=days_ago(1),
)

analysis_process = SparkSubmitOperator(
    application="/spark-scripts/spark-olist-analysis.py",
    conn_id="spark_main",
    task_id="olist_analysis_task",
    jars="/spark-scripts/jars/postgresql-42.2.20.jar",
    dag=dag,
)

analysis_process