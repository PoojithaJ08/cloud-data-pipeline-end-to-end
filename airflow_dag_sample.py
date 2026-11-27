#!/usr/bin/env python3
"""
Apache Airflow DAG Sample — Daily SAP Master Data Pipeline
Poojitha Krishna Jonnalagadda - Data Engineer (Python, SQL, Spark/PySpark)

Demonstration sample: the orchestration wrapper for the pipeline in this
project (pyspark_etl_sample.py). On my AWS programmes I scheduled pipelines
with Apache Airflow and set up failure alerts so load problems surfaced the
same morning instead of at month end; on Azure programmes the equivalent
runs as Azure Data Factory pipelines with exception alerts (see
cloud-pipeline-notes.md). This DAG shows that pattern: ingest -> Spark
transform/validate -> data-quality gate -> load, with an alert on failure.

ILLUSTRATIVE — requires an Airflow environment to run (pip install
apache-airflow, plus the Spark provider for SparkSubmitOperator). Paths and
connection IDs are placeholders.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def alert_on_failure(context):
    """Same-morning failure alerting: page the data team with the failed
    task, the run date and the log link instead of letting a broken load
    sit unnoticed. (Notification channel wired per environment.)"""
    ti = context["task_instance"]
    print(f"ALERT: task {ti.task_id} failed in DAG {ti.dag_id} "
          f"for run {context['ds']} - see log: {ti.log_url}")


def quality_gate(**context):
    """Fail the run if the validation report shows a reconciliation
    mismatch - a load that does not reconcile does not proceed."""
    with open("/data/pipeline/output/validation-report.txt") as fh:
        report = fh.read()
    if "MISMATCH" in report:
        raise ValueError("Validation reconciliation mismatch - load held")
    return "quality gate passed"


default_args = {
    "owner": "poojitha",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="sap_master_data_pipeline_daily",
    description="Ingest -> transform -> validate -> load SAP master and "
                "transactional data (customers, materials, sales orders)",
    schedule="0 5 * * *",  # 05:00 daily, before the business day starts
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["sap", "master-data", "etl"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_extracts",
        bash_command="python3 /data/pipeline/ingest_extracts.py "
                     "--date {{ ds }} --dest /data/pipeline/landing/",
    )

    transform_validate = BashOperator(
        task_id="spark_transform_and_validate",
        # In production this is a SparkSubmitOperator / EMR step / Databricks
        # job running pyspark_etl_sample.py; the local runner stands in here.
        bash_command="python3 /data/pipeline/run_pipeline_local.py "
                     "/data/pipeline/landing/{{ ds }} /data/pipeline/output/",
    )

    gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=quality_gate,
    )

    load = BashOperator(
        task_id="load_to_warehouse",
        bash_command="python3 /data/pipeline/load_warehouse.py "
                     "--input /data/pipeline/output/ --date {{ ds }}",
    )

    ingest >> transform_validate >> gate >> load
