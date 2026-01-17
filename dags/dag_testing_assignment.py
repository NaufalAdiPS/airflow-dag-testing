"""
DAG: data_validation_dag

Simulasi pipeline ETL sederhana yang dapat diuji
menggunakan pytest dan airflow tasks test.

Owner: data-engineering-team
Schedule: @daily
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator





# Python Functions

def extract_data(**context):
    """
    Extract data dari sumber (simulasi).
    """
    try:
        data = [
            {"name": "apple"},
            {"name": "banana"},
        ]
        logging.info("Extracted data: %s", data)

        # Push data ke XCom
        context["ti"].xcom_push(key="raw_data", value=data)
        return data
    except Exception as e:
        logging.error("Error in extract_data", exc_info=True)
        raise


def transform_data(data):
    """
    Ubah field 'name' menjadi uppercase.
    """
    transformed_data = []

    for item in data:
        transformed_data.append(
            {"name": item["name"].upper()}
        )

    logging.info("Transformed data: %s", transformed_data)
    return transformed_data


def transform_task_callable(**context):
    """
    Task wrapper untuk transform_data dengan XCom.
    """
    try:
        ti = context["ti"]
        raw_data = ti.xcom_pull(
            task_ids="extract_task",
            key="raw_data"
        )

        # Fallback untuk airflow tasks test (isolated run)
        if not raw_data:
            logging.warning(
                "XCom tidak ditemukan, menggunakan data dummy untuk testing. "
            )
            raw_data = [
                {"name": "apple"},
                {"name": "banana"},
            ]

        transformed = transform_data(raw_data)

        ti.xcom_push(
            key="transformed_data",
            value=transformed
        )

        return transformed

    except Exception:
        logging.error("Error in transform_task", exc_info=True)
        raise



def load_data(**context):
    """
    Me Load data dari hasil transform.
    """
    try:
        ti = context["ti"]
        transformed_data = ti.xcom_pull(
            task_ids="transform_task",
            key="transformed_data"
        )

        # Fallback untuk isolated task test
        if not transformed_data:
            logging.warning(
                "XCom tidak ditemukan, menggunakan data dummy untuk testing."
            )
            transformed_data = [
                {"name": "APPLE"},
                {"name": "BANANA"},
            ]

        # Simulasi load
        logging.info("Loaded data: %s", transformed_data)

        return transformed_data

    except Exception:
        logging.error("Error in load_data", exc_info=True)
        raise




# DAG Definition

default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_validation_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["testing", "validation", "dag-testing"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task_callable,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
