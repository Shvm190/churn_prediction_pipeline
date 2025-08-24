"""
Airflow DAG to orchestrate an end-to-end data management pipeline for customer churn prediction.

This DAG defines a series of tasks for:
1. Data Ingestion: Fetching two datasets from the internet.
2. Data Validation: Checking for missing values, data types, and duplicates.
3. Data Preparation: Cleaning, handling missing values, and performing EDA.
4. Data Transformation: Engineering new features and storing them in an SQLite database.
5. Data Versioning: Using DVC and Git to version datasets at each stage, fully automated within the DAG.
"""
from __future__ import annotations
import pendulum
import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Set up logging for the DAG
log = logging.getLogger(__name__)

# Define the default arguments for the DAG. These are inherited by all tasks.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG itself
with DAG(
    dag_id='end_to_end_churn_pipeline_automated_dvc',  # New DAG ID to distinguish
    default_args=default_args,
    description='An end-to-end data management pipeline with automated DVC/Git versioning.',
    schedule_interval=timedelta(days=1),  # Schedule the pipeline to run daily
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['churn', 'ml', 'pipeline', 'dvc'],
) as dag:
    # 1. Data Ingestion Task
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=lambda: os.system("python3 /opt/airflow/scripts/ingest.py"),
    )

    # 2. Automated DVC and Git for Raw Data
    add_and_commit_raw_data = BashOperator(
        task_id='add_and_commit_raw_data',
        bash_command='''
            dvc add /opt/airflow/data/raw/telco_churn.csv /opt/airflow/data/raw/bank_marketing.csv &&
            git add /opt/airflow/data/.gitignore /opt/airflow/data/raw.dvc &&
            git commit -m "Version raw ingested data from pipeline run {{ ds }}"
        ''',
        cwd="/opt/airflow",  # Set the working directory to the project root
    )
    
    # 3. Data Validation Task
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=lambda: os.system("python3 /opt/airflow/scripts/validate.py"),
    )

    # 4. Data Preparation Task
    prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=lambda: os.system("python3 /opt/airflow/scripts/prepare.py"),
    )

    # 5. Automated DVC and Git for Prepared Data
    add_and_commit_prepared_data = BashOperator(
        task_id='add_and_commit_prepared_data',
        bash_command='''
            dvc add /opt/airflow/data/prepared/customer_data_cleaned.csv &&
            git add /opt/airflow/data/prepared.dvc &&
            git commit -m "Version prepared data from pipeline run {{ ds }}"
        ''',
        cwd="/opt/airflow",
    )

    # 6. Data Transformation and Storage Task
    transform_and_store = PythonOperator(
        task_id='transform_and_store',
        python_callable=lambda: os.system("python3 /opt/airflow/scripts/transform.py"),
    )

    # 7. Automated DVC and Git for Transformed Data
    add_and_commit_transformed_data = BashOperator(
        task_id='add_and_commit_transformed_data',
        bash_command='''
            dvc add /opt/airflow/data/processed/customer_features.db &&
            git add /opt/airflow/data/processed.dvc &&
            git commit -m "Version transformed features from pipeline run {{ ds }}"
        ''',
        cwd="/opt/airflow",
    )

    # Define the task dependencies to create the automated pipeline flow
    ingest_data >> add_and_commit_raw_data >> validate_data >> prepare_data >> add_and_commit_prepared_data >> transform_and_store >> add_and_commit_transformed_data
