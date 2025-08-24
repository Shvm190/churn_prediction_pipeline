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
from datetime import timedelta

# Import the necessary modules from the Airflow provider
from airflow.decorators import dag, task

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

# Define the DAG itself using the @dag decorator
@dag(
    dag_id='end_to_end_churn_pipeline_automated_dvc',
    default_args=default_args,
    description='An end-to-end data management pipeline with automated DVC/Git versioning.',
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['churn', 'ml', 'pipeline', 'dvc'],
)
def churn_prediction_pipeline():
    # 1. Data Ingestion Task using @task decorator
    @task
    def ingest_data_task():
        """Fetches raw data from the internet."""
        os.system("python3 /opt/airflow/scripts/ingest.py")

    # 2. Automated DVC and Git for Raw Data using @task.bash
    @task.bash
    def add_and_commit_raw_data_task():
        """Uses DVC and Git to version the raw data."""
        return """
            dvc add /opt/airflow/data/raw/telco_churn.csv /opt/airflow/data/raw/bank_marketing.csv &&
            git add /opt/airflow/data/.gitignore /opt/airflow/data/raw.dvc &&
            git commit -m "Version raw ingested data from pipeline run {{ ds }}"
        """

    # 3. Data Validation Task using @task decorator
    @task
    def validate_data_task():
        """Validates the ingested data."""
        os.system("python3 /opt/airflow/scripts/validate.py")

    # 4. Data Preparation Task using @task decorator
    @task
    def prepare_data_task():
        """Prepares and cleans the data."""
        os.system("python3 /opt/airflow/scripts/prepare.py")

    # 5. Automated DVC and Git for Prepared Data using @task.bash
    @task.bash
    def add_and_commit_prepared_data_task():
        """Uses DVC and Git to version the prepared data."""
        return """
            dvc add /opt/airflow/data/prepared/customer_data_cleaned.csv &&
            git add /opt/airflow/data/prepared.dvc &&
            git commit -m "Version prepared data from pipeline run {{ ds }}"
        """

    # 6. Data Transformation and Storage Task using @task decorator
    @task
    def transform_and_store_task():
        """Transforms data and stores it in a database."""
        os.system("python3 /opt/airflow/scripts/transform.py")

    # 7. Automated DVC and Git for Transformed Data using @task.bash
    @task.bash
    def add_and_commit_transformed_data_task():
        """Uses DVC and Git to version the transformed data."""
        return """
            dvc add /opt/airflow/data/processed/customer_features.db &&
            git add /opt/airflow/data/processed.dvc &&
            git commit -m "Version transformed features from pipeline run {{ ds }}"
        """

    # Define the task dependencies using bitshift operators
    # This creates the same pipeline flow as the original code
    ingest_data_task() >> add_and_commit_raw_data_task() >> validate_data_task() >> prepare_data_task() >> add_and_commit_prepared_data_task() >> transform_and_store_task() >> add_and_commit_transformed_data_task()

# Call the function to create the DAG instance
churn_prediction_pipeline()
