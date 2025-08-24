# dags/churn_pipeline.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "shivam",
    "retries": 5,
    "retry_delay": timedelta(seconds=5),
}

def run_cmd(cmd):
    subprocess.run(cmd, check=True)

@dag(
    dag_id="churn_pipeline_taskflow",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
)
def churn_pipeline():
    
    @task
    def ingestion():
        run_cmd(["python", "scripts/ingest.py"])
        run_cmd(["dvc", "add", "data/raw/telco_churn.csv", "data/raw/hf_bank_customer_support.csv"])
        run_cmd(["git", "add", "*.dvc"])
        run_cmd(["git", "commit", "-m", "Version raw ingested data"])
        run_cmd(["dvc", "push"])

    @task
    def validation():
        run_cmd(["python", "scripts/validate.py"])
        run_cmd(["dvc", "add", "data/validation_report.csv"])
        run_cmd(["git", "add", "*.dvc"])
        run_cmd(["git", "commit", "-m", "Version validation report"])
        run_cmd(["dvc", "push"])

    @task
    def prepare():
        run_cmd(["python", "scripts/prepare.py"])
        run_cmd(["dvc", "add", "data/prepared/customer_data_cleaned.csv"])
        run_cmd(["git", "add", "*.dvc"])
        run_cmd(["git", "commit", "-m", "Version prepared data"])
        run_cmd(["dvc", "push"])

    @task
    def transform():
        run_cmd(["python", "scripts/transform.py"])
        run_cmd(["dvc", "add", "data/processed/customer_features.db"])
        run_cmd(["git", "add", "*.dvc"])
        run_cmd(["git", "commit", "-m", "Version transformed features"])
        run_cmd(["dvc", "push"])

    @task
    def training():
        run_cmd(["python", "scripts/model_training.py", "--db-path", "data/processed/customer_features.db"])
        run_cmd(["dvc", "add", "models/model.pkl"])
        run_cmd(["git", "add", "*.dvc"])
        run_cmd(["git", "commit", "-m", "Version trained model"])
        run_cmd(["dvc", "push"])

    ingestion() >> validation() >> prepare() >> transform() >> training()

churn_pipeline()
