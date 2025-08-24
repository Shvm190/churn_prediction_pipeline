# flow_prefect.py
from prefect import flow, task
import subprocess

@task
def run_ingestion():
    subprocess.run(["python", "scripts/ingest.py"], check=True)
    subprocess.run(["dvc", "add", "data/raw/telco_churn.csv", "data/raw/hf_bank_customer_support.csv"], check=True)
    subprocess.run(["git", "add", "*.dvc"], check=True)
    subprocess.run(["git", "commit", "-m", "Version raw ingested data"], check=True)
    subprocess.run(["dvc", "push"], check=True)

@task
def run_validation():
    subprocess.run(["python", "scripts/validate.py"], check=True)
    subprocess.run(["dvc", "add", "data/validation_report.csv"], check=True)
    subprocess.run(["git", "add", "*.dvc"], check=True)
    subprocess.run(["git", "commit", "-m", "Version validation report"], check=True)
    subprocess.run(["dvc", "push"], check=True)

@task
def run_prepare():
    subprocess.run(["python", "scripts/prepare.py"], check=True)
    subprocess.run(["dvc", "add", "data/prepared/customer_data_cleaned.csv"], check=True)
    subprocess.run(["git", "add", "*.dvc"], check=True)
    subprocess.run(["git", "commit", "-m", "Version prepared data"], check=True)
    subprocess.run(["dvc", "push"], check=True)

@task
def run_transform():
    subprocess.run(["python", "scripts/transform.py"], check=True)
    subprocess.run(["dvc", "add", "data/processed/customer_features.db"], check=True)
    subprocess.run(["git", "add", "*.dvc"], check=True)
    subprocess.run(["git", "commit", "-m", "Version transformed features"], check=True)
    subprocess.run(["dvc", "push"], check=True)

@task
def run_training():
    subprocess.run(["python", "scripts/model_training.py", "--db-path", "data/processed/customer_features.db"], check=True)
    subprocess.run(["dvc", "add", "models/model.pkl"], check=True)
    subprocess.run(["git", "add", "*.dvc"], check=True)
    subprocess.run(["git", "commit", "-m", "Version trained model"], check=True)
    subprocess.run(["dvc", "push"], check=True)

@flow
def churn_pipeline():
    run_ingestion()
    run_validation()
    run_prepare()
    run_transform()
    run_training()

if __name__ == "__main__":
    churn_pipeline()
