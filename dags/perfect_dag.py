from prefect import flow, task
import subprocess

def dvc_add_safe(*files, commit_message=None):
    """Add files to DVC only if there are changes."""
    try:
        # Check if DVC thinks the files are changed
        result = subprocess.run(["dvc", "status"] + list(files), capture_output=True, text=True)
        if result.stdout.strip():  # there is some change
            subprocess.run(["dvc", "add"] + list(files), check=True)
            subprocess.run(["git", "add", "*.dvc"], check=True)
            if commit_message:
                subprocess.run(["git", "commit", "-m", commit_message], check=True)
            subprocess.run(["dvc", "push"], check=True)
        else:
            print(f"No changes detected for {files}, skipping DVC add/push.")
    except subprocess.CalledProcessError as e:
        print(f"Skipping DVC add for {files}, caught error: {e}")

@task
def run_ingestion():
    subprocess.run(["python", "scripts/ingest.py"], check=True)
    dvc_add_safe("data/raw/telco_churn.csv", "data/raw/hf_bank_customer_support.csv",
                 commit_message="Version raw ingested data")

@task
def run_validation():
    subprocess.run(["python", "scripts/validate.py"], check=True)
    dvc_add_safe("data/validation_report.csv", commit_message="Version validation report")

@task
def run_prepare():
    subprocess.run(["python", "scripts/prepare.py"], check=True)
    dvc_add_safe("data/prepared/customer_data_cleaned.csv", commit_message="Version prepared data")

@task
def run_transform():
    subprocess.run(["python", "scripts/transform.py"], check=True)
    dvc_add_safe("data/processed/customer_features.db", commit_message="Version transformed features")

@task
def run_training():
    subprocess.run(["python", "scripts/model_training.py", "--db-path", "data/processed/customer_features.db"], check=True)
    dvc_add_safe("models/model.pkl", commit_message="Version trained model")

@flow
def churn_pipeline():
    run_ingestion()
    run_validation()
    run_prepare()
    run_transform()
    run_training()

if __name__ == "__main__":
    churn_pipeline()
