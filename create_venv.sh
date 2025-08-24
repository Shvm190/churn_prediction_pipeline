# Create and activate venv
python -m venv .venv

source .venv/bin/activate

# Upgrade pip
python -m pip install --upgrade pip wheel setuptools

# Install Airflow + libs (Airflow pin is important)
# If your Python is 3.10, use constraints-3.10.txt; for 3.11, constraints-3.11.txt
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Project deps
pip install pandas numpy scikit-learn joblib pyyaml pyarrow dvc kagglehub datasets great_expectations ydata-profiling

sudo "/System/Volumes/Data/Applications/Python 3.11/Install Certificates.command"

