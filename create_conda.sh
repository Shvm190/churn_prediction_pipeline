

conda create -n churn_pipeline python=3.11 -y
conda activate churn_pipeline

export AIRFLOW_VERSION=2.9.3
export PYTHON_VERSION="$(python --version | cut -d' ' -f2 | cut -d. -f1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow db init

export AIRFLOW_HOME=/Users/homeaccount/PycharmProjects/dmml/churn_prediction_pipeline/airflow_home

mkdir -p $AIRFLOW_HOME
airflow db migrate   # new way (instead of db init)


mkdir -p $AIRFLOW_HOME/dags
cp dags/* $AIRFLOW_HOME/dags/

airflow users create \
  --username admin \
  --firstname Shivam \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password admin

# mkdir -p ~/airflow/dags
# cp churn_pipeline.py ~/airflow/dags/


