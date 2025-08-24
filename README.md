# churn_prediction_pipeline
Data Management for Machine Learning - ML Orchestration


# Setup Directory
git clone https://github.com/Shvm190/churn_prediction_pipeline.git

cd churn_prediction_pipeline


# Create .venv
chmod +x create_venv.sh

./create_venv.sh

# Docker Run
```
chmod +x scripts/entrypoint.sh

docker-compose up --build

```

Docker CLose:
```
docker-compose down -v 
docker-compose down --volumes --remove-orphans
```

# Perfect & Cron:
```
# Run daily at 1 AM
0 1 * * * python /path/to/flow_prefect.py >> /path/to/logs/churn.log 2>&1
```


# TroubleShoot

Exhaustive close Docker:
```
docker-compose down --volumes --remove-orphans
```

If getting .ipynb issue:
```
rm churn_prediction_pipeline/airflow/dags/.ipynb_checkpoints/churn_pipeline_dag-checkpoint.py
```

great expectation init
```
python -m great_expectations init
```
