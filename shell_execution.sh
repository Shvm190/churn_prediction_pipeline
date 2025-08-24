#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Initial Setup (Run Once Manually) ---
# dvc init
# git add .dvc .gitignore && git commit -m "Initialize DVC"

# --- Ingest Raw Data ---
echo "--- Running Data Ingestion ---"
python scripts/ingest.py
dvc add data/raw/telco_churn.csv data/raw/hf_bank_customer_support.csv
git add data/raw/telco_churn.csv.dvc data/raw/hf_bank_customer_support.csv.dvc
git commit -m "Version raw ingested data"
dvc push
echo "--- Data Ingestion Complete ---"
echo ""

# --- Validate Data ---
echo "--- Running Data Validation ---"
python scripts/validate.py
dvc add data/validation_report.csv
git add data/validation_report.csv.dvc
git commit -m "Version validation report"
dvc push
echo "--- Data Validation Complete ---"
echo ""

# --- Prepare Data ---
echo "--- Running Data Preparation ---"
python scripts/prepare.py
dvc add data/prepared/customer_data_cleaned.csv
git add data/prepared/customer_data_cleaned.csv.dvc
git commit -m "Version prepared data"
dvc push
echo "--- Data Preparation Complete ---"
echo ""

# --- Transform Features ---
echo "--- Running Feature Transformation ---"
python scripts/transform.py
dvc add data/processed/customer_features.db
git add data/processed/customer_features.db.dvc
git commit -m "Version transformed features"
dvc push
echo "--- Feature Transformation Complete ---"
echo ""

# --- Train Model ---
echo "--- Running Model Training ---"
python scripts/model_training.py --db-path data/processed/customer_features.db
dvc add models/model.pkl
git add models/model.pkl.dvc
git commit -m "Version trained model"
dvc push
echo "--- Model Training Complete ---"
echo ""
