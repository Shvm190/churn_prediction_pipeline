# scripts/ingest.py
# Deliverables: Python scripts for ingestion and screenshots of ingested data

import pandas as pd
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_data():
    """
    Ingests data from two different sources and saves them in the raw data directory.
    Simulates fetching data from two different sources (CSV files).
    """
    try:
        # Create directories if they don't exist
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        os.makedirs(raw_data_dir, exist_ok=True)

        # Ingest Telco Customer Churn data
        logging.info("Starting ingestion of Telco Customer Churn data...")
        telco_url = "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-premise/main/data/Telco-Customer-Churn.csv"
        df_telco = pd.read_csv(telco_url)
        telco_file_path = os.path.join(raw_data_dir, "telco_churn.csv")
        df_telco.to_csv(telco_file_path, index=False)
        logging.info(f"Successfully ingested Telco data. Rows: {len(df_telco)}, Columns: {len(df_telco.columns)}")

        # Ingest Bank Marketing data
        logging.info("Starting ingestion of Bank Marketing data...")
        bank_url = "https://raw.githubusercontent.com/subhadip-13/bank-marketing/main/bank.csv"
        df_bank = pd.read_csv(bank_url, sep=';')
        bank_file_path = os.path.join(raw_data_dir, "bank_marketing.csv")
        df_bank.to_csv(bank_file_path, index=False)
        logging.info(f"Successfully ingested Bank Marketing data. Rows: {len(df_bank)}, Columns: {len(df_bank.columns)}")

    except Exception as e:
        logging.error(f"Data ingestion failed with error: {e}")
        # A real-world scenario would include more robust error handling like alerts
    
if __name__ == '__main__':
    ingest_data()
