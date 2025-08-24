# scripts/profile_data.py

import pandas as pd
import os
from ydata_profiling import ProfileReport
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def profile_data():
    """
    Generates a ydata-profiling report after explicitly defining column types.
    """
    logging.info("Starting data profiling...")
    
    try:
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        
        # Read the data
        df_telco = pd.read_csv(os.path.join(raw_data_dir, "telco_churn.csv"))
        
        df_telco['TotalCharges'] = df_telco['TotalCharges'].str.strip()
        df_telco['TotalCharges'] = pd.to_numeric(df_telco['TotalCharges'])
        
        df_telco = df_telco.fillna({'TotalCharges': 0})

        # Create the profile report
        profile_telco = ProfileReport(df_telco, title="Telco Churn Dataset")
        
        profile_telco.to_file(os.path.join(raw_data_dir, "telco_churn_profile.html"))
        logging.info("Telco Churn profile report saved.")

        logging.info("Data profiling complete.")

    except Exception as e:
        logging.error(f"Data profiling failed with error: {e}")

if __name__ == '__main__':
    profile_data()