
# ---
# scripts/prepare.py
# Deliverables: A Jupyter notebook/Python script showcasing the process, visualizations, and a clean dataset

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def prepare_data():
    """
    Cleans and preprocesses the raw data and performs basic EDA.
    """
    logging.info("Starting data preparation...")
    
    try:
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        prepared_data_dir = os.path.join(os.path.dirname(__file__), '../data/prepared')
        os.makedirs(prepared_data_dir, exist_ok=True)

        # Load datasets
        df_telco = pd.read_csv(os.path.join(raw_data_dir, "telco_churn.csv"))
        df_bank = pd.read_csv(os.path.join(raw_data_dir, "bank_marketing.csv"))

        # Clean 'TotalCharges' column in Telco data
        df_telco['TotalCharges'] = pd.to_numeric(df_telco['TotalCharges'], errors='coerce')
        df_telco.dropna(subset=['TotalCharges'], inplace=True)
        logging.info("Handled missing values in 'TotalCharges'.")
        
        # Join datasets (simulating combining data from multiple sources)
        # Assuming we can join on customerID, but since these are different datasets,
        # we'll just concatenate them for a single, large dataset for the purpose of the demo
        df_bank.rename(columns={'age': 'customerID'}, inplace=True)  # Simple rename for a demo join
        df_merged = pd.merge(df_telco, df_bank, on='customerID', how='inner')
        logging.info(f"Merged datasets. New shape: {df_merged.shape}")

        # Basic EDA and visualization
        plt.style.use('seaborn-v0_8-whitegrid')
        
        # EDA: Churn distribution
        plt.figure(figsize=(6, 4))
        sns.countplot(x='Churn', data=df_telco)
        plt.title('Distribution of Churn')
        plt.savefig(os.path.join(prepared_data_dir, 'churn_distribution.png'))
        plt.close()
        logging.info("Generated churn distribution plot.")
        
        # EDA: Monthly Charges vs Churn
        plt.figure(figsize=(8, 6))
        sns.boxplot(x='Churn', y='MonthlyCharges', data=df_telco)
        plt.title('Monthly Charges by Churn Status')
        plt.savefig(os.path.join(prepared_data_dir, 'monthly_charges_vs_churn.png'))
        plt.close()
        logging.info("Generated monthly charges vs churn plot.")

        # Save the cleaned dataset
        cleaned_file_path = os.path.join(prepared_data_dir, 'customer_data_cleaned.csv')
        df_merged.to_csv(cleaned_file_path, index=False)
        logging.info(f"Cleaned dataset saved to {cleaned_file_path}.")

    except Exception as e:
        logging.error(f"Data preparation failed with error: {e}")

if __name__ == '__main__':
    prepare_data()
