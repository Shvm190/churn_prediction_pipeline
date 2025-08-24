# scripts/clean_total_charges.py

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_total_charges(value):
    """
    Cleans a single value in the TotalCharges column.
    Converts numbers in strings to floats and handles empty strings.
    """
    if isinstance(value, str):
        # Remove any leading/trailing spaces
        stripped_value = value.strip()
        if stripped_value == '':
            return np.nan  # Convert empty strings to NaN
        try:
            return float(stripped_value)
        except ValueError:
            return np.nan # Catch any other non-numeric strings
    return value  # Return the value as is if it's already a number

def validate_and_clean_data():
    try:
        logging.info("Starting data cleaning for TotalCharges column...")
        
        raw_data_path = os.path.join(os.path.dirname(__file__), '../data/raw/telco_churn.csv')
        df_telco = pd.read_csv(raw_data_path)
        
        # Count and report issues before cleaning
        initial_blanks = (df_telco['TotalCharges'] == ' ').sum()
        initial_non_numeric = df_telco['TotalCharges'].apply(lambda x: not isinstance(x, (int, float)) and x.strip().isdigit() == False).sum()
        
        logging.info(f"Found {initial_blanks} blank spaces and other {initial_non_numeric} non-numeric values in TotalCharges before cleaning.")

        # Apply the row-by-row cleaning function
        df_telco['TotalCharges'] = df_telco['TotalCharges'].apply(clean_total_charges)
        
        # Count and report issues after cleaning
        final_nan_count = df_telco['TotalCharges'].isnull().sum()
        logging.info(f"After cleaning, TotalCharges has {final_nan_count} NaN values.")

        # Save the cleaned data
        processed_data_path = os.path.join(os.path.dirname(__file__), '../data/processed/cleaned_telco_churn.csv')
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        df_telco.to_csv(processed_data_path, index=False)
        
        logging.info(f"Data cleaning complete. Cleaned data saved to {processed_data_path}")
        
    except FileNotFoundError:
        logging.error("The 'telco_churn.csv' file was not found.")
    except Exception as e:
        logging.error(f"Data cleaning failed with error: {e}")

if __name__ == '__main__':
    validate_and_clean_data()