
# ---
# scripts/validate.py
# Deliverables: A Python script for automated validation and a data quality report (simulated)

import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data():
    """
    Performs data validation checks on the raw datasets.
    Generates a simple data quality report.
    """
    logging.info("Starting data validation...")
    
    try:
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        report_path = os.path.join(os.path.dirname(__file__), '../data/validation_report.csv')

        df_telco = pd.read_csv(os.path.join(raw_data_dir, "telco_churn.csv"))
        df_bank = pd.read_csv(os.path.join(raw_data_dir, "bank_marketing.csv"))

        validation_issues = []

        # Check for duplicates
        if df_telco.duplicated().sum() > 0:
            validation_issues.append({"dataset": "telco_churn", "issue": "duplicates", "count": df_telco.duplicated().sum()})
        if df_bank.duplicated().sum() > 0:
            validation_issues.append({"dataset": "bank_marketing", "issue": "duplicates", "count": df_bank.duplicated().sum()})

        # Check for missing values
        telco_missing = df_telco.isnull().sum()
        for col, count in telco_missing.items():
            if count > 0:
                validation_issues.append({"dataset": "telco_churn", "issue": "missing_values", "column": col, "count": count})

        bank_missing = df_bank.isnull().sum()
        for col, count in bank_missing.items():
            if count > 0:
                validation_issues.append({"dataset": "bank_marketing", "issue": "missing_values", "column": col, "count": count})

        # Check for inconsistent data (e.g., 'TotalCharges' in Telco has ' ' and is an object)
        df_telco['TotalCharges'] = pd.to_numeric(df_telco['TotalCharges'], errors='coerce')
        if df_telco['TotalCharges'].isnull().sum() > 0:
            validation_issues.append({"dataset": "telco_churn", "issue": "inconsistent_data", "column": "TotalCharges", "count": df_telco['TotalCharges'].isnull().sum()})

        # Generate report
        report_df = pd.DataFrame(validation_issues)
        report_df.to_csv(report_path, index=False)
        logging.info(f"Validation complete. Report saved to {report_path}.")
        logging.info("\n" + report_df.to_string())

    except Exception as e:
        logging.error(f"Data validation failed with error: {e}")

if __name__ == '__main__':
    validate_data()
