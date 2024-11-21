import pandas as pd
from transform import clean_data, fill_missing_numeric, fill_missing_categorical, validate_data

# File paths
input_file_path = "credit_customer_data.csv"
output_file_path = "credit_customer_data_cleaned.csv"

# Schema definition
expected_schema = {
    "checking_status": ["<0", "0<=X<20000", ">=20000", "no checking account"],
    "duration": "integer",
    "credit_history": ["no credits taken/all paid back duly", "existing paid", "delayed previously", "critical/other existing credit", "unknown"],
    "purpose": ["car", "television", "furniture/equipment", "used car", "business", "domestic appliance", "repairs", "other", "retraining", "unknown"],
    "credit_amount": "integer",
    "savings_status": ["<100", "100<=X<1000", "1000<=X<10000", ">=10000", "no known savings account", "unknown"],
    "employment": ["unemployed", "<1", "1<=X<4", "4<=X<7", ">=7"],
    "installment_commitment": "float",
    "personal_status": [
        "male div/sep",
        "female div/dep/mar",
        "male single",
        "male mar/wid",
        "female single",
        "unknown"
    ],
    "other_parties": ["none", "co-applicant", "guarantor", "unknown"],
    "residence_since": "integer",
    "property_magnitude": ["real estate", "life insurance", "car", "no known property", "unknown"],
    "age": "integer",
    "other_payment_plans": ["none", "bank", "stores", "unknown"],
    "housing": ["rent", "own", "free", "unknown"],
    "existing_credits": "integer",
    "job": ["unemployed", "unskilled", "skilled", "high qualif/self emp/mgmt", "unknown"],
    "num_dependents": "integer",
    "own_telephone": "boolean",
    "foreign_worker": "boolean",
    "class": ["good", "bad"],
}

##### VALIDATION FUNCTION
class ValidationError(Exception):
    pass

##### EXTRACT
def extract(file_path):
    df = pd.read_csv(file_path)
    df.dtypes
    return df

##### TRANSFORM
def transform(df):
    # Clean the data
    df = clean_data(df)

    while True:  # Change from False to True to ensure the loop executes
        # Define column groups
        numeric_columns = ["duration", "credit_amount", "installment_commitment", "residence_since", "age", "existing_credits", "num_dependents"]
        categorical_columns = [col for col in df.columns if col not in numeric_columns]

        # Transformations
        df = fill_missing_numeric(df, numeric_columns)
        df = fill_missing_categorical(df, categorical_columns)
        
        # Validate data after all transformations
        try:
            validate_data(df, expected_schema)
        except ValidationError as e:
            print(f"Validation error after transformation: {e}")
            continue  # Continue the loop to re-check after transformations

        # Convert specific float columns to integer
        for col in ["duration", "credit_amount", "residence_since", "age", "existing_credits", "num_dependents"]:
            if col in df.columns and pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].fillna(0).astype(int)  # Fill NaNs with 0 before converting

        # After transformations, re-validate the data
        try:
            validate_data(df, expected_schema)
        except ValidationError as e:
            print(f"Validation error after transformation: {e}")
            continue  # Continue the loop to re-check after transformations

    return df

##### LOAD
def load(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

# Main ETL process
if __name__ == "__main__":
    # Extract
    raw_data = extract(input_file_path)

    # Transform
    cleaned_data = transform(raw_data)

    # Load
    load(cleaned_data, output_file_path)