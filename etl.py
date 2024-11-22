import pandas as pd
from transform import clean_data, clean_boolean_columns, fill_missing_numeric, fill_missing_categorical, convert_float_to_int, split_personal_status, validate_data

# File paths
input_file_path = "credit_customer_data.csv"
output_file_path = "credit_customer_data_cleaned.csv"

# Define expected schema
expected_schema = {
    "checking_status": ["<0", "0<=X<20000", ">=20000", "no checking account"],
    "duration": "integer",
    "credit_history": ["no credits taken/all paid back duly",
                       "existing paid",
                       "delayed previously",
                       "critical/other existing credit",
                       "unknown"],
    "purpose": ["car",
                "radio/tv",
                "education",
                "new car",
                "used car", 
                'business', 
                'repairs',
                'other',
                'retraining',
                "furniture/equipment",
                "domestic appliance"],
    "credit_amount": "integer",
    "savings_status": ["<100", "100<=X<1000", "1000<=X<10000", ">=10000", "no known savings account", "unknown"],
    "employment": ["unemployed", "<1", "1<=X<4", "4<=X<7", ">=7"],
    "installment_commitment": "float",
    "marital_status": ['divorced/separated', 'divorced/dependent/married','single', 'married/widowed'],
    "sex": ["female", "male", "unknown"],
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

##### EXTRACT
def extract(file_path):
    try:        
        df = pd.read_csv(file_path)
        print(f"File is successfully extracted from {file_path}")
        return df
    except:
        print(f"There is an error loading your file in: {file_path}. Please check before trying again.")

##### TRANSFORM
def transform(df):
    # Clean the data
    df = clean_data(df)
    
    # Clean specified boolean columns
    boolean_to_clean = ['own_telephone', 'foreign_worker']
    df = clean_boolean_columns(df, boolean_to_clean)
    
    # Convert specific float columns to integer
    floats_to_integers = ["duration", "credit_amount", "residence_since", "age", "existing_credits", "num_dependents"]
    df = convert_float_to_int(df, floats_to_integers)
    
    # Split personal_status into sex and marital_status
    df = split_personal_status(df, 'personal_status')

    while True:
        # Define column groups
        numeric_columns = ["duration", "credit_amount", "installment_commitment", "residence_since", "age", "existing_credits", "num_dependents"]
        categorical_columns = [col for col in df.columns if col not in numeric_columns]

        # Transformations
        df = fill_missing_numeric(df, numeric_columns)
        df = fill_missing_categorical(df, categorical_columns)
        
        # Validate data after all transformations
        try:
            validate_data(df, expected_schema)
            print("File has been transformed successfully.")
            break  # Exit the loop if validation is successful
        except:
            print("There is an error transforming your file. We are trying again.")
            continue
    return df

##### LOAD
def load(df, output_path):
    try:
        df.to_csv(output_path, index=False)
        print(f"Cleaned file is successfully loaded to {output_path}")
    except:
        print("There is an error loading your file. Please check before trying again.")

# Main ETL process
if __name__ == "__main__":
    # Extract
    raw_data = extract(input_file_path)

    # Transform
    cleaned_data = transform(raw_data)

    # Load
    load(cleaned_data, output_file_path)