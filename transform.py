### ETL

import pandas as pd

##### VALIDATION FUNCTION
class ValidationError(Exception):
    pass

# Validation function
def validate_data(df, schema):
    validation_issues = []

    for col, expected in schema.items():
        # Check if the column exists
        if col not in df.columns:
            validation_issues.append(f"Column '{col}' is missing.")
            continue

        # Check for missing values
        if df[col].isnull().any():
            validation_issues.append(f"Column '{col}' contains missing values.")

        # Validate based on expected type or values
        if isinstance(expected, list):
            # Check for invalid categorical values
            invalid_values = df[~df[col].isin(expected)][col].dropna().unique()
            if len(invalid_values) > 0:
                validation_issues.append(f"Invalid values in '{col}': {invalid_values.tolist()}. Expected values are: {expected}.")
        
        elif expected == "integer":
            # Check if column is of integer type
            if not pd.api.types.is_integer_dtype(df[col]):
                 df[col] = df[col].fillna(0).astype(int)
            else:
                # Check if all integers are within a certain range (if applicable)
                if not df[col].apply(lambda x: isinstance(x, int)).all():
                    validation_issues.append(f"Column '{col}' contains non-integer values.")

        elif expected == "float":
            # Check if column is of float type
            if not pd.api.types.is_float_dtype(df[col]):
                validation_issues.append(f"Column '{col}' must contain float values. Found type: {df[col].dtype}.")

        elif expected == "boolean":
            # Validate boolean columns
            df[col] = df[col].map({"yes": True, "no": False, "none": None})

            # Check for invalid values that could not be converted
            if df[col].isnull().any():
                # Collect invalid values for debugging
                invalid_values = df[col][df[col].isnull()].index.tolist()
                validation_issues.append(f"Column '{col}' contains invalid values that could not be converted to boolean. Invalid entries: {invalid_values}")
                
    if validation_issues:
        raise ValidationError("Data validation issues found:\n" + "\n".join(validation_issues ))

# Transformation function: Fill missing values for numeric columns
def fill_missing_numeric(df, columns):
    for col in columns:
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)
    return df

# Transformation function: Fill missing values for categorical columns
def fill_missing_categorical(df, columns):
    for col in columns:
        if col in df.columns:
            df[col].fillna("None", inplace=True)
    return df

def clean_data(df):
    # Clean checking_status
    df['checking_status'] = df['checking_status'].replace({
        '0<=X<200': '0<=X<20000',  # Correcting an invalid value
        'no checking': 'no checking account',
        '>=200': '>=20000'  # Correcting an invalid value
    })

    # Clean credit_history
    df['credit_history'] = df['credit_history'].replace({
        'no credits/all paid': 'no credits taken/all paid back duly',
        'all paid': 'no credits taken/all paid back duly',
        'critical/other existing credit': 'no credits taken/all paid back duly',
        'delayed previously': 'no credits taken/all paid back duly'
    })

    # Clean purpose
    df['purpose'] = df['purpose'].replace({
        'radio/tv': 'television',
        'education': 'furniture/equipment',  # Assuming a valid purpose
        'new car': 'car',
        'used car': 'car',
        'business': 'furniture/equipment',
        'domestic appliance': 'furniture/equipment',
        'repairs': 'furniture/equipment',
        'other': 'furniture/equipment',
        'retraining': 'furniture/equipment'
    })

    # Convert numeric columns to integers, handling non-integer values
    numeric_columns = ["duration", "credit_amount", "residence_since", "age", "existing_credits", "num_dependents"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, set errors to NaN

    # Fill NaN values with 0 or appropriate values
    df[numeric_columns] = df[numeric_columns].fillna(0)  # You can adjust this as necessary

    # Clean savings_status
    df['savings_status'] = df['savings_status'].replace({
        'no known savings': 'no known savings account',
        '500<=X<1000': '100<=X<1000',
        '>=1000': '>=10000',
        '100<=X<500': '<100'
    })

    # Clean other_parties
    df['other_parties'] = df['other_parties'].replace({
        'co applicant': 'co-applicant'
    })

    # Clean job
    df['job'] = df['job'].replace({
        'unemp/unskilled non res': 'unskilled',
        'unskilled resident': 'unskilled',
    })

    # Clean own_telephone
    df['own_telephone'] = df['own_telephone'].replace({
        'none': 'no'
    })

    # Clean housing
    df['housing'] = df['housing'].replace({
        'for free': 'free'
    })

    return df