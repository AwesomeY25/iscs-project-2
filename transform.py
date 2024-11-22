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
            invalid_values = df[col][~df[col].isin([True, False, None])].unique()
            if len(invalid_values) > 0:
                validation_issues.append(f"Column '{col}' contains invalid values that could not be converted to boolean. Invalid entries: {invalid_values.tolist()}")

    if validation_issues:
        raise ValidationError("Data validation issues found:\n" + "\n".join(validation_issues ))

# Transformation function: Fill missing values for numeric columns
def fill_missing_numeric(df, columns):
    for col in columns:
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)
    return df

def convert_float_to_int(df, columns):
    for col in columns:
        if col in df.columns and pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].fillna(0).astype(int)
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
        'unknown': 'other',
    })

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
        'none': False})
    df['own_telephone'] = df['own_telephone'].map({"yes": True, "no": False})
    df['own_telephone'] = df['own_telephone'].fillna(False)

     # Clean foreign_worker 
    df['foreign_worker'] = df['foreign_worker'].replace({
        'no': False,
        'yes': True
    })
    df['foreign_worker'] = df['foreign_worker'].fillna(False)
    
    # Clean housing
    df['housing'] = df['housing'].replace({
        'for free': 'free'
    })

    return df

def clean_boolean_columns(df, columns):
    for col in columns:
        if col in df.columns:
            # Replace specific values and map to boolean
            df[col] = df[col].replace({'none': False})
            df[col] = df[col].map({"yes": True, "no": False})
            
            # Fill NaNs with False
            df[col] = df[col].fillna(False)
    
    return df

def split_personal_status(df, column_name):
    if column_name in df.columns:
        # Create new columns by splitting the personal_status values
        df[['sex', 'marital_status']] = df[column_name].str.split(' ', n=1, expand=True)
        
        # Clean up the marital_status to retain only relevant information
        df['marital_status'] = df['marital_status'].replace({
            'div/sep': 'divorced/separated',
            'div/dep/mar': 'divorced/dependent/married',
            'single': 'single',
            'mar/wid': 'married/widowed'
        })
        
        # Handle cases where marital_status may not be clear
        df['marital_status'] = df['marital_status'].fillna('unknown')

        # Optionally drop the original personal_status column
        df.drop(columns=[column_name], inplace=True)

    return df