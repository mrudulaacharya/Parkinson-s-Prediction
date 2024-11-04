import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function 1: Data Cleaning

def clean_data(df):
    # Drop duplicate rows
    df = df.drop_duplicates()
    # Drop columns with more than 90% missing values
    drop_threshold = 0.6 * len(df)
    missing_values = df.isnull().sum()
    columns_to_drop = missing_values[missing_values > drop_threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    
    # Handle missing values
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            # Fill numerical columns with mean
            df[col].fillna(int(df[col].mean()), inplace=True)
        elif df[col].dtype == 'object':
            # Fill categorical columns with mode
            df[col].fillna(df[col].mode()[0], inplace=True)
    
    print("Data cleaning complete. Missing values handled and duplicates dropped.")
    df.to_csv('cleaned_data.csv', index=False)

    return df

# Function 2: Data Preprocessing
def preprocess_data(df):
    # Convert date columns to datetime format if any
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # One-hot encode categorical variables
    categorical_cols = df.select_dtypes(include=['object']).columns
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
    
    print("Data preprocessing complete. Date conversion and encoding done.")
    return df

# Function 3: EDA for ENROLL_AGE, SEX, and COHORT columns
def eda(df):
    # Distribution of ENROLL_AGE
    if 'ENROLL_AGE' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.histplot(df['ENROLL_AGE'], kde=True, bins=20)
        plt.title('Distribution of ENROLL_AGE')
        plt.xlabel('ENROLL_AGE')
        plt.ylabel('Frequency')
        plt.show()
    else:
        print("ENROLL_AGE column not found in the dataset.")
    
    # Count plot for SEX
    if 'SEX' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.countplot(x='SEX', data=df)
        plt.title('Distribution of SEX')
        plt.xlabel('SEX')
        plt.ylabel('Count')
        plt.show()
    else:
        print("SEX column not found in the dataset.")
    
    # Count plot for COHORT
    if 'COHORT' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.countplot(x='COHORT', data=df)
        plt.title('Distribution of COHORT')
        plt.xlabel('COHORT')
        plt.ylabel('Count')
        plt.show()
    else:
        print("COHORT column not found in the dataset.")

file_path = '/home/yutika/Downloads/modified_dataset.csv' 
df = pd.read_csv(file_path)

# Step 1: Clean the data
df_cleaned = clean_data(df)

# Step 2: Preprocess the data
df_preprocessed = preprocess_data(df_cleaned)

# Step 3: Perform EDA for ENROLL_AGE, SEX, and COHORT
eda(df_preprocessed)
