import pandas as pd
import numpy as np

def handle_missing_values(df, drop_threshold=0.5, strategy='mean', constant_value=None, identify_only=False):
    """
    Identifies and handles missing values in the dataset.

    Parameters:
    df (pandas.DataFrame): Input dataset
    drop_threshold (float): Threshold for dropping columns with missing values (default: 0.5)
    strategy (str): Strategy for handling missing values ('mean', 'median', 'constant')
    constant_value (float): Value to use for constant imputation (if strategy is 'constant')
    identify_only (bool): If True, only identifies missing values without modifying the dataset

    Returns:
    pandas.DataFrame or dict: Cleaned dataset or dictionary of missing value counts
    """
    # Identify columns with missing values
    missing_info = df.isnull().sum()
    missing_values_count = missing_info[missing_info > 0]
    print("Missing values identified:")
    print(missing_values_count)

    if identify_only:
        return missing_values_count.to_dict()

    # Drop columns with a missing value ratio above the threshold
    columns_to_drop = missing_info[missing_info / df.shape[0] > drop_threshold].index
    df = df.drop(columns=columns_to_drop)
    print(f"Dropped columns: {list(columns_to_drop)}")

    # Handle missing values based on the strategy
    for column in df.columns:
        if df[column].isnull().sum() > 0:
            if df[column].dtype in [np.float64, np.int64]:  # Numeric columns
                if strategy == 'mean':
                    df[column].fillna(df[column].mean(), inplace=True)
                elif strategy == 'median':
                    df[column].fillna(df[column].median(), inplace=True)
                elif strategy == 'constant' and constant_value is not None:
                    df[column].fillna(constant_value, inplace=True)
            else:  # Non-numeric columns
                df[column].fillna(df[column].mode()[0], inplace=True)

    print("Missing values handled.")
    return df

if __name__ == "__main__":
    # Load the dataset
    file_path = 'merged__m_output.csv'
    dataset = pd.read_csv(file_path)

    # Identify missing values
    missing_values = handle_missing_values(dataset, identify_only=True)
    print("\nMissing value counts:")
    for column, count in missing_values.items():
        print(f"{column}: {count}")

    # Handle missing values
    cleaned_dataset = handle_missing_values(dataset, drop_threshold=0.5, strategy='mean')

    # Save the cleaned dataset
    cleaned_dataset.to_csv('cleaned_dataset.csv', index=False)
    print("\nCleaned dataset saved as 'cleaned_dataset.csv'")

    # Print summary of the cleaned dataset
    print("\nCleaned dataset summary:")
    print(cleaned_dataset.describe())